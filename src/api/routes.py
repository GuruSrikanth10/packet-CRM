import os
import json
import re
from fastapi import APIRouter, Depends, HTTPException, Security, Request
from fastapi.security import APIKeyHeader
import time
from src.models.schemas import MessagePayload
from src.core.agent_orchestrator import get_agent
from src.utils.s3_uploader import upload_logs_to_s3
from src.storage.factory import get_casebook_storage
from src.utils.dlq_publisher import publish_to_dlq
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

router = APIRouter()

API_KEY_HEADER = APIKeyHeader(name="X-API-Key", auto_error=False)
API_KEYS = [os.environ.get("PACKET_CRM_API_KEY", "dev-secret-key")]

# Simple in-memory rate limiting: 10 requests per minute per IP
RATE_LIMIT = 10
RATE_WINDOW = 60
_rate_limits = {}

def get_api_key(api_key_header: str = Security(API_KEY_HEADER)):
    if api_key_header in API_KEYS:
        return api_key_header
    raise HTTPException(status_code=403, detail="Could not validate API KEY")

def rate_limiter(request: Request):
    client_ip = request.client.host
    current_time = time.time()
    
    # Initialize or clean up old entries
    if client_ip not in _rate_limits:
        _rate_limits[client_ip] = []
        
    _rate_limits[client_ip] = [t for t in _rate_limits[client_ip] if current_time - t < RATE_WINDOW]
    
    if len(_rate_limits[client_ip]) >= RATE_LIMIT:
        raise HTTPException(status_code=429, detail="Too Many Requests")
        
    _rate_limits[client_ip].append(current_time)

@router.get("/health")
def health_check():
    import time
    from pathlib import Path
    
    heartbeat_file = Path(__file__).resolve().parent.parent.parent / "local_checkpoints" / "consumer_heartbeat.txt"
    last_heartbeat = None
    if heartbeat_file.exists():
        try:
            with open(heartbeat_file, "r") as f:
                last_heartbeat = float(f.read().strip())
        except Exception:
            pass
            
    is_consumer_alive = False
    if last_heartbeat is not None:
        # Consumer should poll every 5s, we give it a 30s grace period
        is_consumer_alive = (time.time() - last_heartbeat) < 30
        
    return {
        "status": "up",
        "consumer_alive": is_consumer_alive,
        "last_heartbeat": last_heartbeat
    }

@router.get("/ready")
def readiness_check():
    import sqlite3
    import os
    from pathlib import Path
    from src.utils.dlq_publisher import get_producer
    
    # Check SQLite connectivity
    db_path = Path(__file__).resolve().parent.parent.parent / "local_checkpoints" / "checkpoints.db"
    db_ready = False
    try:
        conn = sqlite3.connect(db_path)
        conn.execute("SELECT 1")
        conn.close()
        db_ready = True
    except Exception as e:
        logger.error(f"Readiness check failed on DB: {e}")
        
    # Check Kafka producer
    kafka_ready = False
    try:
        producer = get_producer()
        if producer is not None:
            # Just checking if producer is created successfully
            kafka_ready = True
    except Exception as e:
        logger.error(f"Readiness check failed on Kafka Producer: {e}")
        
    if db_ready and kafka_ready:
        return {"status": "ready"}
    else:
        raise HTTPException(status_code=503, detail="Service Unavailable")

@router.post("/process-rejection", dependencies=[Depends(get_api_key), Depends(rate_limiter)])
def process_rejection(signal: MessagePayload):
    """
    Endpoint that receives only rejected signals from Kafka.
    It provisions the Agent to investigate the error and decides on the solution.
    """
    signal_dict = signal.model_dump()
    event_id = str(signal.eventId).strip()

    storage = get_casebook_storage()
    existing_casebook = storage.load(event_id, filename="casebook.json")
    existing_status = storage.load(event_id, filename="status.json")
    
    agent = get_agent()
    config = {"configurable": {"thread_id": event_id}}
    state = agent.get_state(config)
    has_active_checkpoint = bool(state and getattr(state, "next", None))

    if existing_casebook:
        status = existing_casebook.get("Packet Status", {}).get("Status")
        terminal_statuses = ("COMPLETED", "REJECTED", "NEEDS_MANUAL_REVIEW", "FAILED_PERMANENT", "DLQ", "FAILED_TIMEOUT")
        if status in terminal_statuses:
            print(f"\\n[API] Skipping Event ID: {event_id}. Terminal casebook already exists.")
            return {"status": "already_processed", "event_id": event_id}
            
    if existing_status:
        status = existing_status.get("Packet Status", {}).get("Status")
        if status == "IN_PROGRESS":
            started_at = existing_status.get("Metadata - Packet Details", {}).get("started_at", 0)
            max_age = int(os.environ.get("MAX_IN_PROGRESS_AGE_SECONDS", 1800))
            is_stale = (time.time() - started_at) > max_age
            
            if is_stale and not has_active_checkpoint:
                print(f"\\n[API] Event ID: {event_id} is IN_PROGRESS but stale with no active checkpoint. Reprocessing fresh.")
            elif has_active_checkpoint:
                print(f"\\n[API] Event ID: {event_id} has active checkpoint. Resuming.")
                return {"status": "already_processing_resumed", "event_id": event_id}

    # Write IN_PROGRESS stub before invoking graph to status.json
    storage.save(event_id, {
        "Metadata - Packet Details": {"EID": event_id, "started_at": time.time()},
        "Packet Status": {"Status": "IN_PROGRESS"}
    }, filename="status.json")

    log = logger.bind(event_id=event_id)
    log.info("Processing Rejection", state="IN_PROGRESS")
    
    # Run the agent with exception handling for DLQ
    log.info("Dispatching payload to LangGraph")
    try:
        if has_active_checkpoint:
            result = agent.invoke(None, config=config)
        else:
            result = agent.invoke({"payload": signal_dict}, config=config)
    except Exception as e:
        import traceback
        error_msg = traceback.format_exc()
        log.error("Unhandled exception during agent processing", exc_info=True, state="DLQ")
        publish_to_dlq(signal_dict, error_msg)
        
        # Mark as FAILED_PERMANENT in storage so it doesn't get re-run on redelivery
        storage.save(event_id, {
            "Metadata - Packet Details": {"EID": event_id},
            "Packet Status": {"Status": "DLQ"},
            "Resolution": {"Synthesis": "Failed unrecoverably during pipeline. Sent to DLQ."}
        })
        return {"status": "dlq", "event_id": event_id, "error": str(e)}
        
    log.info("Agent investigation complete", state="COMPLETED_GRAPH")
    final_message = result.get("synthesis", "{}")
    
    try:
        # Fix: support matching both JSON objects {} and arrays []
        fenced_match = re.search(r"```(?:json)?\s*([\{\[].*?[\}\]])\s*```", final_message, re.DOTALL)
        if fenced_match:
            investigation_result = json.loads(fenced_match.group(1))
        else:
            json_match = re.search(r"(\{.*\})", final_message, re.DOTALL)
            if json_match:
                investigation_result = json.loads(json_match.group(1))
            else:
                investigation_result = json.loads(final_message)
    except Exception:
        investigation_result = final_message
    
    # Extract metadata safely
    packet_meta = signal_dict.get("packetMetaData") or {}
    flow_meta = signal_dict.get("flowMetaData") or {}
    exec_summary = signal_dict.get("packetExecutionSummary") or {}
    
    # Extract rejection code safely
    error_data = exec_summary.get("errorData") or []
    rejection_code = None
    for err in error_data:
        if err and err.get("errorReasonCode"):
            rejection_code = err.get("errorReasonCode")
            break
            
    # Handle investigation result defaults if it's not a dict
    if not isinstance(investigation_result, dict):
        investigation_result = {"Rejection_description": str(investigation_result)}
    
    # Handle Rejection_logs logic
    # The original logs fetch string is in result.get("logs") if we passed it back, 
    # but the orchestrator state keeps it inside the graph state memory.
    # To get it, we need to extract from `result`. Wait, `agent.invoke` returns the final state dict!
    raw_logs = result.get("logs", "")
    processed_logs = None
    
    if not raw_logs or raw_logs == "Log fetching disabled." or raw_logs.startswith("Failed to query"):
        processed_logs = None
    elif len(raw_logs) > 5000:
        print("[API] Logs are too large for JSON, uploading to S3...")
        processed_logs = upload_logs_to_s3(event_id, raw_logs)
    else:
        processed_logs = raw_logs
        
    casebook_data = {
        "Metadata - Packet Details": {
            "SRN": packet_meta.get("srn"),
            "EID": event_id,
            "REFID": packet_meta.get("refId"),
            "SOURCE": signal_dict.get("sourceTopic"),
            "PACKET_TYPE": packet_meta.get("enrolmentType"),
            "MBU": None,  # MBU mapping not immediately available in payload
            "Update_type": None,  # B/D mapping not immediately available
            "is_child": None,  # Age determination not immediately available
            "Created_at": signal_dict.get("eventTimestamp"),
            "Uploaded_at": signal_dict.get("eventTimestamp")
        },
        "Packet Status": {
            "Status": "NEEDS_MANUAL_REVIEW" if investigation_result.get("Action") == "MANUAL_REVIEW" else exec_summary.get("packetStatus"),
            "Service": flow_meta.get("stage"),
            "sub_service": flow_meta.get("subStage"),
            "last_updated": None,
            "Inprocess": False if exec_summary.get("packetStatus") == "REJECTED" else None,
            "Rejection Data": {
                "Rejection_code": rejection_code,
                "Rejection_description": investigation_result.get("Rejection_description"),
                "Rejection_logs": processed_logs,
                "Artifact_design": investigation_result.get("Artifact_design")
            }
        },
        "Resolution": {
            "Synthesis": investigation_result.get("Synthesis"),
            "Action": investigation_result.get("Action"),
            "Resident_action": investigation_result.get("Resident_action"),
            "UIDAI_ACTION": investigation_result.get("UIDAI_ACTION")
        }
    }
    
    storage = get_casebook_storage()
    storage.save(event_id, casebook_data)
        
    log.info("Successfully saved finalized Casebook", final_status=casebook_data["Packet Status"]["Status"], state="COMPLETED")
    
    return {"status": "processed", "event_id": event_id}
