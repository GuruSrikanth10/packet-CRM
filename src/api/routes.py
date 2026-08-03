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

@router.post("/process-rejection", dependencies=[Depends(get_api_key), Depends(rate_limiter)])
def process_rejection(signal: MessagePayload):
    """
    Endpoint that receives only rejected signals from Kafka.
    It provisions the Agent to investigate the error and decides on the solution.
    """
    signal_dict = signal.model_dump()
    event_id = str(signal.eventId).strip()

    storage = get_casebook_storage()
    if storage.exists(event_id, terminal_only=True):
        print(f"\n[API] Skipping Event ID: {event_id}. Terminal casebook already exists.")
        return {"status": "already_processed", "event_id": event_id}

    log = logger.bind(event_id=event_id)
    log.info("Processing Rejection", state="IN_PROGRESS")
    
    agent = get_agent()
    
    # Run the agent with exception handling for DLQ
    log.info("Dispatching payload to LangGraph")
    try:
        result = agent.invoke(
            {"payload": signal_dict},
            config={"configurable": {"thread_id": event_id}}
        )
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
