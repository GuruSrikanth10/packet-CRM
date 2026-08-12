import os
import json
import re
import time
import asyncio
import functools
import threading
import concurrent.futures
from fastapi import APIRouter, Depends, HTTPException, Security, Request
from fastapi.security import APIKeyHeader
from src.models.schemas import MessagePayload
from src.core.agent_orchestrator import get_agent
from src.utils.s3_uploader import upload_logs_to_s3
from src.storage.factory import get_casebook_storage
from src.utils.dlq_publisher import publish_to_dlq
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

# Statuses written by another actor (e.g. the consumer's own timeout handler,
# or the DLQ path on a previous attempt) that a slow, late-finishing agent
# run must never overwrite with a stale "successful" result.
_PROTECTED_TERMINAL_STATUSES = ("FAILED_TIMEOUT", "DLQ")

# A dedicated, bounded pool for agent.invoke() calls -- separate from
# Starlette's own threadpool used to dispatch sync endpoints/dependencies.
# Before this, a burst of multi-minute investigations could occupy every
# slot in that shared pool and leave /health, /ready, and the sync
# dependencies (get_api_key, rate_limiter) queued behind them (2.6). Sized
# to match the consumer's own concurrency ceiling so the API enforces the
# same bound independently of how many requests the consumer's semaphore
# actually lets through.
#
# Note: a timed-out invocation can't be forcibly killed (Python threads
# can't be interrupted), so under sustained timeouts this pool can fill with
# abandoned-but-still-running work; new requests then queue for a free slot
# rather than each spawning an unbounded new OS thread. That's an accepted
# tradeoff for a bounded, predictable ceiling.
_MAX_CONCURRENT_INVESTIGATIONS = int(os.environ.get("MAX_CONCURRENT_INVESTIGATIONS", "5"))
_agent_invoke_executor = concurrent.futures.ThreadPoolExecutor(
    max_workers=_MAX_CONCURRENT_INVESTIGATIONS, thread_name_prefix="agent-invoke"
)

# Kafka producer health, cached so a burst of /ready probes (an orchestrator
# typically polls this every few seconds) doesn't each attempt a fresh
# connection/DNS resolution against the broker (2.7).
_PRODUCER_HEALTH_TTL_SECONDS = float(os.environ.get("PRODUCER_HEALTH_TTL_SECONDS", "30"))
_producer_health_lock = threading.Lock()
_producer_health_cache = {"ready": False, "checked_at": 0.0}


def _check_kafka_producer_ready() -> bool:
    now = time.time()
    with _producer_health_lock:
        if now - _producer_health_cache["checked_at"] < _PRODUCER_HEALTH_TTL_SECONDS:
            return _producer_health_cache["ready"]

    ready = False
    try:
        from src.utils.dlq_publisher import get_producer
        producer = get_producer()
        ready = producer is not None
    except Exception as e:
        logger.error(f"Readiness check failed on Kafka Producer: {e}")

    with _producer_health_lock:
        _producer_health_cache["ready"] = ready
        _producer_health_cache["checked_at"] = now
    return ready


def _get_agent_invoke_timeout_seconds() -> float:
    """Server-side budget for a single agent.invoke() call.

    The consumer's HTTP client gives up after PACKET_TIMEOUT_SECONDS (default
    300s) and marks the packet FAILED_TIMEOUT / DLQ on its own. Without a
    server-side budget, the API thread keeps running an already-abandoned
    investigation and can later overwrite that terminal status with a
    "successful" result once the LLM call finally returns (0.8). Read at
    call time (not module import time) so it tracks PACKET_TIMEOUT_SECONDS
    when that is reconfigured, e.g. in tests.
    """
    packet_timeout = float(os.environ.get("PACKET_TIMEOUT_SECONDS", "300"))
    default_budget = max(packet_timeout - 30, 30)
    return float(os.environ.get("AGENT_INVOKE_TIMEOUT_SECONDS", default_budget))

router = APIRouter()

API_KEY_HEADER = APIKeyHeader(name="X-API-Key", auto_error=False)
API_KEYS = [os.environ.get("PACKET_CRM_API_KEY", "dev-secret-key")]

# Simple in-memory rate limiting: 10 requests per minute per IP
RATE_LIMIT = 10
RATE_WINDOW = 60
_rate_limits = {}
# FastAPI runs sync dependencies like this one in a threadpool, so concurrent
# requests from different IPs (or racing requests from the same IP) can
# interleave a read-modify-write on _rate_limits without a lock (1.19).
_rate_limits_lock = threading.Lock()

def get_api_key(api_key_header: str = Security(API_KEY_HEADER)):
    if api_key_header in API_KEYS:
        return api_key_header
    raise HTTPException(status_code=403, detail="Could not validate API KEY")

def rate_limiter(request: Request):
    client_ip = request.client.host
    current_time = time.time()

    with _rate_limits_lock:
        # Periodically evict stale IPs to prevent unbounded memory growth
        if len(_rate_limits) > 1000:
            stale_ips = [ip for ip, ts_list in _rate_limits.items()
                         if not ts_list or (current_time - max(ts_list)) > RATE_WINDOW]
            for ip in stale_ips:
                del _rate_limits[ip]

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
    from src.utils.paths import CHECKPOINT_DB_PATH

    # Check SQLite connectivity. A missing DB file should fail the probe,
    # not be silently created by sqlite3.connect().
    db_ready = False
    try:
        if not CHECKPOINT_DB_PATH.exists():
            raise FileNotFoundError(f"Checkpoint DB not found at {CHECKPOINT_DB_PATH}")
        conn = sqlite3.connect(str(CHECKPOINT_DB_PATH))
        conn.execute("SELECT 1")
        conn.close()
        db_ready = True
    except Exception as e:
        logger.error(f"Readiness check failed on DB: {e}")

    kafka_ready = _check_kafka_producer_ready()

    if db_ready and kafka_ready:
        return {"status": "ready"}
    else:
        raise HTTPException(status_code=503, detail="Service Unavailable")

@router.post("/process-rejection", dependencies=[Depends(get_api_key), Depends(rate_limiter)])
async def process_rejection(signal: MessagePayload):
    """
    Endpoint that receives only rejected signals from Kafka.
    It provisions the Agent to investigate the error and decides on the solution.

    async so that awaiting the (potentially multi-minute) agent.invoke() call
    below yields the event loop instead of occupying a slot in Starlette's
    sync-dispatch threadpool for the whole duration (2.6).
    """
    signal_dict = signal.model_dump()
    event_id = str(signal.eventId).strip()

    storage = get_casebook_storage()
    existing_casebook = storage.load(event_id, filename="casebook.json")
    existing_status = storage.load(event_id, filename="status.json")

    # Check terminal short-circuit before provisioning the agent or touching
    # the checkpoint DB -- an already-terminal packet has no business paying
    # for either.
    if existing_casebook:
        status = existing_casebook.get("packet_status", {}).get("status")
        terminal_statuses = ("COMPLETED", "REJECTED", "NEEDS_MANUAL_REVIEW", "FAILED_PERMANENT", "DLQ", "FAILED_TIMEOUT")
        if status in terminal_statuses:
            logger.bind(event_id=event_id).info("Skipping event; terminal casebook already exists.")
            return {"status": "already_processed", "event_id": event_id}

    agent = get_agent()
    config = {"configurable": {"thread_id": event_id}}
    state = agent.get_state(config)
    has_active_checkpoint = bool(state and getattr(state, "next", None))

    if existing_status:
        status = existing_status.get("packet_status", {}).get("status")
        pre_invoke_log = logger.bind(event_id=event_id)

        if status == "IN_PROGRESS":
            started_at = existing_status.get("packet_metadata", {}).get("started_at", 0)
            max_age = int(os.environ.get("MAX_IN_PROGRESS_AGE_SECONDS", 1800))
            is_stale = (time.time() - started_at) > max_age

            if is_stale and not has_active_checkpoint:
                pre_invoke_log.info("IN_PROGRESS but stale with no active checkpoint. Reprocessing fresh.")
            elif has_active_checkpoint:
                pre_invoke_log.info("Has active checkpoint. Resuming.")
                return {"status": "already_processing_resumed", "event_id": event_id}
            else:
                # Not stale, and no active checkpoint: a run is in flight but
                # between checkpoint writes. Treat as already processing
                # instead of falling through to a full duplicate reprocess.
                pre_invoke_log.info("IN_PROGRESS and not stale. Skipping duplicate invocation.")
                return {"status": "already_processing", "event_id": event_id}

    # Write IN_PROGRESS stub before invoking graph to status.json
    storage.save(event_id, {
        "packet_metadata": {"eid": event_id, "started_at": time.time()},
        "packet_status": {"status": "IN_PROGRESS"}
    }, filename="status.json")

    log = logger.bind(event_id=event_id)
    log.info("Processing Rejection", state="IN_PROGRESS")
    
    # Run the agent with exception handling for DLQ. Offloaded onto the
    # module-level bounded executor (not a per-request one) so agent.invoke
    # never occupies Starlette's own threadpool for its multi-minute
    # duration (2.6).
    log.info("Dispatching payload to LangGraph")
    agent_invoke_timeout_seconds = _get_agent_invoke_timeout_seconds()
    loop = asyncio.get_running_loop()
    try:
        if has_active_checkpoint:
            invoke_call = functools.partial(agent.invoke, None, config=config)
        else:
            # retry_count is explicitly reset here: thread_id is the eventId,
            # and a redelivered "fresh" invocation (no active checkpoint) can
            # otherwise resume a persisted checkpoint whose retry_count is
            # already at/over MAX_INVESTIGATION_RETRIES, escalating instantly
            # without doing any work (0.5).
            invoke_call = functools.partial(
                agent.invoke, {"payload": signal_dict, "retry_count": 0}, config=config
            )
        try:
            result = await asyncio.wait_for(
                loop.run_in_executor(_agent_invoke_executor, invoke_call),
                timeout=agent_invoke_timeout_seconds,
            )
        except asyncio.TimeoutError:
            log.error(
                "Agent invocation exceeded server-side budget",
                exc_info=False,
                state="FAILED_TIMEOUT",
                timeout_seconds=agent_invoke_timeout_seconds,
            )
            storage.save(event_id, {
                "packet_metadata": {"eid": event_id},
                "packet_status": {"status": "FAILED_TIMEOUT"},
                "resolution": {"synthesis": f"Investigation exceeded the server-side budget of {agent_invoke_timeout_seconds}s."}
            }, filename="status.json")
            return {"status": "failed_timeout", "event_id": event_id}
    except Exception as e:
        import traceback
        error_msg = traceback.format_exc()
        log.error("Unhandled exception during agent processing", exc_info=True, state="DLQ")
        publish_to_dlq(signal_dict, error_msg)

        # Mark as DLQ in storage so it doesn't get re-run on redelivery. Both
        # files must move to a terminal status together -- leaving
        # status.json at IN_PROGRESS here previously left it stuck forever,
        # since only casebook.json was written (1.5).
        dlq_status = {
            "packet_metadata": {"eid": event_id},
            "packet_status": {"status": "DLQ"},
            "resolution": {"synthesis": f"Failed with {type(e).__name__}: {str(e)}"}
        }
        storage.save(event_id, dlq_status)
        storage.save(event_id, dlq_status, filename="status.json")
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
        investigation_result = {"rejection_description": str(investigation_result)}
    
    # Handle Rejection_logs logic
    # The original logs fetch string is in result.get("logs") if we passed it back, 
    # but the orchestrator state keeps it inside the graph state memory.
    # To get it, we need to extract from `result`. Wait, `agent.invoke` returns the final state dict!
    # fetch_elastic_logs returns None (not an error-prefixed string) on
    # failure, so a plain falsiness check is sufficient and doesn't miss
    # failure modes with a different message prefix (1.6).
    raw_logs = result.get("logs")
    processed_logs = None

    if not raw_logs or raw_logs == "Log fetching disabled.":
        processed_logs = None
    elif len(raw_logs) > 5000:
        log.info("Logs are too large for JSON, uploading to S3...")
        uploaded_url = upload_logs_to_s3(event_id, raw_logs)
        if uploaded_url:
            processed_logs = uploaded_url
        else:
            # S3 not configured or the upload failed -- previously this
            # silently substituted a fake s3://mock-bucket/... URL while
            # discarding the actual log text (1.12). Keep a truncated copy
            # inline instead of losing the evidence entirely.
            log.warning(
                "S3 upload unavailable; embedding truncated logs inline instead of an S3 URL",
                state="LOGS_TRUNCATED",
            )
            processed_logs = raw_logs[:5000] + "\n...[TRUNCATED: S3 upload unavailable, see raw_logs.txt on disk for the full trace]"
    else:
        processed_logs = raw_logs
        
    casebook_data = {
        "packet_metadata": {
            "srn": packet_meta.get("srn"),
            "eid": event_id,
            "ref_id": packet_meta.get("refId"),
            "source": signal_dict.get("sourceTopic"),
            "packet_type": packet_meta.get("enrolmentType"),
            "is_mbu": None,  # MBU mapping not immediately available in payload
            "update_type": None,  # B/D mapping not immediately available
            "is_child": None,  # Age determination not immediately available
            "created_at": signal_dict.get("eventTimestamp"),
            "uploaded_at": signal_dict.get("eventTimestamp")
        },
        "packet_status": {
            "status": "NEEDS_MANUAL_REVIEW" if investigation_result.get("action") == "MANUAL_REVIEW" else exec_summary.get("packetStatus"),
            "service": flow_meta.get("stage"),
            "sub_service": flow_meta.get("subStage"),
            "last_updated": None,
            "is_in_process": False if exec_summary.get("packetStatus") == "REJECTED" else None,
            "rejection_data": {
                "rejection_code": rejection_code,
                "rejection_description": investigation_result.get("rejection_description"),
                "rejection_logs": processed_logs
            }
        },
        "resolution": {
            "source": result.get("resolution_source", "agent"),
            "synthesis": investigation_result.get("synthesis"),
            "action": investigation_result.get("action"),
            "resident_action": investigation_result.get("resident_action")
        }
    }
    
    storage = get_casebook_storage()

    # Guard against overwriting a terminal status another actor already
    # recorded while this invocation was still in flight -- e.g. the
    # consumer's own client-side timeout fired and wrote FAILED_TIMEOUT/DLQ
    # before this slow LLM call finally returned (0.8).
    current_status_doc = storage.load(event_id, filename="status.json")
    current_status = (current_status_doc or {}).get("packet_status", {}).get("status")
    if current_status in _PROTECTED_TERMINAL_STATUSES:
        log.warning(
            "Discarding late result; a terminal status was already recorded by another actor",
            recorded_status=current_status,
            state=current_status,
        )
        return {"status": "already_processed", "event_id": event_id}

    storage.save(event_id, casebook_data)

    # Clean up the IN_PROGRESS status.json now that we have a final casebook
    try:
        storage.save(event_id, {
            "packet_metadata": {"eid": event_id},
            "packet_status": {"status": casebook_data["packet_status"]["status"]}
        }, filename="status.json")
    except Exception as e:
        log.error("Failed to update status.json after completion", error=str(e), exc_info=True)
        
    log.info("Successfully saved finalized Casebook", final_status=casebook_data["packet_status"]["status"], state="COMPLETED")
    
    return {"status": "processed", "event_id": event_id}
