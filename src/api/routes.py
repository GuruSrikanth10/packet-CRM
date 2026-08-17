import os
import json
import re
import time
import hmac
import asyncio
import contextlib
import functools
import ipaddress
import threading
import concurrent.futures
from typing import Literal, Optional
from fastapi import APIRouter, Depends, HTTPException, Security, Request, Response
from fastapi.security import APIKeyHeader
from pydantic import BaseModel, Field
from src.models.schemas import EVENT_ID_PATTERN, MessagePayload
from src.models.synthesis import parse_synthesis
from src.utils import metrics
from src.utils.outcomes import (
    InvalidVerdictError,
    UnknownEventError,
    record_outcome,
)
from src.core.agent_orchestrator import get_agent, prompt_fingerprint
from src.utils.s3_uploader import upload_logs_to_s3
from src.storage.factory import get_casebook_storage
from src.utils.dlq_publisher import publish_to_dlq
from src.utils.logging_config import get_logger
from src.storage.base import PROTECTED_TERMINAL_STATUSES, TERMINAL_STATUSES

logger = get_logger(__name__)

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

# Set on SIGTERM so /ready starts failing and the orchestrator stops routing
# new work here while in-flight investigations finish (G9).
_draining = threading.Event()

# Event ids currently inside agent.invoke(), so a shutdown can mark whatever
# it could not finish rather than leaving IN_PROGRESS stubs behind.
_in_flight_lock = threading.Lock()
_in_flight_events: set = set()

# How long the API waits for in-flight investigations on shutdown. Kept under
# a typical terminationGracePeriodSeconds so the drain completes before
# SIGKILL, mirroring the consumer's SHUTDOWN_DRAIN_SECONDS.
API_SHUTDOWN_DRAIN_SECONDS = float(os.environ.get("API_SHUTDOWN_DRAIN_SECONDS", "25"))


def _in_flight_investigations() -> int:
    with _in_flight_lock:
        return len(_in_flight_events)


def drain_and_shutdown() -> None:
    """Stop accepting work, let in-flight investigations finish, mark the rest.

    F12 gave the consumer signal handling, a bounded drain, and a final
    commit. The API got none of it: uvicorn stopped accepting connections
    while `agent.invoke()` kept running on non-daemon threads, and SIGKILL
    landed mid-investigation. Every packet caught that way left an
    IN_PROGRESS status.json, which blocks reprocessing for
    MAX_IN_PROGRESS_AGE_SECONDS -- 30 minutes by default. A rolling deploy
    stalled every in-flight packet for half an hour (G9).
    """
    _draining.set()
    logger.info("API draining", in_flight=_in_flight_investigations(),
                budget_seconds=API_SHUTDOWN_DRAIN_SECONDS)

    deadline = time.monotonic() + API_SHUTDOWN_DRAIN_SECONDS
    while time.monotonic() < deadline and _in_flight_investigations() > 0:
        time.sleep(0.25)

    # A Python thread cannot be interrupted, so anything still running is
    # abandoned rather than stopped. Its status.json must not be left at
    # IN_PROGRESS or the packet is unreprocessable until it goes stale.
    with _in_flight_lock:
        stragglers = set(_in_flight_events)

    if stragglers:
        logger.warning("Marking investigations abandoned at shutdown",
                       count=len(stragglers))
        storage = get_casebook_storage()
        for event_id in stragglers:
            try:
                if storage.terminal_status(event_id):
                    continue
                storage.save_terminal(event_id, {
                    "packet_metadata": {"eid": event_id},
                    "packet_status": {"status": "FAILED_SHUTDOWN"},
                    "resolution": {"synthesis": (
                        "The API shut down while this investigation was in "
                        "flight. The Kafka offset was never committed, so the "
                        "packet will be redelivered."
                    )},
                })
            except Exception as e:
                logger.error("Could not mark an abandoned investigation",
                             event_id=event_id,
                             error=f"{type(e).__name__}: {e}")

    _agent_invoke_executor.shutdown(wait=False, cancel_futures=True)
    logger.info("API shutdown complete")

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
        logger.error("Readiness check failed on the Kafka producer", error=f"{type(e).__name__}: {e}")

    with _producer_health_lock:
        _producer_health_cache["ready"] = ready
        _producer_health_cache["checked_at"] = now
    return ready


@contextlib.contextmanager
def _tracked_in_flight(event_id: str):
    """Register a packet as in flight for the duration of the block.

    Paired with `drain_and_shutdown`, which needs to know what it interrupted
    so those packets can be marked terminal instead of stranded at
    IN_PROGRESS (G9). Removal is in a finally so every exit path -- success,
    timeout, DLQ, exception -- deregisters.
    """
    with _in_flight_lock:
        _in_flight_events.add(event_id)
    try:
        yield
    finally:
        with _in_flight_lock:
            _in_flight_events.discard(event_id)


@contextlib.contextmanager
def _packet_metrics(resolution_source: str = "agent"):
    """Record PACKETS_TOTAL and PACKET_DURATION however the packet exits.

    Both used to be incremented on the last line of process_rejection, so
    every early return skipped them: FAILED_TIMEOUT, DLQ, already-processed,
    already-processing, and the late-result discard. The `status` label could
    therefore only ever take success values, which made the timeout and DLQ
    rates -- the two numbers worth paging on -- structurally unobservable, and
    biased p95 latency downward by dropping exactly the slow tail it measures
    (G15).

    The caller sets `outcome["status"]` and, optionally, `outcome["source"]`.
    """
    outcome = {"status": None, "source": resolution_source}
    started = time.monotonic()
    try:
        yield outcome
    finally:
        # Collapse runbook:<id>@v<n> so the label set stays bounded -- a
        # per-runbook label would grow cardinality with the runbook catalog.
        source = str(outcome.get("source") or "agent")
        source_kind = "runbook" if source.startswith("runbook:") else "agent"
        metrics.PACKETS_TOTAL.labels(
            status=str(outcome.get("status") or "unknown"),
            resolution_source=source_kind,
        ).inc()
        metrics.PACKET_DURATION.labels(resolution_source=source_kind).observe(
            time.monotonic() - started
        )


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

# The rate limiter exists to blunt external abuse, not to throttle this
# system's own Kafka consumer. It was doing the latter: every packet arrives
# from one IP, so the 11th packet in any 60s window got a 429, the consumer
# didn't commit that offset, and throughput collapsed -- instantly under
# RUNBOOK_MODE=serve (sub-second responses) or during any backlog drain (F5).
#
# Two changes: the ceiling now tracks the concurrency the API is actually
# built to serve, and trusted internal callers are exempt entirely.
RATE_LIMIT = int(os.environ.get("RATE_LIMIT_PER_MINUTE",
                               str(max(60, _MAX_CONCURRENT_INVESTIGATIONS * 20))))
RATE_WINDOW = 60
_rate_limits = {}
# FastAPI runs sync dependencies like this one in a threadpool, so concurrent
# requests from different IPs (or racing requests from the same IP) can
# interleave a read-modify-write on _rate_limits without a lock (1.19).
_rate_limits_lock = threading.Lock()


def _parse_networks(raw: str) -> list:
    networks = []
    for entry in raw.split(","):
        entry = entry.strip()
        if not entry:
            continue
        try:
            networks.append(ipaddress.ip_network(entry, strict=False))
        except ValueError:
            logger.warning("Ignoring invalid CIDR in rate-limit exemption list", entry=entry)
    return networks


# Callers exempt from rate limiting: this system's own consumer, and whatever
# internal ranges an operator declares. Defaults to loopback so a single-host
# deployment (start.py) works out of the box.
_RATE_LIMIT_EXEMPT_CIDRS = _parse_networks(
    os.environ.get("RATE_LIMIT_EXEMPT_CIDRS", "127.0.0.0/8,::1/128")
)

# Proxies whose X-Forwarded-For we trust. Behind an ingress, request.client.host
# is the *proxy's* IP, so every caller shares one bucket. XFF is only honoured
# when the immediate peer is one of these -- trusting it unconditionally would
# let any caller forge an exempt source address.
_TRUSTED_PROXY_CIDRS = _parse_networks(os.environ.get("TRUSTED_PROXY_CIDRS", ""))


def _ip_in(networks: list, address: str) -> bool:
    if not networks or not address:
        return False
    try:
        parsed = ipaddress.ip_address(address)
    except ValueError:
        return False
    return any(parsed in network for network in networks)


def client_ip_for(request: Request) -> str:
    """Resolve the caller's address, honouring X-Forwarded-For only from a
    trusted proxy peer."""
    peer = request.client.host if request.client else ""
    if _ip_in(_TRUSTED_PROXY_CIDRS, peer):
        forwarded = request.headers.get("x-forwarded-for", "")
        if forwarded:
            # Left-most entry is the original client.
            return forwarded.split(",")[0].strip()
    return peer


def get_api_key(api_key_header: str = Security(API_KEY_HEADER)):
    # Constant-time comparison: `in` on a list short-circuits on the first
    # differing byte, which leaks key material through response timing (F5).
    if api_key_header:
        for candidate in API_KEYS:
            if candidate and hmac.compare_digest(str(api_key_header), str(candidate)):
                return api_key_header
    raise HTTPException(status_code=403, detail="Could not validate API KEY")

def rate_limiter(request: Request):
    client_ip = client_ip_for(request)

    if _ip_in(_RATE_LIMIT_EXEMPT_CIDRS, client_ip):
        return

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
    """Liveness for THIS process only.

    `consumer_alive` used to be answered by reading the consumer's heartbeat
    file off the local disk. That works under start.py, where both run on one
    host, and cannot work when they are separate pods -- the deployment Phase F
    exists to enable -- so it reported a permanently dead consumer and any
    alert on it fired forever and got muted (G8). The consumer now serves its
    own /health (CONSUMER_HEALTH_PORT); this reports what the API can actually
    observe.

    The heartbeat fields are still emitted when the file is readable, so a
    single-host start.py deployment keeps the signal it had.
    """
    from src.utils.paths import CONSUMER_HEARTBEAT_PATH

    payload = {
        "status": "draining" if _draining.is_set() else "up",
        "draining": _draining.is_set(),
        "in_flight": _in_flight_investigations(),
        "capacity": _MAX_CONCURRENT_INVESTIGATIONS,
    }

    # Co-located deployment only. Absent is absent, not dead.
    try:
        stamp = float(CONSUMER_HEARTBEAT_PATH.read_text(encoding="utf-8").strip())
        payload["last_heartbeat"] = stamp
        payload["consumer_alive"] = (time.time() - stamp) < 30
    except Exception:
        payload["last_heartbeat"] = None
        payload["consumer_alive"] = None  # unknown, not false

    return payload

@router.get("/ready")
def readiness_check():
    from src.core.checkpointer import backend_name as checkpoint_backend_name
    from src.core.checkpointer import health_check as checkpoint_health_check

    # Check checkpoint storage. Probing SQLite unconditionally would report a
    # Postgres-backed deployment as unready forever, since it has no local
    # checkpoint file at all (4.7).
    #
    # This probes the checkpointer the graph is already holding rather than
    # building a new one. Building one per probe opened a Postgres connection
    # and ran setup()'s DDL every few seconds, never closing any of them, so
    # the probe exhausted max_connections and took the database down with it
    # (G4).
    # A draining pod must fail readiness immediately: that is what makes the
    # orchestrator stop sending it new packets while it finishes the ones it
    # already has (G9).
    if _draining.is_set():
        raise HTTPException(status_code=503, detail="Draining")

    db_ready = False
    backend = checkpoint_backend_name()
    try:
        db_ready = checkpoint_health_check()
    except Exception as e:
        logger.error("Readiness check failed on the checkpoint store", backend=backend, error=f"{type(e).__name__}: {e}")

    kafka_ready = _check_kafka_producer_ready()

    if db_ready and kafka_ready:
        return {"status": "ready"}
    else:
        raise HTTPException(status_code=503, detail="Service Unavailable")

@router.get("/metrics")
def metrics_endpoint():
    """Prometheus exposition (4.5).

    Unauthenticated by design -- a scrape target that needs a secret is one
    operators route around. It exposes counters and latencies only, never
    packet content or resident data.
    """
    # Breaker state is sampled at scrape time: pybreaker offers no transition
    # hook here, and a breaker that reset on its own timeout would otherwise
    # leave a stale "open" reading behind (G17).
    metrics.sample_breaker_states()

    payload = metrics.render_latest()
    if payload is None:
        raise HTTPException(
            status_code=501,
            detail="prometheus_client is not installed; metrics are unavailable.",
        )
    return Response(content=payload, media_type=metrics.CONTENT_TYPE_LATEST)


class OutcomeRequest(BaseModel):
    """Operator verdict on a completed investigation (4.1)."""

    verdict: Literal["CORRECT", "INCORRECT", "PARTIAL"]
    verified_by: str = Field(min_length=1, max_length=128)
    notes: str = Field(default="", max_length=4000)
    corrected_action: Optional[str] = Field(default=None, max_length=64)


@router.post("/outcome/{event_id}", dependencies=[Depends(get_api_key), Depends(rate_limiter)])
def record_resolution_outcome(event_id: str, body: OutcomeRequest):
    """Attach ground truth to a resolution, so accuracy becomes measurable.

    Nothing in the system recorded whether a resolution was actually right,
    which blocks runbook promotion, regression detection, and any statement
    about how well the pipeline works (4.1).
    """
    if not re.fullmatch(EVENT_ID_PATTERN, event_id):
        raise HTTPException(status_code=422, detail="Invalid event_id format")

    try:
        outcome = record_outcome(
            event_id=event_id,
            verdict=body.verdict,
            verified_by=body.verified_by,
            notes=body.notes,
            corrected_action=body.corrected_action,
        )
    except UnknownEventError:
        raise HTTPException(
            status_code=404,
            detail=f"No casebook found for event {event_id}; nothing to judge.",
        )
    except InvalidVerdictError as e:
        raise HTTPException(status_code=422, detail=str(e))

    return {"status": "recorded", "event_id": event_id, "verdict": outcome["verdict"]}


@router.post("/process-rejection", dependencies=[Depends(get_api_key), Depends(rate_limiter)])
async def process_rejection(signal: MessagePayload):
    """
    Endpoint that receives only rejected signals from Kafka.
    It provisions the Agent to investigate the error and decides on the solution.

    async so that awaiting the (potentially multi-minute) agent.invoke() call
    below yields the event loop instead of occupying a slot in Starlette's
    sync-dispatch threadpool for the whole duration (2.6).
    """
    event_id = str(signal.eventId).strip()
    with _packet_metrics() as outcome, _tracked_in_flight(event_id):
        response = await _investigate_packet(signal, outcome)
        # Paths that did not classify themselves fall back to the response
        # status, so no exit path goes uncounted (G15).
        if not outcome["status"]:
            outcome["status"] = response.get("status", "unknown")
        return response


async def _investigate_packet(signal: MessagePayload, outcome: dict):
    """The packet lifecycle. `outcome` carries the terminal status back out to
    the metrics wrapper above."""
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
        if status in TERMINAL_STATUSES:
            logger.bind(event_id=event_id).info("Skipping event; a terminal casebook already exists", recorded_status=status)
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
                pre_invoke_log.info("Stale IN_PROGRESS with no active checkpoint; reprocessing from scratch", max_age_seconds=max_age)
            elif has_active_checkpoint:
                pre_invoke_log.info("Active checkpoint found; resuming the existing run")
                return {"status": "already_processing_resumed", "event_id": event_id}
            else:
                # Not stale, and no active checkpoint: a run is in flight but
                # between checkpoint writes. Treat as already processing
                # instead of falling through to a full duplicate reprocess.
                pre_invoke_log.info("IN_PROGRESS and not stale; skipping duplicate invocation")
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
            # Both files move together, so the consumer (and any later
            # redelivery) sees the same verdict whichever one it reads (F4).
            outcome["status"] = "FAILED_TIMEOUT"
            storage.save_terminal(event_id, {
                "packet_metadata": {"eid": event_id},
                "packet_status": {"status": "FAILED_TIMEOUT"},
                "resolution": {"synthesis": f"Investigation exceeded the server-side budget of {agent_invoke_timeout_seconds}s."}
            })
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
        outcome["status"] = "DLQ"
        storage.save_terminal(event_id, {
            "packet_metadata": {"eid": event_id},
            "packet_status": {"status": "DLQ"},
            "resolution": {"synthesis": f"Failed with {type(e).__name__}: {str(e)}"}
        })
        return {"status": "dlq", "event_id": event_id, "error": str(e)}

    log.info("Agent investigation complete", state="COMPLETED_GRAPH")
    final_message = result.get("synthesis", "{}")

    # Parse through the one contract, not a second private one.
    #
    # This used to run its own regex/json.loads/str() fallback chain over the
    # same string the orchestrator had already validated and serialised. Two
    # parsers on one payload, and this one accepted anything: a malformed
    # response became {"rejection_description": "<raw text>"} and produced a
    # casebook with action: null, indistinguishable downstream from a packet
    # the agents genuinely could not classify -- the exact failure the
    # Synthesis contract was introduced to eliminate (G5).
    synthesis_result, synthesis_error = parse_synthesis(final_message)
    if synthesis_result is None:
        log.error(
            "Synthesis output failed the contract at persistence time",
            error=synthesis_error,
            state="FAILED_SYNTHESIS_PARSE",
        )

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
        log.info("Logs exceed the inline size limit; uploading to S3", log_chars=len(raw_logs))
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
        
    # Resolve the fields the casebook needs from the validated model. On a
    # parse failure the evidence is still persisted -- metadata, rejection
    # code, logs -- and only the verdict is replaced, so a contract breach
    # never costs us the trace that explains it.
    if synthesis_result is not None:
        packet_status = (
            "NEEDS_MANUAL_REVIEW" if synthesis_result.action == "MANUAL_REVIEW"
            else exec_summary.get("packetStatus")
        )
        rejection_description = synthesis_result.rejection_description
        resolution = {
            "source": result.get("resolution_source", "agent"),
            "synthesis": synthesis_result.synthesis,
            "action": synthesis_result.action,
            "resident_action": synthesis_result.resident_action,
            # Persisted so accuracy_report can correlate stated confidence
            # against recorded correctness. Without this the abstention
            # threshold could never be calibrated, and so could never be
            # responsibly enabled (G5, N2).
            "confidence": synthesis_result.confidence,
            "abstained": synthesis_result.abstained,
        }

    else:
        packet_status = "FAILED_SYNTHESIS_PARSE"
        rejection_description = (
            "The agents produced output that does not satisfy the Synthesis "
            f"contract: {synthesis_error}"
        )
        resolution = {
            "source": result.get("resolution_source", "agent"),
            "synthesis": (
                "No valid resolution was produced. This packet needs a human: "
                "the agent output could not be validated against the contract."
            ),
            # Explicit, not null. A reader can tell this apart from a genuine
            # MANUAL_REVIEW verdict by packet_status.
            "action": "MANUAL_REVIEW",
            "resident_action": "PENDING",
            "confidence": None,
            "abstained": False,
            "parse_error": synthesis_error,
            "raw_output": final_message[:2000],
        }

    casebook_data = {
        "packet_metadata": {
            "srn": packet_meta.get("srn"),
            "eid": event_id,
            "ref_id": packet_meta.get("refId"),
            "source": signal_dict.get("sourceTopic"),
            "packet_type": packet_meta.get("pktSource"),
            "is_mbu": None,  # MBU mapping not immediately available in payload
            "update_type": packet_meta.get("enrolmentType"),  # B/D mapping not immediately available
            "is_child": None,  # Age determination not immediately available
            "created_at": signal_dict.get("sidDate"),
            "uploaded_at": signal_dict.get("eventTimestamp")
        },
        "packet_status": {
            "status": packet_status,
            "service": flow_meta.get("stage"),
            "sub_service": flow_meta.get("subStage"),
            "last_updated": None,
            "is_in_process": False if exec_summary.get("packetStatus") == "REJECTED" else None,
            "rejection_data": {
                "rejection_code": rejection_code,
                "rejection_description": rejection_description,
                "rejection_logs": processed_logs
            }
        },
        "resolution": resolution
    }

    # What the shadowed runbook would have decided. Persisted so
    # accuracy_report can score it against the operator's verdict and answer
    # "would this runbook have been right?" -- the question that gates
    # promotion to serve, and which shadow mode previously answered only in
    # unaggregatable warning lines (G18).
    shadow = result.get("shadow_comparison")
    if shadow:
        casebook_data["resolution"]["shadow"] = shadow

    # Which prompts produced this. Lets an accuracy movement be attributed to
    # a prompt change instead of merely correlated with one (G23).
    casebook_data["resolution"]["provenance"] = {
        "prompt_fingerprint": prompt_fingerprint(),
    }

    # Guard against overwriting a terminal status another actor already
    # recorded while this invocation was still in flight -- e.g. the
    # consumer's own client-side timeout fired and wrote FAILED_TIMEOUT/DLQ
    # before this slow LLM call finally returned (0.8). Checks BOTH files:
    # the consumer's timeout handler used to write only casebook.json while
    # this guard read only status.json, so it never fired (F4).
    current_status = storage.terminal_status(event_id)
    if current_status in PROTECTED_TERMINAL_STATUSES:
        log.warning(
            "Discarding late result; a terminal status was already recorded by another actor",
            recorded_status=current_status,
            state=current_status,
        )
        outcome["status"] = "discarded_late_result"
        return {"status": "already_processed", "event_id": event_id}

    # casebook.json and status.json reach the terminal status together, so a
    # crash between them can't leave status.json stuck at IN_PROGRESS.
    storage.save_terminal(event_id, casebook_data)

    final_status = casebook_data["packet_status"]["status"]
    resolution_source = casebook_data["resolution"]["source"] or "agent"
    # Recorded by the _packet_metrics wrapper on the way out, so the timeout,
    # DLQ, and skip paths are counted on the same footing (G15).
    outcome["status"] = str(final_status)
    outcome["source"] = resolution_source

    log.info("Successfully saved finalized Casebook", final_status=final_status,
             resolution_source=resolution_source, state="COMPLETED")

    return {"status": "processed", "event_id": event_id}
