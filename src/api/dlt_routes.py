"""Phase 5 of DLT_PLAN.md -- the DLT fetch endpoint.

`POST /fetch-dlt-logs` is the DLT flow's equivalent of `/fetch-logs`: bounded
I/O, no LLM. It parses the record, persists the evidence, fetches whatever pod
logs cover the failing attempt, and hands the case to the analysis queue.

Two ordering rules matter here:

* **Evidence is persisted before analysis, always.** `headers.json` and
  `trace.txt` are written verbatim first, so a parser bug is recoverable
  without re-consuming Kafka -- by which time the record may have aged out of
  the topic's retention.

* **Redaction runs before any persistence.** A stacktrace message can carry a
  UID, and this text lands on disk and possibly in S3. The `refId` is
  allowlisted so it survives -- it is an operational correlation id, and
  scrubbing it would destroy the investigation.
"""
import json
import time
from typing import Optional

from fastapi import APIRouter, Depends

from src.api.routes import get_api_key, rate_limiter
from src.dlt import registry
from src.dlt.case_storage import get_dlt_storage
from src.dlt.classify import classify
from src.dlt.headers import parse_headers
from src.dlt.stacktrace import (
    build_signature,
    compute_fingerprint,
    normalise_frames,
    parse_stacktrace,
)
from src.dlt.window import derive_window
from src.log_pipeline import redaction
from src.log_pipeline.pipeline import reduce_logs
from src.models.dlt_schemas import DltMessage
from src.storage.base import LOGS_FETCHED_STATUS, TERMINAL_STATUSES
from src.utils import metrics
from src.utils.analysis_queue_publisher import publish_to_dlt_analysis_queue
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

router = APIRouter()

#: Artifact names written per case. Kept here so Phase 8 and the operator CLI
#: read them under one set of names.
HEADERS_ARTIFACT = "headers.json"
TRACE_ARTIFACT = "trace.txt"
PARSED_TRACE_ARTIFACT = "parsed_trace.json"
FETCHED_LOGS_ARTIFACT = "fetched_logs.txt"


def build_failure(headers, exception_message: Optional[str]) -> dict:
    """Parse, classify and fingerprint one record's failure.

    Shared with `/analyze-dlt` so both stages derive identical values from the
    same bytes -- the parse is pure, so re-deriving it is cheaper and safer
    than trusting a summary carried across a topic.
    """
    trace = parse_stacktrace(headers.stacktrace)
    result = classify(trace, exception_message)
    frames = normalise_frames(trace.root_frames)
    root_fqcn = trace.root.fqcn if trace.root else None
    code = result.business_code or ""

    return {
        "failure_class": result.failure_class.value,
        "class_reason": result.reason,
        "root_fqcn": root_fqcn,
        "root_message": trace.root.message if trace.root else "",
        "business_code": result.business_code,
        "registry_description": registry.lookup(result.business_code),
        "fingerprint": compute_fingerprint(root_fqcn, frames, code),
        "signature": build_signature(root_fqcn, frames, code),
        "frames": list(frames),
        "truncated": trace.truncated,
        "chain": [
            {"fqcn": link.fqcn, "message": link.message, "frames": list(link.frames)}
            for link in trace.chain
        ],
    }


def _persist_evidence(storage, case_id: str, message: DltMessage,
                      failure: dict, allowlist: list) -> None:
    """Write the verbatim record and the parsed failure, redacted."""
    redacted_headers = {
        name: (redaction.redact_text(value, allowlist=allowlist).text
               if isinstance(value, str) else value)
        for name, value in (message.headers or {}).items()
    }
    storage.save_artifact(case_id, HEADERS_ARTIFACT,
                          json.dumps(redacted_headers, indent=2, ensure_ascii=False))

    raw_trace = (message.headers or {}).get("kafka_exception-stacktrace") or ""
    storage.save_artifact(case_id, TRACE_ARTIFACT,
                          redaction.redact_text(raw_trace, allowlist=allowlist).text)

    storage.save_artifact(
        case_id, PARSED_TRACE_ARTIFACT,
        redaction.redact_text(json.dumps(failure, indent=2, ensure_ascii=False),
                              allowlist=allowlist).text)


@router.post("/fetch-dlt-logs", dependencies=[Depends(get_api_key), Depends(rate_limiter)])
def fetch_dlt_logs(message: DltMessage):
    """Endpoint the DLT consumer forwards dead-lettered records to.

    Deliberately NOT async, for the same reason as `/fetch-logs`: this is
    bounded I/O, not a multi-minute LLM call, so it belongs on Starlette's
    sync-dispatch threadpool.
    """
    case_id = message.case_id
    log = logger.bind(case_id=case_id, ref_id=message.ref_id)
    storage = get_dlt_storage()

    recorded_status = storage.terminal_status(case_id)
    if recorded_status in TERMINAL_STATUSES:
        log.info("Skipping fetch; a terminal DLT case already exists",
                 recorded_status=recorded_status)
        return {"status": "already_processed", "case_id": case_id}

    headers = parse_headers(message.headers)
    failure = build_failure(headers, headers.exception_message)
    allowlist = [v for v in (message.ref_id, case_id) if v]

    _persist_evidence(storage, case_id, message, failure, allowlist)
    metrics.record_dlt_case(failure["failure_class"])

    gaps = []
    window = derive_window(headers)

    if storage.artifact_exists(case_id, FETCHED_LOGS_ARTIFACT):
        log.info("Logs already fetched; reusing the persisted artifact")
    elif not message.ref_id:
        # Header-only is a valid outcome, not a failure. The stacktrace is
        # still the primary evidence; we simply cannot corroborate it.
        gaps.append("NO_CORRELATION_ID")
        log.warning("No refId on the payload; skipping the log fetch")
        storage.save_artifact(case_id, FETCHED_LOGS_ARTIFACT,
                              "No refId available; logs were not fetched.")
    elif window is None:
        gaps.append("NO_TIMESTAMP")
        log.warning("No usable timestamp in the headers; skipping the log fetch")
        storage.save_artifact(case_id, FETCHED_LOGS_ARTIFACT,
                              "No usable timestamp; logs were not fetched.")
    elif window.too_old:
        # A fetch certain to return nothing still costs a full Kubernetes
        # fan-out across every pod in the namespace.
        gaps.append("LOGS_TOO_OLD")
        log.warning("Log window is older than DLT_MAX_LOG_AGE_SECONDS; skipping the fetch",
                    window=window.describe())
        storage.save_artifact(case_id, FETCHED_LOGS_ARTIFACT,
                              f"Log window too old to fetch: {window.describe()}")
    else:
        log.info("Fetching logs for the DLT case", window=window.describe())
        metrics.record_dlt_window_age(window.age_seconds)
        try:
            formatted = reduce_logs(
                # Search on refId -- the only identifier the service logs --
                # but persist under case_id (DLT_PLAN.md 5.5).
                message.ref_id,
                extra_identifiers=(case_id,),
                storage_key=case_id,
                window=window.to_time_window(),
                storage=storage,
            )
            storage.save_artifact(case_id, FETCHED_LOGS_ARTIFACT, formatted)
        except Exception as e:
            # The stacktrace is already persisted, so a log-fetch failure
            # degrades the case rather than losing it.
            gaps.append("LOG_FETCH_FAILED")
            log.error("Log fetch failed; continuing header-only",
                      error=f"{type(e).__name__}: {e}")
            storage.save_artifact(case_id, FETCHED_LOGS_ARTIFACT,
                                  f"Log fetch failed: {type(e).__name__}: {e}")

    existing = storage.load(case_id, filename="status.json")
    existing_value = (existing or {}).get("packet_status", {}).get("status")
    if existing_value in (None, LOGS_FETCHED_STATUS):
        storage.save(case_id, {
            "packet_metadata": {"eid": case_id, "ref_id": message.ref_id,
                                "started_at": time.time()},
            "packet_status": {"status": LOGS_FETCHED_STATUS},
        }, filename="status.json")

    queued = message.model_dump()
    queued["evidence_gaps"] = gaps
    queued["log_window"] = window.describe() if window else None
    publish_to_dlt_analysis_queue(queued)

    log.info("Queued DLT case for analysis", state=LOGS_FETCHED_STATUS,
             failure_class=failure["failure_class"], gaps=gaps)
    return {"status": "queued_for_analysis", "case_id": case_id,
            "failure_class": failure["failure_class"], "gaps": gaps}
