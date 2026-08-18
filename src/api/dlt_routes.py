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
from src.dlt import canned, groups, orchestrator, registry, reuse
from src.dlt.case_storage import get_dlt_storage
from src.dlt.corroborate import corroborate
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
from src.models.dlt_synthesis import DltFinding, apply_dlt_confidence_policy
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

#: Bumped when the DLT casebook shape changes. Independent of the rejection
#: casebook's CASEBOOK_SCHEMA_VERSION -- different schema, different lifecycle.
DLT_CASEBOOK_SCHEMA_VERSION = "1.0"


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


# ---------------------------------------------------------------------------
# Phase 8 -- the analysis lane
# ---------------------------------------------------------------------------

def _casebook(message: DltMessage, headers, failure: dict, corroboration,
              finding, decision, group: Optional[dict], gaps: list,
              window_description: Optional[str], provenance_source: str) -> dict:
    """Assemble the terminal casebook. See DLT_PLAN.md 7.1."""
    return {
        "schema_version": DLT_CASEBOOK_SCHEMA_VERSION,
        "case_id": message.case_id,
        "detected_at": time.time(),
        "source": {
            "original_topic": headers.original_topic,
            "partition": headers.original_partition,
            "offset": headers.original_offset,
            "consumer_group": headers.consumer_group,
            "attempts": headers.attempts,
            "original_timestamp": headers.original_timestamp_ms,
            "last_attempt_timestamp": headers.last_attempt_ms,
            "anchor_is_fallback": headers.anchor_is_fallback,
            "type_id": headers.type_id,
        },
        "packet": {"ref_id": message.ref_id},
        "failure": {
            "class": failure["failure_class"],
            "class_reason": failure["class_reason"],
            "root_fqcn": failure["root_fqcn"],
            "business_code": failure["business_code"],
            "registry_description": failure["registry_description"],
            "signature": failure["signature"],
            "fingerprint": failure["fingerprint"],
            "frames": failure["frames"],
            "truncated": failure["truncated"],
        },
        "evidence": {
            "corroboration": corroboration.verdict.value,
            "corroboration_reason": corroboration.reason,
            "citations": list(corroboration.citations),
            "unexplained_exceptions": list(corroboration.unexplained),
            "could_not_look": corroboration.could_not_look,
            "log_window": window_description,
            "gaps": gaps,
        },
        "finding": {
            "narrative": finding.narrative,
            "discrepancy": finding.discrepancy,
            "recommendation": finding.recommendation,
            "action": finding.action,
        },
        "confidence": {
            "score": finding.confidence,
            "ceilings_applied": finding.ceilings_applied,
            "abstained": finding.abstained,
        },
        "provenance": {
            "source": provenance_source,
            "reuse_decision": decision.decision.value,
            "reuse_reason": decision.reason,
            "group_fingerprint": failure["fingerprint"],
            "group_occurrences": (group or {}).get("occurrence_count"),
            "recommendation_state": groups.STATE_DRAFT,
        },
        "packet_status": {"status": _terminal_status(finding)},
    }


def _terminal_status(finding) -> str:
    """Map an action onto a terminal status the storage layer recognises."""
    if finding.action == "NO_ACTION":
        return "COMPLETED"
    return "NEEDS_MANUAL_REVIEW"


@router.post("/analyze-dlt", dependencies=[Depends(get_api_key), Depends(rate_limiter)])
def analyze_dlt(message: DltMessage):
    """Endpoint the DLT analysis consumer forwards fetched cases to.

    The reuse policy decides whether this costs an LLM call. Logs and
    corroboration run either way -- never serve a cached recommendation blind
    (DLT_PLAN.md 5.7), because that would disable the mis-cast detector on
    exactly the occurrences worth catching.
    """
    case_id = message.case_id
    log = logger.bind(case_id=case_id, ref_id=message.ref_id)
    storage = get_dlt_storage()

    recorded_status = storage.terminal_status(case_id)
    if recorded_status in TERMINAL_STATUSES:
        log.info("Skipping analysis; a terminal DLT case already exists",
                 recorded_status=recorded_status)
        return {"status": "already_processed", "case_id": case_id}

    headers = parse_headers(message.headers)
    failure = build_failure(headers, headers.exception_message)
    fingerprint = failure["fingerprint"]

    logs = storage.load_artifact(case_id, FETCHED_LOGS_ARTIFACT) or ""
    corroboration = corroborate(logs, failure["root_fqcn"],
                                failure["business_code"], failure["frames"])
    metrics.record_dlt_corroboration(corroboration.verdict.value)

    if failure["failure_class"] == "A" and not failure["registry_description"]:
        metrics.record_dlt_registry_miss()

    # Record the occurrence BEFORE deciding, so a canned finding's "N
    # occurrences" count includes the case it is describing. Recording only
    # touches counts and history, never `recommendation`, so the reuse
    # decision below sees exactly the same cache state either way.
    group = groups.record_occurrence(
        fingerprint, case_id,
        signature=failure["signature"],
        failure_class=failure["failure_class"],
        business_code=failure["business_code"],
        corroboration=corroboration.verdict.value,
    )
    decision = reuse.decide(failure["failure_class"],
                            corroboration.verdict.value, group)
    metrics.record_dlt_reuse(decision.decision.value)

    parse_error = None
    if decision.decision is reuse.Decision.CANNED:
        finding = canned.build(failure["failure_class"], failure, corroboration, group)
        provenance = "canned"
    elif decision.decision is reuse.Decision.REUSE_GROUP:
        finding = DltFinding(**(group or {})["recommendation"])
        provenance = "group_reuse"
    else:
        log.info("Running the DLT analysis lane", reason=decision.reason)
        finding, parse_error = orchestrator.investigate(
            case_id, failure, corroboration, logs)
        provenance = "agent"
        if finding is None:
            finding = DltFinding(
                narrative="The analysis produced output that does not satisfy "
                          "the finding contract, even after a repair attempt. "
                          "The verbatim stack trace and logs are attached.",
                recommendation="A human should read the attached evidence.",
                action="NEEDS_MANUAL_REVIEW",
                confidence=0.0,
            )
            provenance = "failed_synthesis"

    finding = apply_dlt_confidence_policy(
        finding,
        failure_class=failure["failure_class"],
        corroboration=corroboration.verdict.value,
        registry_hit=bool(failure["registry_description"]),
        reused=decision.decision is reuse.Decision.REUSE_GROUP,
        logs=logs,
    )

    # Only an agent run produces a recommendation worth caching. A canned
    # treatment is recomputed identically every time, and re-storing a reused
    # one would just rewrite what is already there.
    if provenance == "agent":
        group = groups.attach_recommendation(fingerprint, finding.model_dump(),
                                             state=groups.STATE_DRAFT)

    casebook = _casebook(message, headers, failure, corroboration, finding,
                         decision, group, message.model_dump().get("evidence_gaps") or [],
                         message.model_dump().get("log_window"), provenance)
    if parse_error:
        casebook["finding"]["parse_error"] = parse_error

    storage.save_terminal(case_id, casebook)

    log.info("DLT case analysed", failure_class=failure["failure_class"],
             corroboration=corroboration.verdict.value,
             decision=decision.decision.value, action=finding.action,
             confidence=finding.confidence)

    return {"status": "processed", "case_id": case_id,
            "action": finding.action, "confidence": finding.confidence,
            "corroboration": corroboration.verdict.value,
            "decision": decision.decision.value}
