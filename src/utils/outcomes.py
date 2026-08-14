"""
Resolution outcome recording (ENHANCEMENT_PLAN.md section 4.1).

The gap this closes: nothing in the system measured whether a resolution was
actually right. `eval_harness.py` scores the log pipeline's evidence citation,
not whether the `action` the Synthesis agent chose resolved the packet. So
there was no way to safely auto-promote a runbook, detect agent regression
after a prompt change, justify raising automation levels, or answer "how well
does this work?"

An outcome is deliberately a SEPARATE write from the casebook, appended to the
casebook's own directory as `outcome.json`. It is human-supplied ground truth
about an agent-produced artifact, arriving minutes to weeks later; folding it
into casebook.json would mean rewriting a terminal record long after the fact
and would make "what did the agent conclude" and "was it right" impossible to
tell apart in an audit.
"""
import json
import os
from datetime import datetime, timezone
from typing import Optional

from filelock import FileLock

from src.storage.base import OUTCOME_VERDICTS
from src.utils.logging_config import get_logger
from src.utils.paths import LOCAL_CASESHEETS_DIR, casebook_dir

logger = get_logger(__name__)

OUTCOME_FILENAME = "outcome.json"


class UnknownEventError(Exception):
    """No casebook exists for this event, so there is nothing to judge."""


class InvalidVerdictError(Exception):
    """The verdict is not one of OUTCOME_VERDICTS."""


def outcome_path(event_id: str):
    return casebook_dir(event_id) / OUTCOME_FILENAME


def record_outcome(event_id: str, verdict: str, verified_by: str,
                   notes: str = "", corrected_action: Optional[str] = None) -> dict:
    """Attach an operator verdict to a completed investigation.

    Raises UnknownEventError if no casebook exists -- recording an outcome for
    an event that was never investigated would silently create a directory and
    pollute the accuracy denominator.
    """
    verdict = (verdict or "").strip().upper()
    if verdict not in OUTCOME_VERDICTS:
        raise InvalidVerdictError(
            f"verdict must be one of {OUTCOME_VERDICTS}, got {verdict!r}"
        )

    directory = casebook_dir(event_id)
    casebook_file = directory / "casebook.json"
    if not casebook_file.exists():
        raise UnknownEventError(f"No casebook found for event {event_id}")

    casebook = json.loads(casebook_file.read_text(encoding="utf-8"))
    resolution = casebook.get("resolution") or {}
    rejection_data = (casebook.get("packet_status") or {}).get("rejection_data") or {}

    outcome = {
        "event_id": event_id,
        "verdict": verdict,
        "verified_by": verified_by,
        "verified_at": datetime.now(timezone.utc).isoformat(),
        "notes": notes,
        # Denormalised so accuracy_report can group without re-reading and
        # re-parsing every casebook, and so the outcome stays interpretable
        # even if the casebook is later pruned.
        "reason_code": rejection_data.get("rejection_code"),
        "enrolment_type": (casebook.get("packet_metadata") or {}).get("update_type"),
        "resolution_source": resolution.get("source"),
        "agent_action": resolution.get("action"),
        "corrected_action": corrected_action,
    }

    path = outcome_path(event_id)
    tmp_path = path.with_suffix(".json.tmp")
    with FileLock(str(path) + ".lock", timeout=10):
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(outcome, f, indent=2, ensure_ascii=False)
        os.replace(tmp_path, path)

    logger.info("Resolution outcome recorded", event_id=event_id, verdict=verdict,
                resolution_source=outcome["resolution_source"])
    return outcome


def load_outcome(event_id: str) -> Optional[dict]:
    path = outcome_path(event_id)
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        logger.warning("Unusable outcome file; ignoring", event_id=event_id,
                       error=str(e))
        return None


def iter_outcomes():
    """Yield every recorded outcome across all casebooks."""
    if not LOCAL_CASESHEETS_DIR.exists():
        return
    for directory in LOCAL_CASESHEETS_DIR.iterdir():
        if not directory.is_dir():
            continue
        path = directory / OUTCOME_FILENAME
        if not path.exists():
            continue
        try:
            yield json.loads(path.read_text(encoding="utf-8"))
        except Exception as e:
            logger.warning("Skipping unusable outcome file", path=str(path),
                           error=str(e))


def summarise(outcomes) -> dict:
    """Aggregate outcomes into accuracy by reason code, type, and source.

    `resolution_source` is grouped on deliberately: comparing agent-generated
    against runbook-served accuracy on the same reason code is the evidence
    needed before a runbook can be trusted to short-circuit the agents (4.2).
    """
    buckets = {}
    for outcome in outcomes:
        source = outcome.get("resolution_source") or "unknown"
        # Collapse runbook:<id>@v<n> to "runbook" so versions aggregate.
        source_kind = "runbook" if source.startswith("runbook:") else source

        key = (
            outcome.get("reason_code") or "unknown",
            outcome.get("enrolment_type") or "unknown",
            source_kind,
        )
        bucket = buckets.setdefault(key, {v: 0 for v in OUTCOME_VERDICTS})
        verdict = outcome.get("verdict")
        if verdict in bucket:
            bucket[verdict] += 1

    rows = []
    for (reason_code, enrolment_type, source), counts in sorted(buckets.items()):
        total = sum(counts.values())
        correct = counts["CORRECT"]
        rows.append({
            "reason_code": reason_code,
            "enrolment_type": enrolment_type,
            "resolution_source": source,
            "total": total,
            **counts,
            # Partial credit is deliberately excluded: a partially correct
            # resolution still needed a human, so it is not an automation win.
            "accuracy": round(correct / total, 4) if total else 0.0,
        })
    return {"rows": rows, "total_outcomes": sum(r["total"] for r in rows)}
