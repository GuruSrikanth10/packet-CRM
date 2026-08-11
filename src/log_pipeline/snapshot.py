"""
Log snapshot persistence (KUBERNETES_LOGS_PLAN.md 4.3).

The mechanism that makes a short-retention source viable at all.

Kubelet retention is roughly 10MB x 5 files per container, so a busy service
can lose a two-hour window in minutes. Meanwhile investigations do NOT
reliably run promptly: consumer lag, DLQ replays, MAX_IN_PROGRESS_AGE_SECONDS
staleness resumption, checkpoint resumes, and the Investigator retry loop all
re-enter fetch_logs well after the event. On every one of those paths a naive
fetch-at-analysis-time design returns nothing.

So the first successful fetch is persisted as structured JSONL, and every
later fetch for the same event reuses it and skips the API entirely. Retries
become deterministic and free, the cluster is not re-hit once per retry-loop
iteration, and evidence captured while it existed survives long after the
kubelet dropped it.

JSONL rather than the formatted `raw_logs.txt` so Stages 2-4 can be re-run
later with different tuning.
"""
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from filelock import FileLock

from src.log_pipeline.types import EvidenceGap, GapType
from src.utils.env import get_bool_env
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

RECORDS_FILENAME = "raw_logs_k8s.jsonl"
META_FILENAME = "log_snapshot_meta.json"


def reuse_enabled() -> bool:
    return get_bool_env("LOG_SNAPSHOT_REUSE", True)


def _casesheets_root() -> Path:
    return Path(__file__).resolve().parent.parent.parent / "local_casesheets"


def snapshot_dir(event_id: str) -> Path:
    return _casesheets_root() / f"casebook_{event_id}"


def _records_path(event_id: str) -> Path:
    return snapshot_dir(event_id) / RECORDS_FILENAME


def _meta_path(event_id: str) -> Path:
    return snapshot_dir(event_id) / META_FILENAME


def exists(event_id: str) -> bool:
    return _records_path(event_id).exists()


def save(event_id: str, records: list, gaps: Optional[list] = None,
         source: str = "kubernetes") -> bool:
    """Persist a snapshot atomically. Returns True when written.

    Atomic `.tmp` + `os.replace` under a `filelock`, reusing the discipline
    already in `src/storage/local.py`: a crash mid-write must never leave a
    half-parsed JSONL behind for a later run to trust.
    """
    try:
        directory = snapshot_dir(event_id)
        directory.mkdir(parents=True, exist_ok=True)

        records_path = _records_path(event_id)
        tmp_path = records_path.with_suffix(records_path.suffix + ".tmp")
        lock_path = str(records_path) + ".lock"

        with FileLock(lock_path, timeout=10):
            with open(tmp_path, "w", encoding="utf-8") as f:
                for record in records:
                    f.write(json.dumps(record, ensure_ascii=False) + "\n")
            os.replace(tmp_path, records_path)

            meta = {
                "event_id": event_id,
                "source": source,
                "captured_at": datetime.now(timezone.utc).isoformat(),
                "record_count": len(records),
                "gaps": [
                    {"type": g.gap_type.value, "detail": g.detail, "context": g.context}
                    for g in (gaps or [])
                ],
            }
            meta_tmp = _meta_path(event_id).with_suffix(".json.tmp")
            with open(meta_tmp, "w", encoding="utf-8") as f:
                json.dump(meta, f, indent=2)
            os.replace(meta_tmp, _meta_path(event_id))

        logger.info("Log snapshot saved", event_id=event_id,
                    record_count=len(records), source=source)
        return True
    except Exception as e:
        # A snapshot is an optimisation, never a correctness requirement:
        # failing to write one must not fail the packet.
        logger.warning("Failed to save log snapshot", event_id=event_id,
                       error=f"{type(e).__name__}: {e}")
        return False


def load(event_id: str) -> Optional[tuple]:
    """Return (records, gaps) from a snapshot, or None when unusable."""
    records_path = _records_path(event_id)
    if not records_path.exists():
        return None

    try:
        with FileLock(str(records_path) + ".lock", timeout=10):
            records = []
            with open(records_path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line:
                        records.append(json.loads(line))

            gaps = []
            meta_path = _meta_path(event_id)
            captured_at = None
            if meta_path.exists():
                meta = json.loads(meta_path.read_text(encoding="utf-8"))
                captured_at = meta.get("captured_at")
                for entry in meta.get("gaps", []):
                    try:
                        gaps.append(EvidenceGap(
                            GapType(entry["type"]),
                            entry.get("detail", ""),
                            entry.get("context", {}) or {},
                        ))
                    except (KeyError, ValueError):
                        continue

        logger.info("Log snapshot reused", event_id=event_id,
                    record_count=len(records), captured_at=captured_at)
        return records, gaps
    except Exception as e:
        # A corrupt snapshot must not poison the packet: fall through to a
        # live fetch instead.
        logger.warning("Unusable log snapshot; ignoring", event_id=event_id,
                       error=f"{type(e).__name__}: {e}")
        return None
