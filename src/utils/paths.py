"""
Single source of truth for filesystem paths shared across processes
(API, consumer, and operator CLIs). Defining these in one place prevents the
kind of path drift where two callers independently guess at the same file
and end up pointing at different locations.
"""
import os
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent.parent

LOCAL_CHECKPOINTS_DIR = REPO_ROOT / "local_checkpoints"
CHECKPOINT_DB_PATH = LOCAL_CHECKPOINTS_DIR / "checkpoints.db"

# Where casebooks and their log artifacts live. This was independently
# re-derived in storage/local.py, log_pipeline/pipeline.py (twice) and
# log_pipeline/snapshot.py -- four copies of the same `parent.parent.parent`
# walk, which is exactly the path drift this module exists to prevent (F22).
LOCAL_CASESHEETS_DIR = Path(
    os.environ.get("LOCAL_CASESHEETS_DIR", REPO_ROOT / "local_casesheets")
)


def casebook_dir(event_id: str) -> Path:
    """Directory holding one event's casebook and log artifacts.

    Callers that write there must still go through CasebookStorage for the
    casebook itself; this is for the log artifacts (raw_logs.txt,
    reduced_logs.txt, the Kubernetes snapshot) that sit beside it.
    """
    return LOCAL_CASESHEETS_DIR / f"casebook_{event_id}"
