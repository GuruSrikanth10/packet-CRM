"""
Single source of truth for filesystem paths shared across processes
(API, consumer, and operator CLIs). Defining these in one place prevents the
kind of path drift where two callers independently guess at the same file
and end up pointing at different locations.
"""
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent.parent

LOCAL_CHECKPOINTS_DIR = REPO_ROOT / "local_checkpoints"
CHECKPOINT_DB_PATH = LOCAL_CHECKPOINTS_DIR / "checkpoints.db"
