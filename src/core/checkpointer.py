"""
Checkpointer selection (ENHANCEMENT_PLAN.md 4.7).

`SqliteSaver` on a local file is the second of the three things pinning this
system to one node (S3CasebookStorage removed the first). Two API replicas
cannot share a SQLite file across pods, so a packet checkpointed by one
replica cannot be resumed by another -- which defeats the resume-from-
checkpoint path that MAX_IN_PROGRESS_AGE_SECONDS and DLQ replay rely on.

CHECKPOINT_BACKEND selects:
  sqlite    -- local file, the default, unchanged behaviour
  postgres  -- shared, suitable for multiple replicas

Postgres is imported lazily and its driver is an optional dependency: a
single-node deployment must not be forced to install it.
"""
import os
import sqlite3

from src.utils.logging_config import get_logger
from src.utils.paths import CHECKPOINT_DB_PATH

logger = get_logger(__name__)


def backend_name() -> str:
    return os.environ.get("CHECKPOINT_BACKEND", "sqlite").strip().lower()


def _build_sqlite():
    from langgraph.checkpoint.sqlite import SqliteSaver

    os.makedirs(CHECKPOINT_DB_PATH.parent, exist_ok=True)
    conn = sqlite3.connect(str(CHECKPOINT_DB_PATH), check_same_thread=False)
    # WAL lets readers proceed during a write, which matters because several
    # packet threads share this one connection.
    conn.execute("PRAGMA journal_mode=WAL;")
    logger.info("Checkpointer ready", backend="sqlite", path=str(CHECKPOINT_DB_PATH))
    return SqliteSaver(conn)


def _build_postgres():
    uri = os.environ.get("CHECKPOINT_POSTGRES_URI", "").strip()
    if not uri:
        raise ValueError(
            "CHECKPOINT_BACKEND=postgres requires CHECKPOINT_POSTGRES_URI."
        )

    try:
        from langgraph.checkpoint.postgres import PostgresSaver
    except ImportError as e:
        raise ImportError(
            "CHECKPOINT_BACKEND=postgres requires langgraph-checkpoint-postgres: "
            "pip install langgraph-checkpoint-postgres"
        ) from e

    checkpointer = PostgresSaver.from_conn_string(uri)
    # Enter the context manager's setup explicitly: from_conn_string returns a
    # context manager, but the graph outlives any `with` block here.
    if hasattr(checkpointer, "__enter__"):
        checkpointer = checkpointer.__enter__()

    # Idempotent; creates the checkpoint tables on first run.
    checkpointer.setup()
    logger.info("Checkpointer ready", backend="postgres")
    return checkpointer


def get_checkpointer():
    """Build the configured checkpointer.

    A misconfigured postgres backend fails loudly rather than silently falling
    back to SQLite: a multi-replica deployment that quietly used per-pod local
    checkpoints would look healthy while losing every cross-replica resume.
    """
    backend = backend_name()
    if backend == "postgres":
        return _build_postgres()
    if backend != "sqlite":
        raise ValueError(
            f"Unknown CHECKPOINT_BACKEND {backend!r}; expected 'sqlite' or 'postgres'."
        )
    return _build_sqlite()
