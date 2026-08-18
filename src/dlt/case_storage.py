"""DLT case storage -- a separate root over the shared `CasebookStorage`.

DLT cases live under `dlt_cases/` (local) or the `dlt_cases` key prefix (S3),
not beside the rejection casebooks. They have a different schema, a different
lifecycle and a different audience, and mixing them would put DLT cases in
front of `accuracy_report`, `prune_casesheets` and every other tool that walks
`list_events()` expecting rejection casebooks.

Both backends already take a root, so this is configuration rather than a new
storage implementation: the protocol, the file-locking, the terminal-status
handling and the artifact methods are all reused as-is.
"""
import os
import threading
from typing import Optional

from src.storage.base import CasebookStorage
from src.utils.paths import LOCAL_CASESHEETS_DIR

#: Subdirectory (local) or key prefix (S3) holding DLT cases.
DLT_ROOT_NAME = "dlt_cases"

#: Group records, one per fingerprint. Phase 7 writes these.
DLT_GROUPS_ROOT_NAME = "dlt_groups"

_cache: dict = {}
_cache_lock = threading.Lock()


def _build(root_name: str) -> CasebookStorage:
    backend = os.environ.get("CASEBOOK_STORAGE_BACKEND", "local").lower()
    if backend == "s3":
        from src.storage.s3 import S3CasebookStorage

        base = (os.environ.get("CASEBOOK_S3_PREFIX") or "").strip("/")
        prefix = f"{base}/{root_name}" if base else root_name
        return S3CasebookStorage(prefix=prefix)

    from src.storage.local import LocalFilesystemCasebookStorage

    return LocalFilesystemCasebookStorage(base_dir=str(LOCAL_CASESHEETS_DIR / root_name))


def _get(root_name: str) -> CasebookStorage:
    with _cache_lock:
        existing = _cache.get(root_name)
        if existing is not None:
            return existing
    built = _build(root_name)
    with _cache_lock:
        return _cache.setdefault(root_name, built)


def get_dlt_storage() -> CasebookStorage:
    """Storage for individual DLT cases, keyed by `case_id`."""
    return _get(DLT_ROOT_NAME)


def get_group_storage() -> CasebookStorage:
    """Storage for per-fingerprint group records. Phase 7."""
    return _get(DLT_GROUPS_ROOT_NAME)


def reset_cache() -> None:
    """Drop cached storage handles. For tests, which swap backends per case."""
    with _cache_lock:
        _cache.clear()


def terminal_status(case_id: str) -> Optional[str]:
    """Recorded terminal status for a case, or None. Never raises on a miss."""
    return get_dlt_storage().terminal_status(case_id)
