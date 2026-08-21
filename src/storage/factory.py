import os
import threading

from src.storage.base import CasebookStorage
from src.storage.local import LocalFilesystemCasebookStorage
from src.storage.s3 import S3CasebookStorage

_STORAGE_CACHE = None
# Racing callers each built a backend. Harmless for the local one, but the S3
# constructor resolves credentials and the duplicate is simply discarded --
# and this is now reached from several threads at once, since the API's
# storage calls moved off the event loop.
_STORAGE_LOCK = threading.Lock()


def get_casebook_storage() -> CasebookStorage:
    global _STORAGE_CACHE
    if _STORAGE_CACHE is not None:
        return _STORAGE_CACHE

    with _STORAGE_LOCK:
        if _STORAGE_CACHE is not None:
            return _STORAGE_CACHE

        backend = os.environ.get("CASEBOOK_STORAGE_BACKEND", "local").lower()
        if backend == "s3":
            _STORAGE_CACHE = S3CasebookStorage()
        else:
            _STORAGE_CACHE = LocalFilesystemCasebookStorage()

        return _STORAGE_CACHE


def reset_storage_cache() -> None:
    """Drop the cached backend. For tests, which swap backends per case."""
    global _STORAGE_CACHE
    with _STORAGE_LOCK:
        _STORAGE_CACHE = None
