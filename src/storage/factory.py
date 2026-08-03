import os
from src.storage.base import CasebookStorage
from src.storage.local import LocalFilesystemCasebookStorage
from src.storage.s3 import S3CasebookStorage

_STORAGE_CACHE = None

def get_casebook_storage() -> CasebookStorage:
    global _STORAGE_CACHE
    if _STORAGE_CACHE is not None:
        return _STORAGE_CACHE
        
    backend = os.environ.get("CASEBOOK_STORAGE_BACKEND", "local").lower()
    
    if backend == "s3":
        _STORAGE_CACHE = S3CasebookStorage()
    else:
        _STORAGE_CACHE = LocalFilesystemCasebookStorage()
        
    return _STORAGE_CACHE
