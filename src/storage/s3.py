from typing import Optional
from src.storage.base import CasebookStorage

class S3CasebookStorage(CasebookStorage):
    def __init__(self):
        # Setup boto3 client, bucket name from env, etc.
        pass
        
    def save(self, event_id: str, casebook: dict, filename: str = "casebook.json") -> None:
        raise NotImplementedError("S3CasebookStorage is not yet implemented.")

    def save_terminal(self, event_id: str, casebook: dict) -> None:
        raise NotImplementedError("S3CasebookStorage is not yet implemented.")

    def terminal_status(self, event_id: str) -> Optional[str]:
        raise NotImplementedError("S3CasebookStorage is not yet implemented.")


    def load(self, event_id: str, filename: str = "casebook.json") -> Optional[dict]:
        raise NotImplementedError("S3CasebookStorage is not yet implemented.")
        
    def exists(self, event_id: str, terminal_only: bool = False, filename: str = "casebook.json") -> bool:
        raise NotImplementedError("S3CasebookStorage is not yet implemented.")
