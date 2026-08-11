import os
import json
from pathlib import Path
from typing import Optional
from filelock import FileLock
from src.storage.base import CasebookStorage

class LocalFilesystemCasebookStorage(CasebookStorage):
    def __init__(self, base_dir: str = None):
        if base_dir:
            self.base_dir = Path(base_dir)
        else:
            self.base_dir = Path(__file__).resolve().parent.parent.parent / "local_casesheets"
            
        os.makedirs(self.base_dir, exist_ok=True)
        
    def _get_dir(self, event_id: str) -> Path:
        base_resolved = self.base_dir.resolve()
        target_dir = (self.base_dir / f"casebook_{event_id}").resolve()

        # Defense in depth alongside the Pydantic eventId pattern (0.11):
        # never create or touch a path that resolves outside the storage root.
        try:
            target_dir.relative_to(base_resolved)
        except ValueError:
            raise ValueError(f"Resolved casebook directory escapes storage root: {target_dir}")

        os.makedirs(target_dir, exist_ok=True)
        return target_dir

    def save(self, event_id: str, casebook: dict, filename: str = "casebook.json") -> None:
        target_dir = self._get_dir(event_id)
        final_path = target_dir / filename
        tmp_path = target_dir / f"{filename}.tmp"
        lock_path = target_dir / f"{filename}.lock"
        
        # Enforce schema version for backwards compatibility
        if "schema_version" not in casebook:
            casebook["schema_version"] = "1.0"
        
        with FileLock(str(lock_path), timeout=10):
            with open(tmp_path, "w", encoding="utf-8") as f:
                json.dump(casebook, f, indent=4, ensure_ascii=False)
            os.replace(tmp_path, final_path)
            
    def load(self, event_id: str, filename: str = "casebook.json") -> Optional[dict]:
        target_dir = self._get_dir(event_id)
        final_path = target_dir / filename
        lock_path = target_dir / f"{filename}.lock"
        
        if not final_path.exists():
            return None
            
        with FileLock(str(lock_path), timeout=10):
            try:
                with open(final_path, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception:
                return None
                
    def exists(self, event_id: str, terminal_only: bool = False, filename: str = "casebook.json") -> bool:
        data = self.load(event_id, filename=filename)
        if not data:
            return False
            
        if terminal_only:
            status = data.get("packet_status", {}).get("status")
            # Usually COMPLETED, REJECTED, NEEDS_MANUAL_REVIEW, FAILED_PERMANENT are terminal
            return status in ("COMPLETED", "REJECTED", "NEEDS_MANUAL_REVIEW", "FAILED_PERMANENT", "DLQ", "FAILED_TIMEOUT")
        
        return True
