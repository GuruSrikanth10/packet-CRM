import os
import json
from pathlib import Path
from typing import Optional
from filelock import FileLock
from src.storage.base import (
    CASEBOOK_SCHEMA_VERSION,
    CasebookStorage,
    TERMINAL_STATUSES,
)
from src.utils.paths import LOCAL_CASESHEETS_DIR

class LocalFilesystemCasebookStorage(CasebookStorage):
    def __init__(self, base_dir: str = None):
        if base_dir:
            self.base_dir = Path(base_dir)
        else:
            self.base_dir = LOCAL_CASESHEETS_DIR


        os.makedirs(self.base_dir, exist_ok=True)
        
    def _resolve_dir(self, event_id: str) -> Path:
        """Resolve the casebook directory for event_id without creating it."""
        base_resolved = self.base_dir.resolve()
        target_dir = (self.base_dir / f"casebook_{event_id}").resolve()

        # Defense in depth alongside the Pydantic eventId pattern (0.11):
        # never create or touch a path that resolves outside the storage root.
        try:
            target_dir.relative_to(base_resolved)
        except ValueError:
            raise ValueError(f"Resolved casebook directory escapes storage root: {target_dir}")

        return target_dir

    def _get_dir(self, event_id: str) -> Path:
        """Resolve and create the casebook directory for event_id.

        Only save() should create directories -- load()/exists() calling
        this used to create an empty casebook_<id>/ directory for every
        existence check, including events that were skipped entirely (1.17).
        """
        target_dir = self._resolve_dir(event_id)
        os.makedirs(target_dir, exist_ok=True)
        return target_dir

    def save(self, event_id: str, casebook: dict, filename: str = "casebook.json") -> None:
        target_dir = self._get_dir(event_id)
        final_path = target_dir / filename
        tmp_path = target_dir / f"{filename}.tmp"
        lock_path = target_dir / f"{filename}.lock"

        # Enforce schema version for backwards compatibility
        if "schema_version" not in casebook:
            casebook["schema_version"] = CASEBOOK_SCHEMA_VERSION

        with FileLock(str(lock_path), timeout=10):
            with open(tmp_path, "w", encoding="utf-8") as f:
                json.dump(casebook, f, indent=4, ensure_ascii=False)
            os.replace(tmp_path, final_path)

    def save_terminal(self, event_id: str, casebook: dict) -> None:
        """Write casebook.json then status.json for one terminal outcome.

        casebook.json is written first: if the process dies between the two
        writes, a stale IN_PROGRESS status.json goes stale on its own after
        MAX_IN_PROGRESS_AGE_SECONDS, whereas a terminal status.json with no
        casebook behind it would suppress reprocessing forever.
        """
        self.save(event_id, casebook)

        status = (casebook.get("packet_status") or {}).get("status")
        self.save(event_id, {
            "packet_metadata": {"eid": event_id},
            "packet_status": {"status": status},
            "resolution": {
                "synthesis": (casebook.get("resolution") or {}).get("synthesis")
            },
        }, filename="status.json")

    def terminal_status(self, event_id: str) -> Optional[str]:
        """Return the terminal status found in either file, or None."""
        for filename in ("status.json", "casebook.json"):
            data = self.load(event_id, filename=filename)
            if not data:
                continue
            status = (data.get("packet_status") or {}).get("status")
            if status in TERMINAL_STATUSES:
                return status
        return None

    def load(self, event_id: str, filename: str = "casebook.json") -> Optional[dict]:
        target_dir = self._resolve_dir(event_id)
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
            return data.get("packet_status", {}).get("status") in TERMINAL_STATUSES

        return True
