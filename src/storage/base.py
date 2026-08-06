from typing import Protocol, Optional

class CasebookStorage(Protocol):
    def save(self, event_id: str, casebook: dict, filename: str = "casebook.json") -> None:
        """Save the casebook persistently."""
        ...
        
    def load(self, event_id: str, filename: str = "casebook.json") -> Optional[dict]:
        """Load the casebook for the given event ID."""
        ...
        
    def exists(self, event_id: str, terminal_only: bool = False, filename: str = "casebook.json") -> bool:
        """Check if a casebook exists. If terminal_only is True, returns True only if status is a terminal state."""
        ...
