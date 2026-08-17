from typing import Protocol, Optional

#: Casebook schema version written by the storage layer.
#: 1.1 -> 1.2 adds the optional `resolution_outcome` block (section 4.1), which
#: records whether a resolution was actually correct. It is additive: a 1.1
#: casebook is a valid 1.2 casebook with no outcome recorded yet.
CASEBOOK_SCHEMA_VERSION = "1.2"

#: Verdicts an operator can attach to a resolution.
OUTCOME_VERDICTS = ("CORRECT", "INCORRECT", "PARTIAL")

#: Statuses that end a packet's lifecycle. Defined here rather than duplicated
#: across routes.py, kafkaConsumer.py and local.py, which each carried their
#: own copy and had already drifted (F4).
TERMINAL_STATUSES = (
    "COMPLETED",
    "REJECTED",
    "NEEDS_MANUAL_REVIEW",
    "FAILED_PERMANENT",
    "DLQ",
    "FAILED_TIMEOUT",
)

#: Terminal statuses recorded by an actor *other* than the API's own success
#: path -- a late-finishing agent run must never overwrite one of these with a
#: "successful" result.
PROTECTED_TERMINAL_STATUSES = ("FAILED_TIMEOUT", "DLQ")


class CasebookStorage(Protocol):
    def save(self, event_id: str, casebook: dict, filename: str = "casebook.json") -> None:
        """Save the casebook persistently."""
        ...

    def save_terminal(self, event_id: str, casebook: dict) -> None:
        """Record a terminal outcome in casebook.json AND status.json together.

        The two files must reach a terminal status as one operation. The
        consumer's timeout handler used to write only casebook.json while the
        API's late-result guard read only status.json, so a slow agent run
        could overwrite a FAILED_TIMEOUT casebook with COMPLETED while its DLQ
        message stayed queued (F4).
        """
        ...

    def load(self, event_id: str, filename: str = "casebook.json") -> Optional[dict]:
        """Load the casebook for the given event ID."""
        ...

    def exists(self, event_id: str, terminal_only: bool = False, filename: str = "casebook.json") -> bool:
        """Check if a casebook exists. If terminal_only is True, returns True only if status is a terminal state."""
        ...

    def terminal_status(self, event_id: str) -> Optional[str]:
        """Return the terminal status recorded in EITHER file, or None.

        Checking both is what closes F4: whichever actor got there first, its
        verdict is visible to every other actor.
        """
        ...
