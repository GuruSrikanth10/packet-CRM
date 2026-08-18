"""Phase 3 of DLT_PLAN.md -- the DLT wire model.

What the DLT consumer POSTs to the API, and what it republishes onto the
analysis queue. Deliberately *not* `MessagePayload`: that model requires
`eventId` and `packetExecutionSummary`, neither of which exists on a DLT
message, so validating one against the other would reject every message.

The model carries raw headers and the raw payload, not a parsed failure.
Parsing is pure and deterministic (`src/dlt/stacktrace.py`), so the API
re-derives it rather than trusting a client-supplied summary -- one parser,
one result, no chance of the two drifting apart.
"""
from typing import Any, Dict, Optional

from pydantic import BaseModel, ConfigDict, Field

from src.models.schemas import EVENT_ID_PATTERN


class DltMessage(BaseModel):
    """One dead-lettered record, on its way to analysis."""

    # `extra="allow"` for the same reason `PacketMetaData` allows it: upstream
    # adds fields, and rejecting a message over an unknown key would turn a
    # schema addition into an outage.
    model_config = ConfigDict(extra="allow")

    #: `dlt-{topic}-{partition}-{offset}`. Interpolated into storage paths, so
    #: it carries the same pattern guard as `eventId`.
    case_id: str = Field(pattern=EVENT_ID_PATTERN)

    #: Spring DLT headers, decoded to str. Values may be None; a header whose
    #: value did not decode is kept rather than dropped.
    headers: Dict[str, Optional[str]] = Field(default_factory=dict)

    #: The deserialised payload. `Any` rather than a schema: this is upstream's
    #: contract, we only read an identifier out of it, and one payload shape we
    #: failed to anticipate must not cost us the message.
    payload: Optional[Any] = None

    #: Set only when the payload could not be parsed as JSON, so the raw text
    #: is still available for a human.
    payload_raw: Optional[str] = None

    #: Log-correlation identifier extracted from the payload. None is a valid
    #: state: the case proceeds header-only and skips the log lane.
    ref_id: Optional[str] = None
