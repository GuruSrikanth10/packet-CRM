"""Phase 3 of DLT_PLAN.md -- extracting the log-correlation identifier.

`refId` is the only thing that ties a DLT message to lines in the service's pod
logs, and the DLT headers do not carry it (DLT_PLAN.md 3.1) -- it lives inside
the payload. Its exact path is Open Question 1, so extraction is deliberately
built in two layers: a configured path, then a bounded search. Getting the path
wrong is then a config change, not a code change.

A payload with no recoverable identifier is **not** an error. The case is still
recorded from its headers alone; the log lane is skipped and corroboration
comes out `UNVERIFIABLE`. Losing the message would be worse than losing its
logs.

Pure functions, no I/O.
"""
import os
from collections import deque
from typing import Any, Optional

#: Keys searched when no configured path matches. Ordered by likelihood.
DEFAULT_REFID_KEYS = ("refId", "ref_id", "referenceId")

#: Depth cap on the fallback search. A malformed or hostile payload must not
#: turn identifier extraction into an unbounded walk.
MAX_SEARCH_DEPTH = 8

#: Node cap, for the same reason: a wide payload is as expensive as a deep one.
MAX_SEARCH_NODES = 2000


def refid_path() -> Optional[str]:
    return os.environ.get("DLT_REFID_PATH", "").strip() or None


def refid_keys() -> tuple:
    raw = os.environ.get("DLT_REFID_KEYS", "").strip()
    if not raw:
        return DEFAULT_REFID_KEYS
    parsed = tuple(k.strip() for k in raw.split(",") if k.strip())
    return parsed or DEFAULT_REFID_KEYS


def _scalar(value: Any) -> Optional[str]:
    """Accept only scalars an identifier can plausibly be.

    A dict or list at the target path means the path is wrong, not that the
    identifier is a dict -- returning `str(value)` there would silently poison
    every log query with `{'refId': ...}`.
    """
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (str, int)):
        text = str(value).strip()
        return text or None
    return None


def extract_by_path(payload: Any, path: Optional[str]) -> Optional[str]:
    """Follow a dotted path. Numeric segments index into lists.

    `packetMetaData.refId`, `events.0.refId`. Any missing segment yields None
    rather than raising -- a stale configured path degrades to the fallback
    search instead of dropping the message.
    """
    if not path or payload is None:
        return None

    node = payload
    for segment in path.split("."):
        if isinstance(node, dict):
            if segment not in node:
                return None
            node = node[segment]
        elif isinstance(node, list):
            if not segment.lstrip("-").isdigit():
                return None
            try:
                node = node[int(segment)]
            except IndexError:
                return None
        else:
            return None
    return _scalar(node)


def extract_by_search(payload: Any,
                      keys: Optional[tuple] = None,
                      max_depth: int = MAX_SEARCH_DEPTH) -> Optional[str]:
    """Breadth-first search for a refId-shaped key.

    Breadth-first rather than depth-first so a top-level identifier wins over
    one buried inside a nested candidate list -- in the reference trace the
    failing lookup is for a dedup *candidate*, so a nested refId may well
    belong to a different packet than the one that failed.
    """
    if payload is None:
        return None

    wanted = keys or refid_keys()
    queue = deque([(payload, 0)])
    visited = 0

    while queue:
        node, depth = queue.popleft()
        visited += 1
        if depth > max_depth or visited > MAX_SEARCH_NODES:
            break

        if isinstance(node, dict):
            for key in wanted:
                if key in node:
                    found = _scalar(node[key])
                    if found:
                        return found
            for value in node.values():
                if isinstance(value, (dict, list)):
                    queue.append((value, depth + 1))
        elif isinstance(node, list):
            for value in node:
                if isinstance(value, (dict, list)):
                    queue.append((value, depth + 1))

    return None


def extract_ref_id(payload: Any) -> Optional[str]:
    """The configured path first, then the bounded search."""
    return extract_by_path(payload, refid_path()) or extract_by_search(payload)
