"""
Client-side identifier filtering and context windows
(KUBERNETES_LOGS_PLAN.md 5.6).

The Kubernetes API has no server-side grep: `read_namespaced_pod_log` returns
the whole buffer. Filtering therefore happens here, against the streamed
lines, which is also why streaming (rather than buffering) is mandatory.

Context windows matter more than they look. A bare identifier match is often
one line of a stack trace whose useful part -- the exception, the caused-by
chain -- never repeats the identifier. Without surrounding context the
reduction pipeline receives isolated lines and the Investigator has nothing to
reason over.
"""
import os
from collections import deque
from typing import Callable, Optional


def resolve_search_values(identifier: str,
                          extra: Optional[list] = None) -> list:
    """Identifier values to match lines against.

    `K8S_SEARCH_FIELDS` names which payload fields supply values (default
    `eventId,refId`). Phase 9 plumbs the payload through so `refId` can be
    added; until then the primary identifier plus any explicit extras are
    used. Searching more than one value is cheap insurance against Open
    Question 1 -- we do not yet know which id the services actually log.
    """
    values = [identifier] if identifier else []
    for value in (extra or []):
        if value and value not in values:
            values.append(value)
    return values


def build_matcher(values: list, case_sensitive: bool = True) -> Callable[[str], bool]:
    """Return a predicate matching any of `values` in a line.

    Case-sensitive by default: identifiers are UUIDs and reference numbers, so
    a case-insensitive match buys nothing and risks false positives.
    """
    if not values:
        return lambda _line: False

    if case_sensitive:
        needles = list(values)
        return lambda line: any(needle in line for needle in needles)

    lowered = [v.lower() for v in values]
    return lambda line: any(needle in line.lower() for needle in lowered)


class KeepAllSelector:
    """Emit every line. The default when no identifier filtering applies."""

    def feed(self, line: str) -> list:
        return [line]

    def reset(self):
        pass


class ContextWindowSelector:
    """Emit matching lines plus surrounding context, merging overlaps.

    Fed one line at a time in order, returning the lines to emit for that
    input. Overlapping windows merge naturally: a fresh match resets the
    trailing counter, and lines already emitted are never re-buffered, so no
    line is emitted twice.
    """

    def __init__(self, matcher: Callable[[str], bool],
                 before: int = 5, after: int = 20):
        self._matcher = matcher
        self._before = deque(maxlen=max(0, before))
        self._after = max(0, after)
        self._after_remaining = 0

    def feed(self, line: str) -> list:
        if self._matcher(line):
            emitted = list(self._before) + [line]
            self._before.clear()
            self._after_remaining = self._after
            return emitted

        if self._after_remaining > 0:
            self._after_remaining -= 1
            return [line]

        # Not emitted: hold it as potential leading context for a later match.
        if self._before.maxlen:
            self._before.append(line)
        return []

    def reset(self):
        self._before.clear()
        self._after_remaining = 0


def context_lines_before() -> int:
    try:
        return int(os.environ.get("K8S_CONTEXT_LINES_BEFORE", "5"))
    except ValueError:
        return 5


def context_lines_after() -> int:
    try:
        return int(os.environ.get("K8S_CONTEXT_LINES_AFTER", "20"))
    except ValueError:
        return 20


def build_selector(identifier: str, extra: Optional[list] = None):
    """Build the selector for a fetch.

    An empty identifier yields `KeepAllSelector`: filtering on nothing would
    otherwise silently discard the entire trace.
    """
    values = resolve_search_values(identifier, extra)
    if not values:
        return KeepAllSelector()
    return ContextWindowSelector(
        build_matcher(values),
        before=context_lines_before(),
        after=context_lines_after(),
    )
