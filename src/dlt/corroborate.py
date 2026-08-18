"""Phase 6 of DLT_PLAN.md -- checking the declared trace against the logs.

**The stack trace is a claim, not ground truth.** Application code catches a
technical fault and rethrows it as a business exception; when that happens the
trace confidently names the wrong root cause and every consumer of it -- human
or machine -- inherits the error. Logs from the same pod at the same instant
are the only available check.

Surfacing that discrepancy is the highest-value output of this system, because
it is the one thing a developer reading the trace in Kafka UI structurally
cannot see. It is also the entire justification for the log lane: without it,
the headers alone would do.

    CORROBORATED   the declared root appears in the logs
    PARTIAL        it appears, but so do unexplained errors
    CONTRADICTED   it does not appear, and something else failed instead
    UNVERIFIABLE   no logs, no identifier, or nothing error-level matched

A verdict never adjudicates on its own. `CONTRADICTED` escalates to the LLM
lane with the discrepancy as the framing question, and every verdict carries
the specific lines it relied on so a human can check the machine's reading.

**No real mis-cast example exists yet** (Open Question 2). The thresholds are
deliberately conservative and the verdict is advisory until real samples
validate it.

Pure functions, no I/O.
"""
import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional, Sequence

#: Lines the reducer emits for an error branch, and the level markers inside
#: them. Matching on word boundaries so "TERROR" or "error_code=0" do not count.
_ERROR_LINE = re.compile(r"\b(ERROR|FATAL|SEVERE)\b")

#: A Java exception FQCN appearing in free log text.
_FQCN_IN_TEXT = re.compile(r"\b((?:[a-z][\w$]*\.){2,}[A-Z][\w$]*(?:Exception|Error|Throwable))\b")

#: How many cited lines to keep per verdict. Enough for a human to judge,
#: bounded so a 4,000-line retry storm cannot land in the casebook.
MAX_CITATIONS = 20


class Verdict(str, Enum):
    CORROBORATED = "CORROBORATED"
    PARTIAL = "PARTIAL"
    CONTRADICTED = "CONTRADICTED"
    UNVERIFIABLE = "UNVERIFIABLE"


@dataclass(frozen=True)
class Corroboration:
    verdict: Verdict
    reason: str
    #: Log lines the verdict rests on, so a human can check the reading.
    citations: tuple = ()
    #: Exception FQCNs seen in the logs that the trace does not explain.
    unexplained: tuple = ()
    matched_declared: bool = False
    error_lines_seen: int = 0
    #: Distinguishes "could not look" from "looked and found nothing", the
    #: same distinction `FetchResult.ok` makes in the log pipeline. Collapsing
    #: them would let a finding read as "no errors occurred" when the truth is
    #: "we could not read the logs".
    could_not_look: bool = False
    details: dict = field(default_factory=dict)

    @property
    def is_discrepancy(self) -> bool:
        return self.verdict in (Verdict.CONTRADICTED, Verdict.PARTIAL)


#: Text the fetch stage writes when it deliberately skipped the fetch. These
#: mean "could not look", not "looked and found nothing".
_SKIP_MARKERS = (
    "No refId available",
    "No usable timestamp",
    "Log window too old",
    "Log fetch failed",
    "Log fetching disabled",
)

_EMPTY_MARKERS = ("No logs found for ID:",)


def _simple_name(fqcn: Optional[str]) -> str:
    return fqcn.rsplit(".", 1)[-1] if fqcn else ""


def _error_lines(logs: str) -> list:
    return [line.strip() for line in logs.splitlines() if _ERROR_LINE.search(line)]


def corroborate(logs: Optional[str],
                root_fqcn: Optional[str],
                business_code: Optional[str] = None,
                frames: Optional[Sequence] = None) -> Corroboration:
    """Compare a declared root cause against the fetched logs.

    Matching is deliberately generous about *how* the declared root appears --
    the full FQCN, its simple name, or its business code all count. Services
    log exceptions in all three shapes, and a false CONTRADICTED is far more
    damaging than a missed one: it would tell a developer their trace is lying
    when it is not.
    """
    if not logs or not logs.strip():
        return Corroboration(Verdict.UNVERIFIABLE, "no logs were fetched",
                             could_not_look=True)

    if any(marker in logs for marker in _SKIP_MARKERS):
        return Corroboration(Verdict.UNVERIFIABLE,
                             "the log fetch was skipped or failed",
                             could_not_look=True)

    if any(marker in logs for marker in _EMPTY_MARKERS):
        return Corroboration(Verdict.UNVERIFIABLE,
                             "the log source returned no lines for this identifier",
                             could_not_look=False)

    error_lines = _error_lines(logs)
    if not error_lines:
        return Corroboration(
            Verdict.UNVERIFIABLE,
            "logs were fetched but contain no error-level lines",
            error_lines_seen=0,
            could_not_look=False,
        )

    haystack = "\n".join(error_lines)
    simple = _simple_name(root_fqcn)

    matched = False
    matched_on = None
    for needle, label in ((root_fqcn, "fqcn"), (simple, "simple name"),
                          (business_code, "business code")):
        if needle and needle in haystack:
            matched, matched_on = True, label
            break

    # Frames are corroborating, not decisive: seeing the failing method in the
    # logs supports the trace without proving which exception left it.
    frame_hits = tuple(
        frame for frame in (frames or [])
        if frame and frame.rsplit(".", 1)[-1] and frame.rsplit(".", 1)[-1] in haystack
    )

    seen_fqcns = set(_FQCN_IN_TEXT.findall(haystack))
    unexplained = tuple(sorted(
        fqcn for fqcn in seen_fqcns
        if fqcn != root_fqcn and _simple_name(fqcn) != simple
    ))

    citations = tuple(error_lines[:MAX_CITATIONS])
    details = {
        "matched_on": matched_on,
        "frame_hits": list(frame_hits),
        "exceptions_in_logs": sorted(seen_fqcns),
    }

    if matched and not unexplained:
        return Corroboration(
            Verdict.CORROBORATED,
            f"the declared root appears in the logs (matched on its {matched_on})",
            citations=citations, matched_declared=True,
            error_lines_seen=len(error_lines), details=details)

    if matched and unexplained:
        return Corroboration(
            Verdict.PARTIAL,
            "the declared root appears, but the window also holds errors it "
            "does not explain",
            citations=citations, unexplained=unexplained, matched_declared=True,
            error_lines_seen=len(error_lines), details=details)

    if unexplained:
        # The mis-cast case: the trace claims one thing, the logs show another
        # at the same instant.
        return Corroboration(
            Verdict.CONTRADICTED,
            f"the declared root ({simple or 'unknown'}) does not appear in the "
            f"logs, but {', '.join(_simple_name(f) for f in unexplained[:3])} did",
            citations=citations, unexplained=unexplained, matched_declared=False,
            error_lines_seen=len(error_lines), details=details)

    # Errors present, but nothing named -- not enough to contradict anything.
    return Corroboration(
        Verdict.UNVERIFIABLE,
        "error lines were found but none names an exception type",
        citations=citations, error_lines_seen=len(error_lines),
        could_not_look=False, details=details)
