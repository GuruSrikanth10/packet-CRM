"""Phase 1 of DLT_PLAN.md -- stacktrace parsing, frame normalisation, fingerprinting.

The load-bearing module. Everything downstream -- grouping, recommendation
reuse, cost control -- depends on the fingerprint being *stable* across
occurrences of one bug and *distinct* across genuinely different bugs.

Two failure modes, both silent, both fatal:

* Over-grouping. Key on the wrapper exception and every failure in every
  Spring Kafka consumer collapses into one bucket, and one wrong
  recommendation is then served to all of them. This is why the root is taken
  from the *last* `Caused by:` and never from `kafka_exception-cause-fqcn`
  (DLT_PLAN.md 3.2, Trap 1).

* Under-grouping. Leave a line number or a JVM-generated class name in the
  fingerprint and no two occurrences of the same bug ever match, so the cache
  never hits and LLM cost scales with the full 2,000/day message rate.

Pure functions, no I/O.
"""
import hashlib
import os
import re
from dataclasses import dataclass
from typing import Optional, Sequence

#: Packages considered "ours". Frames outside these are framework or JDK noise
#: and are dropped before fingerprinting -- the reference sample's chain holds
#: 60+ frames, of which 9 are application frames.
DEFAULT_APP_PACKAGES = ("com.uidai.", "in.gov.uidai.")

DEFAULT_FINGERPRINT_FRAMES = 5

#: Application frames that are *exception plumbing* rather than failure sites.
#:
#: Found by running Phase 1 against the reference sample: the top application
#: frame of a `BusinessException` is `CommonErrorFactory.instantiateException`,
#: because that factory constructs every business exception in the codebase.
#: It is therefore identical for every Class A failure -- it contributes no
#: discriminating power to the fingerprint, displaces a frame that would, and
#: makes the human-readable signature name the factory instead of the code
#: that actually failed.
#:
#: Inferred from one sample. Phase 0's corpus should confirm it and reveal any
#: siblings; override with `DLT_BOILERPLATE_FRAMES` (empty value disables).
DEFAULT_BOILERPLATE_FRAMES = ("in.gov.uidai.common.factory.CommonErrorFactory",)

#: `\tat com.foo.Bar.baz(Bar.java:42)`. The location group is optional so a
#: frame written without one still parses.
_FRAME_RE = re.compile(r"^\s*at\s+(?P<target>[^\s(]+)(?:\((?P<location>.*)\))?\s*$")

#: `\t... 14 more` -- frames shared with the enclosing exception.
_ELIDED_RE = re.compile(r"^\s*\.\.\.\s+(?P<count>\d+)\s+more\s*$")

#: A plausible Java binary name, inner classes included.
_FQCN_RE = re.compile(r"^[A-Za-z_$][\w$]*(?:\.[A-Za-z_$][\w$]*)*$")

#: JVM- and framework-generated classes whose names carry a counter that
#: changes between runs, deployments, or even class-loads. Every one of these
#: appears in the reference sample. Left in, they guarantee that no two
#: occurrences of the same bug ever fingerprint alike.
#:
#: Note this deliberately requires a doubled `$$`, so a genuine application
#: lambda method (`AbstractBaseConsumer.lambda$executeConsumption$0`) survives:
#: its index is assigned at compile time and is stable across runs.
_SYNTHETIC_RE = re.compile(
    r"\$\$(?:SpringCGLIB|EnhancerBySpringCGLIB|FastClassBySpringCGLIB|Lambda)"
    r"|GeneratedMethodAccessor\d+"
    r"|GeneratedConstructorAccessor\d+"
    r"|\$Proxy\d+"
)

_CAUSED_BY = "\nCaused by: "


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

def app_packages() -> tuple:
    raw = os.environ.get("DLT_APP_PACKAGES", "").strip()
    if not raw:
        return DEFAULT_APP_PACKAGES
    parsed = tuple(p.strip() for p in raw.split(",") if p.strip())
    return parsed or DEFAULT_APP_PACKAGES


def boilerplate_frames() -> tuple:
    """Frame prefixes to drop as exception plumbing.

    Unset uses the default; explicitly empty disables the filter entirely, so
    an operator who disagrees with the default can turn it off without a code
    change (the fingerprints then shift, which is why it is config).
    """
    raw = os.environ.get("DLT_BOILERPLATE_FRAMES")
    if raw is None:
        return DEFAULT_BOILERPLATE_FRAMES
    return tuple(p.strip() for p in raw.split(",") if p.strip())


def fingerprint_frame_count() -> int:
    try:
        return max(1, int(os.environ.get("DLT_FINGERPRINT_FRAMES",
                                         str(DEFAULT_FINGERPRINT_FRAMES))))
    except (ValueError, TypeError):
        return DEFAULT_FINGERPRINT_FRAMES


# ---------------------------------------------------------------------------
# Parsed shapes
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class ExceptionLink:
    """One `Caused by:` level."""

    fqcn: str
    message: str
    #: Raw frame targets, module prefix and location stripped, in trace order.
    frames: tuple
    #: The `... N more` count, when the link ends with one.
    elided: Optional[int] = None

    @property
    def simple_name(self) -> str:
        return self.fqcn.rsplit(".", 1)[-1] if self.fqcn else ""


@dataclass(frozen=True)
class ParsedTrace:
    """A full exception chain, outermost link first."""

    chain: tuple
    truncated: bool

    @property
    def root(self) -> Optional[ExceptionLink]:
        """The innermost cause -- the only link worth fingerprinting."""
        return self.chain[-1] if self.chain else None

    @property
    def root_frames(self) -> tuple:
        root = self.root
        return root.frames if root else ()

    @property
    def depth(self) -> int:
        return len(self.chain)


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------

def _strip_module(target: str) -> str:
    """`java.base/java.lang.Thread.run` -> `java.lang.Thread.run`."""
    return target.split("/", 1)[-1] if "/" in target else target


def _parse_link(block: str) -> ExceptionLink:
    """Parse one chain link: a header, then frames, then an optional elision.

    The header may span several lines -- an exception message is free text and
    can contain newlines -- so it is everything up to the first frame line.
    """
    header_lines = []
    frames = []
    elided = None
    in_frames = False

    for line in block.split("\n"):
        frame_match = _FRAME_RE.match(line)
        if frame_match:
            in_frames = True
            frames.append(_strip_module(frame_match.group("target")))
            continue

        elided_match = _ELIDED_RE.match(line)
        if elided_match:
            in_frames = True
            elided = int(elided_match.group("count"))
            continue

        if not in_frames:
            header_lines.append(line)

    header = "\n".join(header_lines).strip()

    fqcn, message = "", header
    if header:
        candidate_fqcn, _, candidate_message = header.partition(": ")
        if _FQCN_RE.match(candidate_fqcn):
            fqcn, message = candidate_fqcn, candidate_message.strip()
        elif _FQCN_RE.match(header):
            # A class name with no message at all.
            fqcn, message = header, ""

    return ExceptionLink(fqcn=fqcn, message=message,
                         frames=tuple(frames), elided=elided)


def parse_stacktrace(text: Optional[str]) -> ParsedTrace:
    """Split a Java stacktrace into its `Caused by:` chain.

    Never raises. A stacktrace that is absent, empty, or cut mid-frame yields
    a `ParsedTrace` flagged `truncated`, which downstream phases treat as
    Class U rather than fingerprinting a wrapper by mistake.
    """
    if not text or not text.strip():
        return ParsedTrace(chain=(), truncated=True)

    chain = tuple(_parse_link(block) for block in text.split(_CAUSED_BY))

    # A well-formed link ends in either frames or a `... N more` elision.
    # A final link with neither means the header cut the trace short.
    last = chain[-1]
    truncated = not last.frames and last.elided is None

    return ParsedTrace(chain=chain, truncated=truncated)


# ---------------------------------------------------------------------------
# Normalisation and fingerprinting
# ---------------------------------------------------------------------------

def is_synthetic(target: str) -> bool:
    """Is this frame a JVM/framework-generated class with an unstable name?"""
    return bool(_SYNTHETIC_RE.search(target or ""))


def normalise_frames(frames: Sequence,
                     packages: Optional[Sequence] = None,
                     boilerplate: Optional[Sequence] = None) -> tuple:
    """Reduce raw frame targets to the stable application-code subset.

    Three filters, in order: keep only application packages, drop
    JVM/framework-generated classes, drop exception plumbing.

    Line numbers are already absent -- `_parse_link` keeps only the target,
    discarding the `(File.java:4067)` location. `BioDeDuplicationServiceImpl`
    is a 4,000+ line class whose line numbers shift on every release, so
    keeping them would fragment a group that should be stable
    (DLT_PLAN.md 9.3, an accepted tradeoff: two distinct bugs in one method
    will merge).
    """
    prefixes = tuple(packages) if packages else app_packages()
    noise = tuple(boilerplate) if boilerplate is not None else boilerplate_frames()
    return tuple(
        frame for frame in frames
        if frame
        and frame.startswith(prefixes)
        and not is_synthetic(frame)
        and not (noise and frame.startswith(noise))
    )


def compute_fingerprint(root_fqcn: Optional[str],
                        normalised_frames: Sequence,
                        business_code: str = "",
                        limit: Optional[int] = None) -> str:
    """Stable SHA256 identity for one failure mode.

    `business_code` is supplied by Phase 2's classifier; Phase 1 computes
    fingerprints without one. It is a distinct dimension rather than part of
    the message because two different codes raised from the same frame are
    genuinely different failures.
    """
    count = fingerprint_frame_count() if limit is None else max(1, limit)
    payload = "|".join((
        (root_fqcn or "").strip(),
        (business_code or "").strip(),
        "\n".join(normalised_frames[:count]),
    ))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def build_signature(root_fqcn: Optional[str],
                    normalised_frames: Sequence,
                    business_code: str = "") -> str:
    """A human-readable label for a fingerprint.

    `BusinessException[UID_ORIGIN_TRACKER_DATA_NOT_FOUND] @ BioDataBaseHelperServiceImpl.getUidOriginTrackerData`

    An operator reads this in `dlt_report`; the hash is for machines.
    """
    simple = root_fqcn.rsplit(".", 1)[-1] if root_fqcn else "UnknownException"
    code = f"[{business_code}]" if business_code else ""

    if not normalised_frames:
        return f"{simple}{code}"

    top = normalised_frames[0]
    parts = top.rsplit(".", 2)
    location = ".".join(parts[-2:]) if len(parts) >= 2 else top
    return f"{simple}{code} @ {location}"
