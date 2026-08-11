"""
Pod log retrieval (KUBERNETES_LOGS_PLAN.md 5.3 - 5.4).

Reads logs for each discovered (pod, container) target, streaming rather than
buffering, and handles in-place container restarts.

Two mandatory call parameters, both easy to omit and expensive to get wrong:

  timestamps=True
      Makes the kubelet prefix every line with an RFC3339Nano timestamp,
      giving a reliable ordering key even when the application's own log
      format is unparseable. Without it, cross-pod merging is guesswork.

  _preload_content=False
      All filtering is client-side (the API has no server-side grep), so a
      multi-hundred-megabyte log must be streamed and filtered line by line.
      Buffering 20 pods x 10MB is 200MB resident in a process that is also
      running the LangGraph agents.
"""
import os
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Callable, Iterator, Optional

from src.log_pipeline.sources.k8s import client as k8s_client_module
from src.log_pipeline.sources.k8s import fixtures
from src.log_pipeline.sources.k8s.discovery import PodTarget
from src.log_pipeline.sources.k8s.filtering import KeepAllSelector
from src.log_pipeline.sources.k8s.parser import ParseStats, parse_line
from src.log_pipeline.types import EvidenceGap, GapType, TimeWindow
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

_CHUNK_SIZE = 65536


@dataclass
class PodFetchOutcome:
    """Result of reading one (pod, container), current + previous instances."""

    target: PodTarget
    records: list = field(default_factory=list)
    gaps: list = field(default_factory=list)
    stats: ParseStats = field(default_factory=ParseStats)
    bytes_read: int = 0
    truncated: bool = False
    ok: bool = True
    error: Optional[str] = None


@dataclass
class RetrievalOutcome:
    """Aggregate across every target."""

    records: list = field(default_factory=list)
    gaps: list = field(default_factory=list)
    stats: ParseStats = field(default_factory=ParseStats)
    bytes_read: int = 0
    pods_queried: int = 0
    pods_failed: int = 0


# ======================================================================
# Configuration -- read at call time so tests can monkeypatch
# ======================================================================

def _max_bytes_per_pod() -> int:
    try:
        return int(os.environ.get("K8S_MAX_BYTES_PER_POD", str(10 * 1024 * 1024)))
    except ValueError:
        return 10 * 1024 * 1024


def _request_timeout() -> float:
    try:
        return float(os.environ.get("K8S_REQUEST_TIMEOUT_SECONDS", "30"))
    except ValueError:
        return 30.0


def _concurrency() -> int:
    try:
        return max(1, int(os.environ.get("K8S_FETCH_CONCURRENCY", "5")))
    except ValueError:
        return 5


# ======================================================================
# Streaming
# ======================================================================

def _iter_lines_from_response(response, max_bytes: int) -> Iterator:
    """Yield (line, total_bytes_so_far, hit_cap) from a streamed response.

    Chunks are reassembled around newlines with a carry buffer, so a line
    split across two chunks is not corrupted.
    """
    buffer = ""
    total = 0
    for chunk in response.stream(_CHUNK_SIZE, decode_content=True):
        if not chunk:
            continue
        if isinstance(chunk, bytes):
            total += len(chunk)
            chunk = chunk.decode("utf-8", errors="replace")
        else:
            total += len(chunk.encode("utf-8", errors="replace"))

        buffer += chunk
        *lines, buffer = buffer.split("\n")
        for line in lines:
            yield line, total, False

        if total >= max_bytes:
            if buffer:
                yield buffer, total, True
            return

    if buffer:
        yield buffer, total, False


def _iter_lines_from_text(text: str, max_bytes: int) -> Iterator:
    total = 0
    for line in text.splitlines():
        total += len(line.encode("utf-8", errors="replace")) + 1
        if total >= max_bytes:
            yield line, total, True
            return
        yield line, total, False


def _open_stream(target: PodTarget, window: TimeWindow, previous: bool):
    """Return an iterator of (line, bytes, hit_cap) for one container instance."""
    max_bytes = _max_bytes_per_pod()

    if fixtures.is_active():
        text = fixtures.read_log(
            target.namespace, target.pod_name, target.container, previous=previous
        )
        return _iter_lines_from_text(text, max_bytes)

    api = k8s_client_module.get_client()
    if api is None:
        raise RuntimeError("Kubernetes client unavailable")

    response = api.read_namespaced_pod_log(
        name=target.pod_name,
        namespace=target.namespace,
        container=target.container,
        since_seconds=window.seconds,
        timestamps=True,
        limit_bytes=max_bytes,
        _request_timeout=_request_timeout(),
        _preload_content=False,
    )
    return _iter_lines_from_response(response, max_bytes)


def _read_instance(target: PodTarget, window: TimeWindow, previous: bool,
                   selector) -> tuple:
    """Read one container instance. Returns (records, stats, bytes, truncated).

    `selector` is stateful and fed every line in order, returning the lines to
    emit -- that is what lets a match pull in the preceding context lines it
    needs. It is reset per instance so a previous container's trailing context
    cannot leak into the current one.
    """
    records, stats, total_bytes, truncated = [], ParseStats(), 0, False
    instance = "previous" if previous else "current"
    selector.reset()

    for line, total_bytes, hit_cap in _open_stream(target, window, previous):
        if line.strip():
            for emitted in selector.feed(line):
                record, level_ok, was_json = parse_line(
                    emitted, default_app=target.container
                )
                record["pod_name"] = target.pod_name
                record["container"] = target.container
                record["container_instance"] = instance
                records.append(record)

                stats.total += 1
                if level_ok:
                    stats.level_parsed += 1
                if was_json:
                    stats.json_lines += 1

        if hit_cap:
            truncated = True
            break

    return records, stats, total_bytes, truncated


def read_pod_logs(target: PodTarget, window: TimeWindow,
                  selector=None) -> PodFetchOutcome:
    """Read current -- and, after a restart, previous -- logs for one target."""
    selector = selector if selector is not None else KeepAllSelector()
    outcome = PodFetchOutcome(target=target)

    # Previous first: those lines precede the restart chronologically, and
    # they are usually the most valuable in the whole trace.
    if target.restarted:
        try:
            records, stats, read_bytes, truncated = _read_instance(
                target, window, previous=True, selector=selector
            )
            outcome.records.extend(records)
            outcome.bytes_read += read_bytes
            outcome.truncated = outcome.truncated or truncated
            _merge_stats(outcome.stats, stats)
            logger.debug(
                "Read previous container instance",
                pod_name=target.pod_name,
                restart_count=target.restart_count,
                lines=len(records),
            )
        except Exception as e:
            # A 400 here means "no previous container exists" -- an expected
            # outcome, not an error. Anything else is also non-fatal: the
            # current instance is still worth reading.
            status = getattr(e, "status", None)
            level = logger.debug if status == 400 or isinstance(e, FileNotFoundError) else logger.warning
            level(
                "Previous-instance read unavailable",
                pod_name=target.pod_name,
                error=f"{type(e).__name__}: {e}",
            )

    try:
        records, stats, read_bytes, truncated = _read_instance(
            target, window, previous=False, selector=selector
        )
        outcome.records.extend(records)
        outcome.bytes_read += read_bytes
        outcome.truncated = outcome.truncated or truncated
        _merge_stats(outcome.stats, stats)
    except Exception as e:
        status = getattr(e, "status", None)
        if status == 404:
            # The pod vanished between list and read -- skip it, keep going.
            outcome.gaps.append(EvidenceGap(
                GapType.POD_VANISHED,
                f"pod {target.pod_name} disappeared between listing and reading; "
                f"its logs are unrecoverable.",
                {"pod_name": target.pod_name, "namespace": target.namespace},
            ))
        elif status == 403:
            logger.error(
                "Kubernetes RBAC denied pod log read",
                namespace=target.namespace,
                verb="get pods/log",
            )
        outcome.ok = False
        outcome.error = f"{type(e).__name__}: {e}"
        logger.warning(
            "Pod log read failed",
            pod_name=target.pod_name,
            container=target.container,
            error=outcome.error,
        )
        return outcome

    if outcome.truncated:
        outcome.gaps.append(EvidenceGap(
            GapType.TRUNCATED,
            f"pod {target.pod_name}/{target.container} hit the "
            f"{_max_bytes_per_pod()} byte cap; older lines were not read.",
            {"pod_name": target.pod_name, "container": target.container},
        ))

    logger.debug(
        "Pod log read",
        pod_name=target.pod_name,
        container=target.container,
        lines=len(outcome.records),
        bytes=outcome.bytes_read,
    )
    return outcome


def _merge_stats(into: ParseStats, other: ParseStats):
    into.total += other.total
    into.level_parsed += other.level_parsed
    into.json_lines += other.json_lines


# ======================================================================
# Fan-out
# ======================================================================

def read_all(targets: list, window: TimeWindow,
             selector_factory=None) -> RetrievalOutcome:
    """Read every target with bounded parallelism.

    Sequential reads across 10+ replicas would be too slow inside a request
    already bounded by AGENT_INVOKE_TIMEOUT_SECONDS; unbounded parallelism
    invites API-server 429s. Bounded, like the executor in remediation 2.6.
    """
    outcome = RetrievalOutcome()
    if not targets:
        return outcome

    workers = min(_concurrency(), len(targets))
    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="k8s-logs") as pool:
        # Each target gets its OWN selector: the selector is stateful, and
        # sharing one across concurrent pods would interleave their context
        # buffers and emit lines against the wrong pod.
        results = list(pool.map(
            lambda t: read_pod_logs(
                t, window,
                selector_factory() if selector_factory else KeepAllSelector(),
            ),
            targets,
        ))

    for result in results:
        outcome.pods_queried += 1
        if not result.ok:
            outcome.pods_failed += 1
        outcome.records.extend(result.records)
        outcome.gaps.extend(result.gaps)
        outcome.bytes_read += result.bytes_read
        _merge_stats(outcome.stats, result.stats)

    outcome.records.sort(key=_ordering_key)
    return outcome


def _ordering_key(record):
    """Order by kubelet timestamp, with a pod's previous instance before its
    current one so a restart reads chronologically.

    Ordering across pods is approximate: kubelet timestamps come from node
    clocks, which drift relative to each other. Records carry `pod_name` so a
    line can be attributed to a replica rather than relying on interleaving.
    """
    return (
        record.get("timestamp") or "",
        0 if record.get("container_instance") == "previous" else 1,
    )
