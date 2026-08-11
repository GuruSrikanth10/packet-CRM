"""
Pod and container discovery (KUBERNETES_LOGS_PLAN.md 5.2).

No Kubernetes API aggregates logs across a Deployment, so the source lists
pods by label selector and iterates. This module decides *which* pods and
*which* containers within them.
"""
import json
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

from src.log_pipeline.sources.k8s import client as k8s_client_module
from src.log_pipeline.sources.k8s import fixtures
from src.log_pipeline.types import EvidenceGap, GapType
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

#: Pods in this phase have no logs yet, so they are skipped silently.
SKIPPED_PHASES = frozenset({"Pending"})

DEFAULT_SIDECAR_DENYLIST = "istio-proxy,linkerd-proxy,vault-agent"


@dataclass(frozen=True)
class PodTarget:
    """One (pod, container) pair to read logs from."""

    namespace: str
    pod_name: str
    container: str
    restart_count: int = 0
    phase: str = "Unknown"
    start_time: Optional[datetime] = None

    @property
    def restarted(self) -> bool:
        """True when a `previous=True` read is worth attempting (plan 5.4)."""
        return self.restart_count > 0


@dataclass
class DiscoveryResult:
    targets: list = field(default_factory=list)
    gaps: list = field(default_factory=list)
    pods_seen: int = 0
    pods_skipped_pending: int = 0
    truncated: bool = False
    ok: bool = True
    reason: Optional[str] = None

    @property
    def is_empty(self) -> bool:
        return not self.targets


# ======================================================================
# Configuration -- all read at call time so tests can monkeypatch
# ======================================================================

def _service_map() -> dict:
    raw = os.environ.get("K8S_SERVICE_MAP", "").strip()
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except Exception as e:
        logger.warning("K8S_SERVICE_MAP is not valid JSON; ignoring", error=str(e))
        return {}


def resolve_service(app: Optional[str] = None,
                    namespace: Optional[str] = None) -> tuple:
    """Resolve a logical app name to (namespace, label_selector).

    `K8S_SERVICE_MAP` keeps selector syntax out of Kafka payloads and lets an
    operator correct a mapping without a code change. Falls back to the
    conventional `app=<name>`.
    """
    app = app or os.environ.get("K8S_DEFAULT_APP") or "enu-biometric"
    mapping = _service_map().get(app, {})

    resolved_namespace = (
        namespace
        or mapping.get("namespace")
        or os.environ.get("K8S_DEFAULT_NAMESPACE")
    )
    selector = mapping.get("label_selector") or f"app={app}"
    return resolved_namespace, selector


def _sidecar_denylist() -> set:
    raw = os.environ.get("K8S_SIDECAR_DENYLIST", DEFAULT_SIDECAR_DENYLIST)
    return {name.strip() for name in raw.split(",") if name.strip()}


def _max_pods() -> int:
    try:
        return max(1, int(os.environ.get("K8S_MAX_PODS", "20")))
    except ValueError:
        return 20


# ======================================================================
# Selection
# ======================================================================

def select_containers(pod) -> list:
    """Container names worth reading from `pod`.

    Sidecars are dropped, but if the denylist would remove *every* container
    the original list is kept: returning nothing would silently produce an
    empty trace, which is worse than reading a proxy's logs.
    """
    spec = getattr(pod, "spec", None)
    containers = [c.name for c in (getattr(spec, "containers", None) or [])]
    if not containers:
        return []

    denied = _sidecar_denylist()
    kept = [name for name in containers if name not in denied]
    if not kept:
        logger.debug(
            "Sidecar denylist removed every container; keeping all",
            pod=pod.metadata.name,
            containers=containers,
        )
        return containers
    return kept


def _restart_count(pod, container_name: str) -> int:
    statuses = getattr(getattr(pod, "status", None), "container_statuses", None) or []
    for status in statuses:
        if status.name == container_name:
            return int(status.restart_count or 0)
    return 0


def _start_time(pod) -> Optional[datetime]:
    return getattr(getattr(pod, "status", None), "start_time", None)


def _sort_key(pod):
    """Most recently started first; pods without a start time sort last."""
    started = _start_time(pod)
    if started is None:
        return datetime.min.replace(tzinfo=timezone.utc)
    if started.tzinfo is None:
        started = started.replace(tzinfo=timezone.utc)
    return started


def _list_pods(namespace: str, selector: str, request_timeout: float):
    """List pods from fixtures when configured, otherwise from the API."""
    if fixtures.is_active():
        return fixtures.list_pods(namespace, selector)

    api = k8s_client_module.get_client()
    if api is None:
        return None

    response = api.list_namespaced_pod(
        namespace=namespace,
        label_selector=selector,
        timeout_seconds=int(request_timeout),
        _request_timeout=request_timeout,
    )
    return list(response.items or [])


def discover_targets(namespace: Optional[str] = None,
                     app: Optional[str] = None) -> DiscoveryResult:
    """Find every (pod, container) to read for this service."""
    resolved_namespace, selector = resolve_service(app=app, namespace=namespace)

    if not resolved_namespace:
        return DiscoveryResult(
            ok=False,
            reason="no namespace configured (set K8S_DEFAULT_NAMESPACE or K8S_SERVICE_MAP)",
        )

    if not fixtures.is_active() and not k8s_client_module.is_available():
        return DiscoveryResult(
            ok=False,
            reason=k8s_client_module.unavailable_reason() or "Kubernetes client unavailable",
        )

    request_timeout = float(os.environ.get("K8S_REQUEST_TIMEOUT_SECONDS", "30"))
    try:
        pods = _list_pods(resolved_namespace, selector, request_timeout)
    except Exception as e:
        # A 403 here is an RBAC misconfiguration, not a transient fault. It is
        # surfaced distinctly rather than folded into an empty result.
        status = getattr(e, "status", None)
        if status == 403:
            logger.error(
                "Kubernetes RBAC denied pod list",
                namespace=resolved_namespace,
                verb="list",
            )
        else:
            logger.error(
                "Kubernetes pod list failed",
                namespace=resolved_namespace,
                error=f"{type(e).__name__}: {e}",
            )
        return DiscoveryResult(ok=False, reason=f"{type(e).__name__}: {e}")

    if pods is None:
        return DiscoveryResult(
            ok=False,
            reason=k8s_client_module.unavailable_reason() or "Kubernetes client unavailable",
        )

    pods_seen = len(pods)
    gaps = []

    # Skip only Pending. Failed and Succeeded pods are INCLUDED -- a
    # terminated pod frequently holds the exact crash evidence the
    # investigation needs, and filtering to Running is the most common
    # mistake in code like this.
    eligible, skipped_pending = [], 0
    for pod in pods:
        phase = getattr(getattr(pod, "status", None), "phase", None) or "Unknown"
        if phase in SKIPPED_PHASES:
            skipped_pending += 1
            continue
        eligible.append(pod)

    eligible.sort(key=_sort_key, reverse=True)

    cap = _max_pods()
    truncated = len(eligible) > cap
    if truncated:
        logger.warning(
            "Pod fan-out capped",
            matched=len(eligible),
            cap=cap,
            namespace=resolved_namespace,
        )
        gaps.append(EvidenceGap(
            GapType.TRUNCATED,
            f"{len(eligible)} pods matched but only the {cap} most recently "
            f"started were read (K8S_MAX_PODS).",
            {"matched": len(eligible), "cap": cap, "namespace": resolved_namespace},
        ))
        eligible = eligible[:cap]

    targets = []
    for pod in eligible:
        phase = getattr(getattr(pod, "status", None), "phase", None) or "Unknown"
        for container in select_containers(pod):
            targets.append(PodTarget(
                namespace=resolved_namespace,
                pod_name=pod.metadata.name,
                container=container,
                restart_count=_restart_count(pod, container),
                phase=phase,
                start_time=_start_time(pod),
            ))

    logger.info(
        "Kubernetes pods discovered",
        namespace=resolved_namespace,
        selector=selector,
        pod_count=len(eligible),
        skipped_pending=skipped_pending,
        truncated=truncated,
        target_count=len(targets),
    )

    if not targets:
        logger.warning(
            "No pods matched -- check namespace and label selector",
            namespace=resolved_namespace,
            selector=selector,
        )

    return DiscoveryResult(
        targets=targets,
        gaps=gaps,
        pods_seen=pods_seen,
        pods_skipped_pending=skipped_pending,
        truncated=truncated,
    )
