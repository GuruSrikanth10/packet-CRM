"""
Offline Kubernetes fixtures (KUBERNETES_LOGS_PLAN.md 10.1).

STRICTLY ADDITIVE. `K8S_FIXTURE_DIR` is consumed only by the Kubernetes
source and has no effect on the Elasticsearch path -- the existing
`ES_MOCK_FILE` CSV workflow is unchanged (design principle 5).

Layout:
    <fixture_dir>/<namespace>/<pod-name>/
        current.log     required
        previous.log    optional -- exercises in-place restart handling
        meta.json       phase, start_time, labels, containers, restart_counts

Fixtures are materialised as real `V1Pod` objects so discovery, retrieval, and
parsing run against fixtures exactly as they do against a live cluster. A
hand-rolled duck type would let the two paths drift.
"""
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from src.utils.logging_config import get_logger

logger = get_logger(__name__)


def fixture_dir() -> Optional[Path]:
    """Read at call time, never at import, so tests can set it per-case."""
    raw = os.environ.get("K8S_FIXTURE_DIR")
    return Path(raw) if raw else None


def is_active() -> bool:
    directory = fixture_dir()
    return directory is not None and directory.is_dir()


def parse_label_selector(selector: Optional[str]) -> dict:
    """Parse an equality-only selector such as `app=foo,tier=web`.

    Set-based selectors (`in`, `notin`) are not supported: `K8S_SERVICE_MAP`
    only ever produces equality selectors, and silently mis-parsing a
    set-based one would select the wrong pods.
    """
    if not selector:
        return {}
    parsed = {}
    for clause in selector.split(","):
        clause = clause.strip()
        if not clause:
            continue
        if "=" not in clause:
            raise ValueError(f"unsupported label selector clause: {clause!r}")
        key, _, value = clause.partition("=")
        parsed[key.strip()] = value.strip()
    return parsed


def _parse_timestamp(raw: Optional[str]) -> Optional[datetime]:
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        logger.warning("Unparseable fixture start_time", value=raw)
        return None


def _build_pod(namespace: str, pod_dir: Path):
    from kubernetes.client.models import (
        V1Container,
        V1ContainerStatus,
        V1ObjectMeta,
        V1Pod,
        V1PodSpec,
        V1PodStatus,
    )

    meta_path = pod_dir / "meta.json"
    meta = {}
    if meta_path.exists():
        try:
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
        except Exception as e:
            logger.warning("Unreadable fixture meta.json", path=str(meta_path), error=str(e))

    container_names = meta.get("containers") or ["app"]
    restart_counts = meta.get("restart_counts") or {}

    container_statuses = [
        V1ContainerStatus(
            name=name,
            image="fixture",
            image_id="fixture",
            ready=True,
            restart_count=int(restart_counts.get(name, 0)),
        )
        for name in container_names
    ]

    return V1Pod(
        metadata=V1ObjectMeta(
            name=pod_dir.name,
            namespace=namespace,
            labels=meta.get("labels") or {},
        ),
        spec=V1PodSpec(containers=[V1Container(name=n) for n in container_names]),
        status=V1PodStatus(
            phase=meta.get("phase", "Running"),
            start_time=_parse_timestamp(meta.get("start_time"))
            or datetime.now(timezone.utc),
            container_statuses=container_statuses,
        ),
    )


def list_pods(namespace: str, label_selector: Optional[str] = None) -> list:
    """Return fixture pods in `namespace` matching `label_selector`."""
    directory = fixture_dir()
    if directory is None:
        return []

    ns_dir = directory / namespace
    if not ns_dir.is_dir():
        return []

    wanted = parse_label_selector(label_selector)
    pods = []
    for pod_dir in sorted(p for p in ns_dir.iterdir() if p.is_dir()):
        pod = _build_pod(namespace, pod_dir)
        labels = pod.metadata.labels or {}
        if all(labels.get(k) == v for k, v in wanted.items()):
            pods.append(pod)
    return pods


def read_log(namespace: str, pod_name: str, container: Optional[str] = None,
             previous: bool = False) -> str:
    """Return fixture log text for a pod.

    Raises FileNotFoundError when `previous=True` and no previous.log exists,
    mirroring the API's 400 for a container that has never restarted. The
    caller is expected to treat that as an expected, non-fatal outcome.
    """
    directory = fixture_dir()
    if directory is None:
        raise FileNotFoundError("K8S_FIXTURE_DIR is not set")

    filename = "previous.log" if previous else "current.log"
    path = directory / namespace / pod_name / filename
    if not path.exists():
        raise FileNotFoundError(str(path))
    return path.read_text(encoding="utf-8")
