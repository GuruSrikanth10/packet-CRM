"""
Elasticsearch log source (KUBERNETES_LOGS_PLAN.md Phase 1).

A thin adapter over `src.log_pipeline.fetcher.fetch_logs`. The fetcher itself
is deliberately NOT modified -- including its `ES_MOCK_FILE` CSV branch, which
keeps working byte for byte (design principle 5).

Elasticsearch remains the primary source (design principle 1); this wrapper
exists so a second source can be added beside it without Stages 2-4 caring.
"""
import time

from src.log_pipeline.fetcher import fetch_logs
from src.log_pipeline.types import (
    FetchContext,
    FetchDiagnostics,
    FetchResult,
    LogRecord,
    TimeWindow,
)


class ElasticLogSource:
    """Wraps the existing Elasticsearch fetcher in the `LogSource` protocol."""

    name = "elastic"

    def fetch(self, identifier: str, window: TimeWindow,
              ctx: FetchContext) -> FetchResult:
        """Fetch records for `identifier`.

        `window` is accepted for protocol conformance but **ignored**: the
        current Elasticsearch query has no time bound, and adding one would be
        a behaviour change explicitly out of scope for Phase 1. The Kubernetes
        source uses it from Phase 3.

        Exceptions are deliberately NOT caught here.
        --------------------------------------------------------------
        `fetch_elastic_logs` in `src/tools/tool_registry.py` carries
        `@es_breaker` and `@retry_transient`, both of which dispatch on the
        *type* of the raised exception. Catching an `ESConnectionError` here
        and returning `ok=False` -- or wrapping it in a custom error -- would
        silently disable retries and the circuit breaker, since neither would
        ever see a type it recognises. Letting exceptions propagate untouched
        preserves today's resilience semantics exactly.

        `FetchResult.ok` is therefore always True for this source. It becomes
        meaningful for the Kubernetes source (Phase 3), which handles its own
        per-pod failures, and for the fallback chain (Phase 9).
        """
        started = time.monotonic()
        raw_logs = fetch_logs(identifier, catalog=ctx.catalog)
        latency_ms = (time.monotonic() - started) * 1000.0

        records = self._stamp_source(raw_logs or [])

        return FetchResult(
            records=records,
            diagnostics=FetchDiagnostics(
                source=self.name,
                records_returned=len(records),
                latency_ms=latency_ms,
            ),
        )

    @staticmethod
    def _stamp_source(raw_logs: list[dict]) -> list[LogRecord]:
        """Tag provenance onto each record.

        Mutates in place rather than copying: the fetcher builds these dicts
        fresh on every call and no caller retains a reference, so copying
        would allocate a second dict per record (up to `LOG_MAX_DOCUMENTS`,
        default 50k) for no benefit.
        """
        for record in raw_logs:
            record["source"] = ElasticLogSource.name
        return raw_logs
