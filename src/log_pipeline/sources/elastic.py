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
from src.utils.resilience import es_breaker


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

        Breaker ownership sits HERE, not on `fetch_logs_for`.
        --------------------------------------------------------------
        `@es_breaker` used to wrap the whole reduction pipeline, which runs
        the entire source chain -- so a Kubernetes outage tripped the
        Elasticsearch breaker and then blocked the Elasticsearch fallback that
        would have rescued the packet (G10). Scoped to this source, the
        breaker counts only Elasticsearch failures, and an open breaker means
        the chain moves on rather than the whole fetch failing.

        Exceptions still propagate untouched. `@retry_transient` on
        `fetch_logs_for` dispatches on exception TYPE, so swallowing an
        `ESConnectionError` here and returning ok=False would silently disable
        retries.
        """
        started = time.monotonic()
        raw_logs = es_breaker.call(fetch_logs, identifier, catalog=ctx.catalog)
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
