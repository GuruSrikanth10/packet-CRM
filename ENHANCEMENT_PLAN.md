# Packet-CRM: Audit Findings & Enhancement Plan

Full-repository audit performed 2026-08-14 against commit `4524091`.
Companion to `REMEDIATION_PLAN.md` (Phases 0-2, now historical). This document
records what the *current* code does wrong, what it does inefficiently, and
what the system still needs to reach its goal: **autonomously resolving
rejected biometric packets with measurable accuracy and minimal human toil.**

Findings are numbered `F1..F22` and referenced by number from the phased plan
in section 4.

**Status at time of audit: the test suite is red (2 failures / 328 tests) and
the entire Runbook pipeline is non-functional.**

---

## 1. P0 — Correctness, broken features

### F1. Reviewer runs on the expensive `complex` LLM tier

`src/core/agent_orchestrator.py:66`

```python
llm = get_llm("complex")
simple_llm = get_llm("complex")   # <-- should be "simple"
```

`ARCHITECTURE.md` §3.2 states the Reviewer is bound to the cheaper `simple`
tier and that "both tiers are now load-bearing rather than one being
constructed and discarded." The code constructs `complex` twice. The `simple`
tier is never instantiated by the graph at all.

**Impact:** every review call — one per investigation, plus one per retry, so
typically 1-4 per packet — bills at complex-tier cost and latency. This is
roughly a third of all LLM calls in the system.

**Already caught by:** `tests/test_phase2_fixes.py::test_reviewer_built_once_with_simple_llm`
(currently failing). The regression test was written correctly; the fix was
never applied.

**Fix:** one word. `simple_llm = get_llm("simple")`.

---

### F2. The Runbook pipeline is completely broken — `TypeError` at every call site

`lookup_rule_by_reason_code` is decorated with `@tool`, which under
langchain-core 1.3.1 makes it a `StructuredTool` instance. `StructuredTool` is
**not callable** — and it takes one argument, not two. Three call sites invoke
it directly with two positional arguments:

| Call site | Consequence |
|---|---|
| `src/core/agent_orchestrator.py:131` (`runbook_lookup_node`) | Uncaught → the whole `agent.invoke()` raises → **every packet with a runbook hit is published to the DLQ** |
| `src/tools/build_runbooks.py:91` | Outside the `try` at line 112 → CLI crashes before drafting anything |
| `src/tools/promote_runbooks.py:138` | `--list` staleness check crashes |

Verified empirically:

```
>>> lookup_rule_by_reason_code('X', 'UPDATE')
TypeError: 'StructuredTool' object is not callable
```

**Impact:** setting `RUNBOOK_MODE=serve` or `shadow` — the whole point of
`RUNBOOK_PLAN.md`, and the single largest cost/latency lever in the system —
does not short-circuit anything. It DLQs traffic. The feature has never run.

Two secondary defects sit behind the same lines:

- **The `enrolment_type` argument is meaningless.** The tool's signature is
  `lookup_rule_by_reason_code(reason_code)`. There is no parameter for
  enrolment type. Filtering by enrolment type happens in Python inside
  `investigator_node` (lines 249-269) and was never factored out. All three
  call sites pass a `db_etype` that no implementation reads.
- **`generate_rule_fingerprint` receives the wrong type.** It is annotated
  `rule_dict: dict` and does `json.dumps(rule_dict, sort_keys=True)`. The tool
  returns a **JSON string** (`matches.to_json(orient="records")`). Fingerprints
  computed over a string are stable but meaningless — they hash the DataFrame's
  serialization including column order, so any harmless re-export of the rules
  table invalidates every runbook.

**Fix:** extract the plain function `_lookup_rule_by_reason_code_impl` plus a
new `lookup_rule_for(reason_code, enrolment_type) -> dict | None` helper that
does the JSON parse and enrolment-type filter once, and have all four callers
(including `investigator_node`) use it. Fingerprint the parsed dict.

---

### F3. Kafka offsets commit out of order → silent message loss

`src/utils/kafkaConsumer.py:74`, `166-176`

Each worker queues its own `offset + 1` on completion; the main loop commits
whatever it drains. Workers finish out of order, so given offsets 10, 11, 12
dispatched together and 12 finishing first:

```
commit(13)     # 10 and 11 are still in flight
<crash>
```

On restart the group resumes at 13. **Messages 10 and 11 are never
redelivered.** They are lost, which is precisely what commit `5cf2dbd`
("Fix Kafka at-least-once delivery") set out to prevent.

The reverse ordering is also wrong but less harmful: `commit(13)` followed by
`commit(11)` moves the committed offset *backwards*, causing redelivery of
already-processed packets (absorbed by the `storage.exists(terminal_only=True)`
dedupe check, so a wasted poll rather than a correctness break).

**Fix:** track a per-partition set of in-flight offsets and only ever commit
the **low-water mark** — the highest offset below which every message has
completed. Standard pattern:

```python
_pending: dict[TopicPartition, set[int]] = defaultdict(set)   # dispatched
_done:    dict[TopicPartition, set[int]] = defaultdict(set)   # completed
# commit floor: smallest pending offset; commit(floor) not commit(done_max+1)
```

---

### F4. The late-result guard reads the wrong file

`src/api/routes.py:400-408` guards against overwriting a terminal status by
loading **`status.json`**. But the consumer's own timeout handler
(`src/utils/kafkaConsumer.py:62-66`) writes `FAILED_TIMEOUT` to
**`casebook.json`** — it never touches `status.json`.

Sequence:

1. Consumer's HTTP client hits `PACKET_TIMEOUT_SECONDS`, writes
   `casebook.json = FAILED_TIMEOUT`, publishes to DLQ.
2. The API's `agent.invoke()` — still running, since a Python thread cannot be
   interrupted — finally returns *under* its own
   `AGENT_INVOKE_TIMEOUT_SECONDS` budget.
3. The guard reads `status.json`, which is still `IN_PROGRESS`, so it passes.
4. `storage.save(event_id, casebook_data)` **overwrites the FAILED_TIMEOUT
   casebook with COMPLETED**, while the DLQ message stays queued.

Result: a packet that is simultaneously "completed" in the casebook and
sitting in the DLQ awaiting replay. Duplicate work downstream.

**Fix:** make the consumer's timeout path write **both** files (mirroring
`routes.py`'s DLQ path at lines 288-294), and have the guard check both.
Better still: give `CasebookStorage` a `save_terminal(event_id, status, doc)`
method that writes the pair atomically, so the two can never diverge again.

---

### F5. The rate limiter throttles the system's own consumer

`src/api/routes.py:92-127` — 10 requests per minute, keyed on
`request.client.host`. The Kafka consumer forwards every packet from a single
IP, so all traffic lands in one bucket.

On the 11th packet in any 60-second window the API returns **429**, which
`forward_signal_to_internal_endpoint` raises, `_process_and_commit` catches,
and the offset is not committed. Under `RUNBOOK_MODE=serve` — where responses
are sub-second because no LLM is involved — the consumer would exceed this in
the first few seconds. The same applies to any backlog drain after an outage
or a bulk DLQ replay.

Two further problems with the same code:

- Behind an ingress or service mesh, `request.client.host` is the **proxy's**
  IP. Every caller shares one bucket and `X-Forwarded-For` is ignored.
- `get_api_key` compares with `api_key_header in API_KEYS` — a non-constant-time
  comparison on a secret. In a UIDAI context that should be
  `hmac.compare_digest`.

**Fix:** exempt an allowlist of internal CIDRs (or require and trust
`X-Forwarded-For` only from configured proxy IPs), raise the limit to track
`MAX_CONCURRENT_INVESTIGATIONS`, and make the key comparison constant-time.

---

### F6. The test suite is red and nothing runs it

```
FAILED tests/test_phase0_fixes.py::test_commit_targets_only_this_message_offset
FAILED tests/test_phase2_fixes.py::test_reviewer_built_once_with_simple_llm
2 failed, 326 passed
```

The second is F1. The first is a **stale test**: it asserts
`_handle_one_message` commits immediately, which commit `5cf2dbd` deliberately
changed to deferred commit-on-completion. The test was never updated.

There is no CI configuration anywhere in the repository, which is why both
failures survived two commits.

**Fix:** rewrite the stale assertion against the deferred-commit contract
(assert the offset lands on `_offsets_to_commit`, not on `consumer.commit`),
fix F1, and add a GitHub Actions workflow running `pytest` on push.

---

## 2. P1 — Reliability & operability

### F7. The Kubernetes fetch deadline does not bound anything

`src/log_pipeline/sources/k8s/retrieval.py:358-406`

The wall-clock deadline breaks out of the `as_completed` loop and calls
`future.cancel()` — but `cancel()` is a no-op on already-running futures, and
exiting the `with ThreadPoolExecutor(...)` block calls `shutdown(wait=True)`,
which **blocks until every pod read finishes anyway**.

Verified empirically: a 3-second task set, broken out of after the first
completion, still takes the full 3 seconds to exit the `with` block.

So `K8S_TOTAL_FETCH_TIMEOUT_SECONDS` records a `TRUNCATED` gap that claims pods
went unread, then waits for them regardless. A slow cluster consumes the
packet's entire `AGENT_INVOKE_TIMEOUT_SECONDS` budget — exactly the failure the
deadline was written to prevent.

**Fix:** drop the context manager and call
`pool.shutdown(wait=False, cancel_futures=True)` explicitly on the deadline
path. Also pass `timeout=remaining` to `as_completed` rather than checking the
clock only when a future happens to complete — as written, if no future
completes, the loop never evaluates the deadline at all.

---

### F8. `call_with_retry` and `k8s_breaker` are dead code

`src/log_pipeline/sources/k8s/retry.py` implements a careful status-aware retry
policy (429/5xx retry with full jitter; never retry 403). `resilience.py`
defines `k8s_breaker`. `ARCHITECTURE.md` §3.10 documents both as active.

Neither is referenced from any production call path. The only importers are
`tests/test_k8s_retry.py`. The three actual Kubernetes API calls —
`read_namespace` (discovery.py:221), `list_namespaced_pod` (discovery.py:259),
`read_namespaced_pod_log` (retrieval.py:170) — are unwrapped.

**Impact:** a single 429 or 503 from the API server fails that pod's read
outright. With `K8S_FETCH_CONCURRENCY=5` hitting one API server, 429s are the
expected failure mode, and the jitter written specifically to handle the
thundering herd never runs.

**Fix:** wrap all three call sites in `call_with_retry`, and put `k8s_breaker`
around `KubernetesLogSource.fetch`.

---

### F9. The consumer heartbeat starves under load → healthy pods get restarted

`src/utils/kafkaConsumer.py:180-190`

The heartbeat file is written once per `poll()` cycle. But
`_queue_semaphore.acquire()` (line 189) blocks the **main polling thread** when
all `MAX_CONCURRENT_INVESTIGATIONS` slots are busy — for as long as an
investigation takes, which is minutes.

`/health` treats a heartbeat older than 30 seconds as a dead consumer
(`routes.py:146`). So under exactly the sustained load the semaphore exists to
manage, the consumer reports itself dead, and a Kubernetes liveness probe
restarts a perfectly healthy pod — killing every in-flight investigation.

**Fix:** move the heartbeat to a dedicated daemon thread with its own ticker,
independent of the poll loop. Write it atomically (`.tmp` + `os.replace`); the
current non-atomic write can also be read mid-flight by `/health`.

---

### F10. No PII redaction on the Elasticsearch path

`redaction.redact_records` is called in exactly one place:
`retrieval.read_pod_logs` (k8s only). `ElasticLogSource.fetch` →
`fetcher.fetch_logs` never redacts.

`redaction.py`'s own module docstring explains why this matters — logs "may
carry resident identifiers or entire request payloads" and that text reaches
`raw_logs.txt`, the log snapshot, and S3. All of that is equally true of the
Elasticsearch `message` field.

Since `LOG_SOURCE` defaults to `kubernetes,elastic` and Kubernetes fails fast
when unconfigured, **Elasticsearch is the path most deployments actually use** —
so in practice redaction is off for nearly all traffic. Unredacted Aadhaar
numbers, VIDs, and mobile numbers land in `local_casesheets/*/raw_logs.txt`, in
the casebook's `rejection_logs` field, and in S3.

**Fix:** move the `redact_records` call up into `pipeline.reduce_logs`, applied
to `fetch_result.records` before `_save_raw_logs`. That covers every source at
one seam and removes the k8s-specific call.

---

### F11. `refId` never reaches the Kubernetes identifier filter

`pipeline.reduce_logs` constructs `FetchContext(event_id=event_id,
catalog=catalog)` — the only `FetchContext(...)` in the codebase.
`extra_identifiers` therefore defaults to `()` on every fetch, so
`_extra_identifiers(ctx)` always returns `[]` and only `eventId` is matched.

`K8S_SEARCH_FIELDS=eventId,refId` is documented in `.env.example` and read
**nowhere in the codebase**.

`filtering.py`'s docstring names this as Open Question 1: "we do not yet know
which id the services actually log." If the services log `refId`, the
Kubernetes source silently returns zero matching lines, the chain falls through
to Elasticsearch, and the Kubernetes work delivers nothing — with no signal
that identifier matching is the reason.

**Fix:** thread the payload through. `fetch_elastic_logs(event_id)` →
`reduce_logs(event_id, extra_identifiers=...)`, sourced from
`payload["packetMetaData"]["refId"]` and `srn` in `fetch_logs_node`. Honour
`K8S_SEARCH_FIELDS` when selecting which payload fields to use.

---

### F12. No graceful shutdown anywhere

No `SIGTERM` handler in `main_consumer.py`, `main_api.py`, or `start.py`.
`consume_forever` is a bare `while True`.

On a Kubernetes rolling deploy: in-flight investigations are killed mid-LLM-call,
their offsets are never committed (correct — they redeliver), but the
`IN_PROGRESS` `status.json` stubs are left behind and block reprocessing until
`MAX_IN_PROGRESS_AGE_SECONDS` (default **30 minutes**) elapses.

`start.py` compounds this: `api_process.wait()` then `consumer_process.wait()`
means if the API dies, the supervisor simply moves on to waiting on the
consumer. Neither child is restarted or health-checked, and the survivor is
never terminated.

**Fix:** a `threading.Event` shutdown flag checked by the poll loop, `SIGTERM`
/ `SIGINT` handlers that set it, a bounded drain of in-flight work, and a
final offset commit. In `start.py`, wait on both processes concurrently and
terminate the sibling when either exits.

---

### F13. Runbook cache invalidation misses on enrolment-type casing

`src/utils/runbook_store.py:144`

```python
cache_key = f"{reason_code}__{enrolment_type}"        # promote: raw
```

but `get_runbook` builds its key from the **normalized** value:

```python
etype = str(enrolment_type).strip().upper() if enrolment_type else "ANY"
cache_key = f"{r_code}__{e_type}"                      # lookup: normalized
```

A draft carrying `"e"` or `" E "` invalidates a key that lookups never use, so
a freshly promoted runbook is not served for up to
`RUNBOOK_CACHE_TTL_SECONDS` (default 600s) while the stale prior version is.

**Fix:** normalize in one place — a `_cache_key(reason_code, enrolment_type)`
helper used by both.

---

### F14. Live-DB mode is very likely broken

`src/tools/tool_registry.py:195-196`

```python
query = "SELECT * FROM rules WHERE reject_reason_code = %s"
matches = pd.read_sql(query, engine, params=(reason_code,))
```

With pandas 3.0.2 against a SQLAlchemy 2.0.51 `Engine`, a raw string is wrapped
in `sqlalchemy.text()`, which uses `:name` bind parameters — not the DBAPI
`%s` paramstyle — and expects a **dict**, not a tuple. This path is exercised
only when `USE_MOCK_DB=false`, and no test covers it.

`get_live_db_engine()` also has no `pool_pre_ping=True`, so pooled connections
go stale across MySQL's `wait_timeout` and the first query after an idle period
fails.

**Fix:** `text("SELECT * FROM rules WHERE reject_reason_code = :rc")` with
`params={"rc": reason_code}`; add `pool_pre_ping=True` and `pool_recycle=3600`.
Add a test with a SQLite in-memory engine so the query at least round-trips.

---

### F15. `_rule_cache` is mutated from multiple threads without a lock

`cachetools.TTLCache` is explicitly **not** thread-safe; its own docs require
external locking. `lookup_rule_by_reason_code` reads and writes it from up to
`MAX_CONCURRENT_INVESTIGATIONS` agent threads. `_runbook_cache` in
`runbook_store.py` has the same exposure.

TTL expiry mutates the internal linked list during `__getitem__`, so concurrent
access can corrupt it or raise `KeyError` from inside the cache.

**Fix:** wrap both in a `threading.Lock`, or use `cachetools.func.ttl_cache`
which locks internally.

---

## 3. P2 — Optimization & hygiene

**F16. Elasticsearch query has no time bound and rebuilds its client per fetch.**
`fetcher.fetch_logs` constructs a fresh `Elasticsearch(...)` on every call
(new TLS handshake per packet) and issues an unbounded `query_string` across
the entire `logs-*` pattern with no `@timestamp` range filter. On a
production-sized index that is a full-history scan per packet. Cache the client
module-level; add a range filter derived from the `TimeWindow` the source
already receives and currently discards.

**F17. `seq_no_primary_term=True` is requested but unused.** The sort tiebreaker
is `_id` (line 135), not `_seq_no`. The flag adds per-hit payload for nothing.
Drop it, or switch the tiebreaker.

**F18. Redaction recompiles and re-reads env per log record.** `redact_text`
evaluates `list(DEFAULT_PATTERNS) + _extra_patterns()` on every call, and
`_extra_patterns()` reads `os.environ`, splits, and calls `re.compile` each
time. At `LOG_MAX_DOCUMENTS=50000` that is 50k env reads and list allocations
per packet. Cache the compiled tuple module-level, invalidated on env change.

**F19. The Elasticsearch app filter is hardcoded.**
`{"term": {"application_name.keyword": "enu-biometric"}}` (fetcher.py:111) means
only one service's logs are ever retrievable, regardless of which stage the
packet failed in. Make it an env-driven list.

**F20. Redundant work in the request path.** `routes.py` calls
`get_casebook_storage()` twice (lines 192 and 394) — harmless but noise.
`s3_uploader` constructs `boto3.client("s3")` per upload rather than caching it.

**F21. Logging is inconsistent — 196 bare `print()` calls in `src/`.**
`kafkaConsumer.py` and `dlq_publisher.py` use stdlib `logging`, not the
`structlog` JSON logger; `s3_uploader.py` and the operator CLIs use `print()`.
`ARCHITECTURE.md` §3.4 claims the pipeline logs "through the same `structlog`
logger... rather than bare `print()`". Any log aggregator ingesting these gets
unparseable lines interleaved with JSON. `logging_config.py` also hardcodes
`level=logging.INFO` with no `LOG_LEVEL` env override.

**F22. Path constants are duplicated across three modules.** `local_casesheets`
is independently derived in `storage/local.py:13`, `log_pipeline/pipeline.py:224`
and `:240`, and `log_pipeline/snapshot.py:45`. `utils/paths.py` exists for
exactly this purpose but only holds the checkpoint DB. Consequence: the log
pipeline writes to the local filesystem even when
`CASEBOOK_STORAGE_BACKEND=s3`, because it bypasses the storage abstraction
entirely. Add `LOCAL_CASESHEETS_DIR` to `paths.py` and route log persistence
through `CasebookStorage`.

Minor: `datetime.utcnow()` (deprecated in 3.12+, and this repo runs 3.14) in
`build_runbooks.py:145` and `promote_runbooks.py:110`; `.env.example` ships a
personal Windows path as `MOCK_DB_PATH`; `runbook_store.py` runs `os.makedirs`
at import time.

---

## 4. What the system still needs — enhancement roadmap

The findings above make the system *correct*. This section is about making it
*achieve its goal*. Ordered by leverage.

### 4.1 Close the outcome loop — the single biggest gap

**Nothing in the system measures whether a resolution was right.**

`eval_harness.py` measures evidence-citation accuracy of the log pipeline. It
does not measure whether the `action` the Synthesis agent chose actually
resolved the packet. There is no field for ground truth, no operator verdict,
no per-reason-code accuracy figure.

This blocks everything downstream: you cannot safely auto-promote runbooks, you
cannot detect agent regression after a prompt change, you cannot justify
raising automation levels, and you cannot tell a stakeholder how well the
system works.

**Build:**
- `resolution_outcome` block in the casebook schema (bump to `1.2`):
  `{verdict: CORRECT|INCORRECT|PARTIAL, verified_by, verified_at, notes}`.
- `src/tools/record_outcome.py` — operator CLI to attach a verdict, plus a
  `POST /outcome/{event_id}` endpoint so an existing ops tool can write it.
- `src/tools/accuracy_report.py` — accuracy by `reason_code` × `enrolment_type`
  × `resolution.source`, so agent-generated and runbook-served results are
  compared directly.

### 4.2 Turn on the Runbook pipeline (unblocked by F2)

Runbook-served resolutions are sub-second and cost zero LLM tokens. With F2
fixed, the natural sequence is: run `RUNBOOK_MODE=shadow` in production for a
sample period, use §4.1's accuracy data to confirm the runbook and the agents
agree, then flip to `serve` per reason code.

Add a per-reason-code override (`RUNBOOK_SERVE_ALLOWLIST`) rather than a global
mode switch, so high-volume, well-understood codes go first.

### 4.3 Make the Synthesis contract enforceable

`routes.py:300-312` parses the Synthesis output with a regex, then
`json.loads`, then falls back to `{"rejection_description": str(...)}`. A
malformed LLM response silently produces a casebook with `action: null` and
`synthesis: null` — indistinguishable, downstream, from a packet the agents
genuinely could not classify.

`SynthesisAgent.md` specifies exact enums for `action` and `resident_action`;
nothing validates them. An LLM returning `"REPLAY_PACKET"` is accepted as-is.

**Build:** a Pydantic `SynthesisResult` model with `Literal` enums; validate on
parse; on `ValidationError`, retry once with the validation error fed back as
feedback; on second failure route to `NEEDS_MANUAL_REVIEW` with the raw text
preserved. Prefer structured output (`llm.with_structured_output`) over prompt
instructions where the local endpoint supports it.

### 4.4 Add calibrated confidence and abstention

Every packet currently gets a confident-sounding answer. There is no path for
"the evidence does not support a conclusion" short of the Reviewer rejecting
findings `MAX_INVESTIGATION_RETRIES` times.

This matters most when the evidence-gap banner is present: the trace is known
incomplete, the prompts tell the agents to qualify their findings, but nothing
stops a confident `REPLAY` being emitted from a partial trace.

**Build:** a `confidence` float in the Synthesis contract; auto-route below
`SYNTHESIS_CONFIDENCE_THRESHOLD` to `MANUAL_REVIEW`; force the ceiling down
when `EvidenceGap`s are present. Validate the calibration against §4.1's
outcome data — a confidence score nobody has checked is worse than none.

### 4.5 Observability

`types.py` states plainly: *"there is no metrics backend in this project."*
`FetchDiagnostics` is built on every fetch and then discarded — never logged,
never aggregated.

Unknowable today: p50/p95 packet latency; LLM token spend per packet;
investigator retry-rate distribution; runbook hit rate; log-source win rate
(Kubernetes vs Elasticsearch); circuit-breaker trip frequency; evidence-gap
rate by type.

**Build:** `prometheus-client`, a `/metrics` endpoint, and counters/histograms
at the seams that already compute the numbers. Emit `FetchDiagnostics` into
the structlog stream at minimum — that is nearly free and immediately useful.
Add token accounting via a LangChain callback handler.

### 4.6 Strengthen the payload contract

`packetMetaData` and `flowMetaData` are `Optional[Dict[str, Any]]`. Every
field the system actually keys on — `refId`, `srn`, `enrolmentType`,
`pktSource`, `stage` — is unvalidated. An upstream rename silently yields
`None` in the casebook, and F11's identifier plumbing depends on these fields
existing.

Also unresolved from `ARCHITECTURE.md` §3.8: `is_mbu`, `is_child`, and
`update_type` are emitted as `null` because "the mapping is not derivable from
the payload alone." MBU handling is called out as a distinct processing mode in
the Synthesis prompt, so the system reasons about a distinction it cannot
record. Resolve the mapping with the upstream team or drop the fields.

**Build:** typed `PacketMetaData` / `FlowMetaData` Pydantic models with the
known fields optional-but-declared, keeping `model_config =
ConfigDict(extra="allow")` so unknown upstream additions don't reject packets.

### 4.7 Remove the single-node ceiling

Three things pin the system to one process:

- `SqliteSaver` checkpointer on a local file
- `LocalFilesystemCasebookStorage` with `filelock`
- `S3CasebookStorage` raising `NotImplementedError` on all three methods

Two API replicas cannot share checkpoints or casebooks today. `filelock` does
not work across pods on different nodes.

**Build:** implement `S3CasebookStorage` (the Protocol is already correct and
the local implementation is a good reference), and swap `SqliteSaver` for
`langgraph-checkpoint-postgres` behind a `CHECKPOINT_BACKEND` env var. Both are
drop-in at the seams that already exist — this is the payoff for the storage
abstraction work already done.

### 4.8 CI and data quality

- **CI:** GitHub Actions running `pytest` on push. F1 and F6 both survived two
  commits because nothing runs the tests.
- **`rules.csv`:** still the single garbled column recorded as Known Gap #1.
  `check_drift.py` detects it; only a clean re-export from the source DB fixes
  it. Until then, rule injection quality is capped regardless of agent quality.
- **Template catalog:** still never built under the fixed pipeline (Known Gap
  #2). Stage 1's `must_not` boilerplate filter and Stage 4's classification are
  both no-ops until `build_catalog.py` runs against real event IDs.

---

## 5. Phased implementation plan

### Phase A — Stop the bleeding (1-2 days)

Restores a green suite and unbreaks the Runbook feature. Every item is small
and independently verifiable.

| # | Change | Files |
|---|---|---|
| F1 | `get_llm("simple")` for the Reviewer | `core/agent_orchestrator.py` |
| F2 | Extract `lookup_rule_for()`; fix 3 call sites; fingerprint a dict | `tools/tool_registry.py`, `core/agent_orchestrator.py`, `tools/build_runbooks.py`, `tools/promote_runbooks.py`, `utils/runbook_store.py` |
| F6 | Rewrite the stale offset test; add CI workflow | `tests/test_phase0_fixes.py`, `.github/workflows/test.yml` |
| F13 | Shared `_cache_key()` normalizer | `utils/runbook_store.py` |

**Exit criteria:** `pytest` green; `RUNBOOK_MODE=shadow` runs a packet end to
end without a DLQ; CI green on push.

### Phase B — Delivery guarantees & safety (3-5 days)

The correctness issues that only appear under production load or failure.

| # | Change |
|---|---|
| F3 | Low-water-mark offset commits |
| F4 | Atomic terminal-status writes across both files |
| F5 | Internal-IP exemption + `X-Forwarded-For` + constant-time key compare |
| F9 | Heartbeat on a dedicated thread, written atomically |
| F12 | `SIGTERM` handling, bounded drain, `start.py` supervision |
| F10 | Redaction moved into `pipeline.reduce_logs` (covers all sources) |

**Exit criteria:** a kill -TERM mid-investigation leaves no `IN_PROGRESS` stub
and loses no offsets; a 200-packet backlog drains without a single 429; an ES
fetch containing a synthetic Aadhaar number lands redacted in `raw_logs.txt`.

### Phase C — Make the Kubernetes source actually work (2-4 days)

Every item here is code already written but not wired up.

| # | Change |
|---|---|
| F7 | `shutdown(wait=False, cancel_futures=True)` + `as_completed(timeout=)` |
| F8 | Wrap the 3 API call sites in `call_with_retry`; add `k8s_breaker` |
| F11 | Thread `refId`/`srn` into `FetchContext.extra_identifiers`; honour `K8S_SEARCH_FIELDS` |
| F14 | `text()` + named params; `pool_pre_ping` |
| F15 | Lock both TTL caches |

**Exit criteria:** a fixture-backed fetch against 20 slow pods returns within
`K8S_TOTAL_FETCH_TIMEOUT_SECONDS` ± 2s; an injected 429 is retried and
succeeds; a packet whose logs carry only `refId` returns matching lines.

### Phase D — Measurement (1 week)

Nothing after this phase can be evaluated without it, so it precedes the
enhancements it enables.

- §4.1 outcome loop: schema `1.2`, `record_outcome.py`, `POST /outcome`,
  `accuracy_report.py`
- §4.5 observability: `/metrics`, `FetchDiagnostics` into structlog, token
  accounting callback
- F21 logging consistency + `LOG_LEVEL`; F16-F20, F22 optimizations

**Exit criteria:** an accuracy figure per reason code exists; p95 packet latency
and per-packet token cost are both graphable.

### Phase E — Raise the automation ceiling (2-3 weeks)

Gated on Phase D's data.

- §4.3 enforceable Synthesis contract with validation-repair retry
- §4.4 confidence + abstention, calibrated against Phase D outcomes
- §4.2 per-reason-code runbook rollout: shadow → measure → serve
- §4.6 typed payload models; resolve or drop `is_mbu`/`is_child`/`update_type`

**Exit criteria:** zero casebooks with `action: null` from parse failures;
runbook-served resolutions match agent accuracy on the codes they serve.

### Phase F — Scale out (2-3 weeks)

- §4.7 `S3CasebookStorage`, Postgres checkpointer
- §4.8 clean `rules.csv` re-export; first real template catalog build
- Horizontal-scale validation: two API replicas + two consumers, one topic

**Exit criteria:** two replicas process a shared topic with no duplicate
casebooks and no checkpoint contention.

---

## 6. Documentation corrections

`ARCHITECTURE.md` currently describes several behaviours the code does not
have. It is explicitly maintained as "a truthful source of truth", so these
should be corrected alongside the fixes:

| §3.2 | Claims the Reviewer uses the `simple` tier — it uses `complex` (F1) |
| §3.9 | Describes runbook serving as working — it raises `TypeError` (F2) |
| §3.10 | Describes `retry.py` status-aware backoff as active — it is dead code (F8) |
| §3.10 | Describes `K8S_DEADLINE_SECONDS` as bounding the fan-out — it does not (F7), and the actual variable is `K8S_TOTAL_FETCH_TIMEOUT_SECONDS` |
| §3.4 | Claims the pipeline logs via structlog "rather than bare `print()`" — 196 `print()` calls remain (F21) |
| §3.10 | PII redaction is described as a property of the log pipeline; it only applies to the Kubernetes source (F10) |
