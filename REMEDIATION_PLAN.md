# Packet-CRM: Bug Remediation & Optimization Plan

Audit date: 2026-08-10. Scope: full `src/`, `tests/`, root scripts, config files.
Findings verified by reading the code and, where marked **[verified]**, by executing it.

Severity: **P0** breaks correctness or silently corrupts output. **P1** degrades
reliability or operability. **P2** is cost, performance, or hygiene.

---

## Phase 0 — Stop the bleeding (P0)

These change what the system *outputs*, not just how fast it runs. Do these first.

### 0.1 Drain3 emits other packets' log templates into this packet's evidence **[verified]**
`src/log_pipeline/reducer.py:111-131`

`cluster_logs` builds a `TemplateMiner` over a **shared, file-persisted** parse tree
(`local_checkpoints/drain3_state/drain3_state.bin`), then iterates
`template_miner.drain.clusters` — which contains every template ever seen, not just
those matched by the current flow. Stale clusters get `count` from `cluster.size`
(a cumulative global counter) and `first_seen=""`, which sorts them to the *front*
of the LLM's evidence block.

Reproduced: clustering flow A (`AAA alpha...`), then flow B (`BBB beta...`), returns
flow A's template inside flow B's output.

In a biometric-rejection context this is both a correctness bug and a cross-packet
data-leak: one resident's log lines are presented to the LLM as evidence for another.

**Fix:** collect the set of `cluster_id`s actually returned by `add_log_message`
during this call, and emit only those. Take `count` exclusively from `cluster_meta`.

```python
seen_ids = set()
for log_entry in logs:
    result = template_miner.add_log_message(log_entry.get("message", ""))
    seen_ids.add(result["cluster_id"])
    ...
for cluster in template_miner.drain.clusters:
    if cluster.cluster_id not in seen_ids:
        continue
```

### 0.2 Concurrent packets corrupt the shared Drain3 state file
`src/log_pipeline/reducer.py:75-81`

A fresh `FilePersistence` + `TemplateMiner` is constructed per call with no lock.
With `MAX_CONCURRENT_INVESTIGATIONS=5`, two packets read the same `.bin`, diverge,
and last-writer-wins — losing template IDs and destabilising the very IDs the
persistence exists to stabilise.

**Fix:** hold a module-level `threading.Lock` (plus a `filelock` for the
multi-process API+consumer split) around miner construction and the feed loop.
Better: build the miner once at process start and reuse it under that lock.

### 0.3 `/ready` can never return ready **[verified]**
`src/api/routes.py:86` vs `src/core/agent_orchestrator.py:268`

The graph writes its checkpoint DB to `src/checkpoints.db`. `/ready` probes
`local_checkpoints/checkpoints.db`. That directory does not even exist at rest,
so `sqlite3.connect` raises, `db_ready` stays `False`, and the endpoint returns
`503` permanently. Any orchestrator wired to this probe will refuse to route traffic.

`src/tools/prune_checkpoints.py:14` has the same wrong path and always prints
"Database not found" — so checkpoints have never actually been pruned.

**Fix:** define the path once (e.g. `src/utils/paths.py: CHECKPOINT_DB_PATH`) and
import it in the orchestrator, `/ready`, and `prune_checkpoints.py`. Move the file
under `local_checkpoints/` (already gitignored) and use
`sqlite3.connect(path)` only after asserting the file exists, so a missing DB fails
the probe instead of being silently created.

### 0.4 `"APPROVED" in feedback` approves rejections
`src/core/agent_orchestrator.py:198-206`

`feedback.upper()` then substring match. "NOT APPROVED", "DISAPPROVED", "this is not
approved because..." all route straight to synthesis, skipping the entire QC loop.
The Reviewer's own prompt only asks it to "reply with exactly 'APPROVED'", so the
happy path masks how fragile this is.

**Fix:** require an exact verdict token. Strip whitespace/markdown and test
`feedback.strip().upper().startswith("APPROVED")` after first rejecting any
negation prefix — or, more robustly, have the Reviewer emit
`{"verdict": "APPROVED"|"REJECTED", "reason": "..."}` and parse it.

### 0.5 Redelivered packets escalate immediately (checkpoint retry carryover) **[verified]**
`src/api/routes.py:125-127`, `src/core/agent_orchestrator.py:195`

`thread_id` is the `eventId` and `retry_count` lives in the persisted checkpoint.
A packet that ran before (and whose casebook was deleted, expired, or never reached
terminal state) resumes with `retry_count` already at or over the limit and escalates
without doing any work. The test run surfaced exactly this: `retry_count: 5` against
`max_retries: 2` on a "fresh" invocation.

**Fix:** make the thread id attempt-scoped (`f"{event_id}:{attempt}"`), or explicitly
reset `retry_count` to `0` when starting a fresh (non-resumed) invocation:
`agent.invoke({"payload": signal_dict, "retry_count": 0}, config=config)`.

### 0.6 The idempotency guard has a fall-through hole
`src/api/routes.py:136-149`

For `status == "IN_PROGRESS"`, only two cases are handled: stale-without-checkpoint,
and has-checkpoint. The fourth combination — **not stale, no active checkpoint** —
matches neither branch and falls through to a full reprocess. That is precisely the
window while a run is in flight but between checkpoint writes, so a redelivery
duplicates the entire agent pipeline (and its LLM spend) for the same packet.

**Fix:** make the branch total — treat "IN_PROGRESS and not stale" as
already-processing regardless of checkpoint state, and only reprocess when stale.

### 0.7 LLM calls are never retried
`src/utils/resilience.py:14-20`

`TRANSIENT_EXCEPTIONS` covers `requests`, `urllib3`, Elasticsearch, and SQLAlchemy
errors. `langchain-openai` raises `openai.APIConnectionError` / `APITimeoutError` /
`RateLimitError` (backed by `httpx`), none of which match. `@retry_transient` on
`invoke_investigator` / `invoke_reviewer` / `invoke_synthesis` is therefore inert:
the first transient blip propagates, trips `llm_breaker` toward its 3-failure limit,
and the packet goes to the DLQ.

**Fix:** add `openai.APIConnectionError`, `openai.APITimeoutError`,
`openai.RateLimitError`, `openai.InternalServerError`, and `httpx.TransportError`
to the tuple. Import them lazily so the module still loads without the openai extra.

### 0.8 Nothing bounds an LLM call server-side
`src/utils/llm_utils.py:50-71`, `src/api/routes.py:161-165`

Neither `ChatOpenAI` nor `ChatMistralAI` is given a `timeout`/`max_retries`, and
`agent.invoke` is not wrapped. `PACKET_TIMEOUT_SECONDS` only sets the *consumer's*
HTTP client deadline (`kafkaConsumer.py:20`), so on a hung model the consumer gives
up, writes `FAILED_TIMEOUT`, publishes to the DLQ — while the API thread keeps
running and later overwrites that casebook with a "successful" result. The terminal
status a packet ends in is a race.

**Fix:** pass `timeout=<seconds>, max_retries=0` to both chat clients (let `tenacity`
own retries), and enforce a server-side budget below the client deadline so the API
is authoritative about its own failure. Guard the final `storage.save` so it does not
overwrite a terminal `FAILED_TIMEOUT`/`DLQ` status written by another actor.

### 0.9 Consumer semaphore can be released twice
`src/utils/kafkaConsumer.py:138-147`

`_worker_pool.submit(...)` is inside the `try`, and so is the `consumer.commit()`
that follows it. If `commit()` raises after a successful submit, the `except` block
releases the semaphore and the worker's `finally` releases it again. Each such event
permanently raises the concurrency ceiling; repeated occurrences remove backpressure
entirely and let unbounded investigations run in parallel.

**Fix:** track ownership explicitly.

```python
submitted = False
try:
    ...
    _worker_pool.submit(_process_and_commit, signal)
    submitted = True
    consumer.commit()
except Exception:
    logger.exception(...)
finally:
    if not submitted:
        _queue_semaphore.release()
```

### 0.10 Offsets are committed for messages that have not been enqueued
`src/utils/kafkaConsumer.py:114, 122, 131, 142`

`consumer.poll()` advances the consumer position past the *entire* returned batch.
A bare `consumer.commit()` after handling the first of N messages therefore commits
all N. Crash mid-batch and the remaining messages are lost with no DLQ entry —
silent data loss, not the at-least-once the design assumes.

**Fix:** commit explicit offsets per message:
`consumer.commit({tp: OffsetAndMetadata(msg.offset + 1, None)})`, or accumulate and
commit once at the end of each fully-enqueued batch.

### 0.11 `eventId` flows unvalidated into filesystem paths
`src/models/schemas.py:17`, `src/storage/local.py:18`, `src/utils/s3_uploader.py:19`

`eventId: str` has no constraint and is interpolated into
`local_casesheets/casebook_{event_id}` and into S3 keys. An `eventId` of
`../../something` escapes the storage root; `os.makedirs` will happily create it.
The endpoint is API-key protected, but the same value also arrives straight off a
Kafka topic.

**Fix:** constrain it in Pydantic —
`Field(pattern=r"^[A-Za-z0-9_.:-]{1,128}$")` — and additionally assert in
`_get_dir` that the resolved path stays under `self.base_dir`.

### 0.12 The test suite is red **[verified]**
`tests/test_resilience.py:64`

`test_loop_guard_max_retries` asserts `casebook["Resolution"]["Synthesis"]`; commit
`84192f9` renamed those keys to `resolution`/`synthesis`. Result: `1 failed, 7 passed`.

Three further defects make the suite weaker than it looks:
- `DUMMY_PAYLOAD` uses `"packetstatus"` (lowercase `s`), which is not the schema
  field, so `packet_status.status` is `None` in every test that touches it.
- `test_idempotency_short_circuit` patches `src.core.agent_orchestrator.get_agent`,
  but `routes.py` imported the symbol into its own namespace — the patch is a no-op
  and `mock_get_agent.assert_not_called()` passes vacuously.
- The fixture wipes `local_casesheets/` but never the checkpoint DB, so LangGraph
  state leaks between tests (this is what produced the `retry_count: 5` above).

**Fix:** update the assertions, correct the payload key, patch
`src.api.routes.get_agent`, and delete the checkpoint DB in the fixture.

---

## Phase 1 — Reliability and operability (P1)

| # | Location | Issue | Fix |
|---|----------|-------|-----|
| 1.1 | `tool_registry.py:83` | `@functools.lru_cache` on `lookup_rule_by_reason_code` caches for the process lifetime — including the *string* `"Failed to query live DB: ..."`. One transient MySQL blip poisons that reason code until restart, and live rule edits never take effect. | Replace with a TTL cache (e.g. `cachetools.TTLCache`, 5-15 min) and never cache a failure result. |
| 1.2 | `build_catalog.py:63` | Consumes the same leaky `cluster_logs` as 0.1, so every template appears in every flow, `pct >= 0.90` for all of them, and the catalog classifies **everything** as boilerplate. Those phrases then become ES `must_not` filters that delete real evidence before it is ever fetched. | Fixed transitively by 0.1; rebuild the catalog afterwards and assert the boilerplate share is plausible (< ~40%). |
| 1.3 | `config_validator.py:13` | Hard-fails startup unless `OPENAI_API_KEY` is set — a variable no code path reads. The real keys are `LLM_API_KEY_COMPLEX` / `HF_TOKEN` / Mistral. | Validate the key that the *selected* provider actually needs. |
| 1.4 | `.env` vs `llm_utils.py` | `.env` defines `LLM_*_MEDIUM` and `LLM_*_BASIC`; the code reads `LLM_*_SIMPLE`. `get_llm("simple")` therefore silently falls back to hardcoded defaults. `.env.example` is missing `MOCK_LLM_WITH_MISTRAL`, `ES_MOCK_FILE`, `MAX_IN_PROGRESS_AGE_SECONDS`, `PACKET_TIMEOUT_SECONDS`, `S3_LOGS_BUCKET`, `ENV`. | Pick one tier vocabulary, align all three files, and fail fast on unknown tiers. |
| 1.5 | `routes.py:166-178` | The DLQ path writes `casebook.json` but leaves `status.json` at `IN_PROGRESS` forever. | Write the terminal status to both files on every exit path. |
| 1.6 | `routes.py:221` | Guards on `raw_logs.startswith("Failed to query")`, but `fetch_elastic_logs` also returns `"Failed to process logs: ..."`. That error string is then treated as log content and can be uploaded to S3 as evidence. | Return a sentinel/`None` from the tool instead of pattern-matching prose. |
| 1.7 | `promote_rules.py:73-75` | Truncates `pending_rules.jsonl` after the interactive session, discarding rules the operator *skipped*, rules that errored, and any rule appended by a running agent during the prompt. Silent loss of learning signal. | Rewrite only the entries that were not promoted; hold the lock across read-modify-write, or move promoted entries to an archive file. |
| 1.8 | `approve_replays.py:37-93` | Same read-then-rewrite race: replays queued while the operator is deciding are erased. Also POSTs the payload as **query parameters** (`params=payload`), putting `notificationEmail` and `notificationMobile` into server access logs, with no auth header. | Hold the lock for the whole cycle (or use an atomic claim), and send the payload as a JSON body over an authenticated call. |
| 1.9 | `fetcher.py:86` | `Elasticsearch(es_host, verify_certs=False)` disables TLS verification unconditionally, and no request timeout is set. | Make verification an explicit env flag defaulting to *on*; add `request_timeout`. |
| 1.10 | `fetcher.py:125-152` | Unbounded pagination — no cap on total documents and no early exit when `len(hits) < page_size`. A noisy `eventId` can pull millions of rows into memory. | Add `LOG_MAX_DOCUMENTS` (e.g. 50k), break when the page is short, and log when the cap truncates. |
| 1.11 | `reducer.py:41-45` | `branch_on_error` keeps 200 preceding lines *and everything after the first error to the end of the trace*. On a cascading failure that is effectively the whole log. | Cap the trailing window too (`ERROR_TRAILING_LINES`), keeping the first and last error regions. |
| 1.12 | `s3_uploader.py:13-15` | With `S3_LOGS_BUCKET` unset it returns `s3://mock-bucket/...`, which is stored in the casebook as if real while the log text is discarded. | Return `None` and keep the truncated text inline, or fail loudly. |
| 1.13 | `dlq_publisher.py:41` | The poison-pill path passes a raw `str` payload; `payload.get('eventId')` then raises `AttributeError` inside the `try`, logging "Failed to publish to DLQ" *after* the send succeeded. | Normalise the payload to a dict, or guard with `isinstance`. |
| 1.14 | `requirements.txt` | Missing `langchain-mistralai` (required by the active `MOCK_LLM_WITH_MISTRAL=true` path), `pytest`, and `httpx`. A clean install cannot boot the configured provider or run the tests. **[verified installed but undeclared]** | Add them; pin versions consistently with the rest of the file. |
| 1.15 | `check_drift.py:9` vs `rules.csv` | `KNOWN_DB_COLUMNS` expects `rule_data, reject_reason_code, rule_description, is_active`; `rules.csv` has a **single** column, `rule_id` (416 rows). The drift detector reports drift on every run. **[verified]** | Re-export `rules.csv` with real columns, or update `KNOWN_DB_COLUMNS` to the true schema. |
| 1.16 | `InvestigatorAgent.md:3,10-12` | The prompt says "Use your tools" and "You MUST call the `lookup_rule_by_reason_code` tool", but the node is built as `create_react_agent(llm, tools=[])`. The model is instructed to do something it cannot, inviting fabricated tool narration. | Rewrite the prompt to state that the rule is pre-fetched and supplied in context. |
| 1.17 | `storage/local.py:38` | `load()` calls `_get_dir()`, which `makedirs` — so every existence check creates an empty `casebook_<id>/` directory, including for events that are skipped. | Only create directories in `save()`. |
| 1.18 | `reducer.py:141-145` | `apply_evidence_guardrails` is annotated `-> list[dict]` but returns a `dict`. | Correct the annotation; consider a `TypedDict`. |
| 1.19 | `routes.py:31-51` | `_rate_limits` is mutated from multiple threadpool threads with no lock (read-modify-write on the per-IP list). | Guard with a `threading.Lock`, or move to a shared store if more than one API replica is planned. |
| 1.20 | project-wide | `.agents/AGENTS.md` forbids emojis in the codebase; `s3_uploader.py`, `approve_replays.py`, and `prune_checkpoints.py` print them. | Strip. |

---

## Phase 2 — Optimizations (P2)

Ordered by payoff per unit of effort.

### 2.1 Stop rebuilding the Reviewer agent on every packet
`agent_orchestrator.py:182` constructs `create_react_agent` inside `reviewer_node`
because `add_learning_rule` closes over `event_id` and `investigation`. That
re-does tool-schema binding on every review, and every retry loop.

Build it once at graph-construction time and pass the per-packet context through
the tool arguments (the LLM already has the event id) or a `contextvars` binding.

### 2.2 Drop the unused `simple` LLM
`agent_orchestrator.py:39` creates `simple_llm` and never uses it. That is a wasted
client construction (and, on the HF path, a wasted handshake) per process. Either
delete it, or use it where it belongs — the Reviewer is a bounded verdict task and
is a natural fit for the cheaper tier, which would meaningfully cut cost per packet.

### 2.3 Stop re-sending the full Kafka payload on every retry
`agent_orchestrator.py:129` does `json.dumps(payload)` of the entire message into
the Investigator prompt, and the whole loop re-runs on rejection. Combined with the
reduced logs and the rule JSON, a 3-retry packet pays for that payload three times.

Project the payload down to the fields the prompt actually needs
(`eventId`, `packetMetaData`, `packetExecutionSummary`, `flowMetaData.stage`), and
on retries send only the delta plus the reviewer feedback rather than the full
context again.

### 2.4 Index the mock rules table once
`tool_registry.py:29-50, 104` re-scans a pandas DataFrame with
`db[col].astype(str) == reason_code` per lookup — an O(n) string cast over every row.
Build a `dict[reason_code] -> list[rows]` once at load time. Also cache the
"file missing" outcome: `_load_mock_db` currently returns an uncached empty frame
and re-stats the path on every call.

### 2.5 Reuse one Drain3 miner per process
Beyond the correctness fix in 0.2, constructing `FilePersistence` + `TemplateMiner`
per packet re-reads and re-deserialises the whole state file each time. Hold a
single instance behind the lock and snapshot to disk periodically instead.

### 2.6 Do not block the API threadpool for minutes
`process_rejection` is a sync `def`, so FastAPI runs it in the default 40-slot
threadpool, and each packet occupies a slot for the full multi-minute agent run.
With `MAX_CONCURRENT_INVESTIGATIONS=5` upstream this is survivable, but `/health`
and `/ready` share that pool and will queue behind investigations under load.

Either move the invocation onto a dedicated bounded executor, or make the endpoint
`async def` and `await asyncio.to_thread(...)` with its own semaphore.

### 2.7 Cheaper `/ready`
`get_producer()` attempts a Kafka connection on every probe. Cache the producer
health and refresh on an interval so a liveness probe cannot stall on broker DNS.

### 2.8 Replace `print()` with the structured logger
`ARCHITECTURE.md` claims structured logging, but the orchestrator, tool registry,
and the entire `log_pipeline` use bare `print()`. Those lines are unparseable,
uncorrelated (no `event_id`), and interleave badly across five worker threads.
Convert to `structlog` with a bound `event_id`.

### 2.9 Rate-limiter eviction
`routes.py:36-40` scans all tracked IPs with a `max()` per entry once the dict
exceeds 1000. Cheap today; switch to a `deque` per IP with monotonic timestamps if
request volume grows.

---

## Suggested sequencing

| Step | Contents | Rationale |
|------|----------|-----------|
| 1 | 0.12 (fix tests) + 0.3 (path unification) | Get a green suite and a working readiness probe before changing behaviour. |
| 2 | 0.1, 0.2, 1.2 | The evidence-contamination cluster. Rebuild the catalog immediately after. |
| 3 | 0.4, 0.5, 0.6 | Control-flow correctness in the graph and the idempotency guard. |
| 4 | 0.7, 0.8 | Timeout and retry behaviour; removes the terminal-status race. |
| 5 | 0.9, 0.10, 0.11 | Consumer backpressure, offset safety, path validation. |
| 6 | Phase 1 table | Reliability sweep, roughly top to bottom. |
| 7 | 2.1-2.4 | Cost and latency once behaviour is stable. |
| 8 | 2.5-2.9 | Hygiene and scaling headroom. |

Add a regression test alongside each Phase 0 fix — in particular one asserting that
`cluster_logs` on flow B never returns a template id unique to flow A, since that is
the failure mode most likely to silently return.
