import os
import json
import requests
import signal
import threading
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from kafka import KafkaConsumer
from kafka.structs import OffsetAndMetadata
from pathlib import Path
from .env import get_required_env
from src.storage.factory import get_casebook_storage
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

kafkaConsumerBrokers = [
    b.strip() for b in get_required_env("KAFKA_CONSUMER_BROKERS", "localhost:9092").split(",") if b.strip()
]
kafkaConsumerTopicName = get_required_env("KAFKA_CONSUMER_TOPIC_NAME", "rejections")
kafkaConsumerGroupId = get_required_env("KAFKA_CONSUMER_GROUP_ID", "rejection-agents-group")
kafkaConsumerInternalEndpoint = get_required_env("KAFKA_CONSUMER_INTERNAL_ENDPOINT", "http://localhost:8000/process-rejection")
kafkaConsumerInternalTimeoutSec = float(os.environ.get("PACKET_TIMEOUT_SECONDS", "300"))

# Consumer will be instantiated lazily in consume_forever
consumer = None
MAX_CONCURRENT_INVESTIGATIONS = int(get_required_env("MAX_CONCURRENT_INVESTIGATIONS", "5"))
_worker_pool = ThreadPoolExecutor(max_workers=MAX_CONCURRENT_INVESTIGATIONS)
# Create a bounded queue by acquiring semaphore BEFORE reading from Kafka
_queue_semaphore = threading.Semaphore(MAX_CONCURRENT_INVESTIGATIONS)

# Set by SIGTERM/SIGINT so the poll loop can stop between messages rather
# than being killed mid-investigation (F12).
_shutdown = threading.Event()

# How long to wait for in-flight investigations to finish on shutdown. Kept
# under a typical Kubernetes terminationGracePeriodSeconds (30s) so the drain
# completes before SIGKILL rather than being cut off by it.
SHUTDOWN_DRAIN_SECONDS = float(os.environ.get("SHUTDOWN_DRAIN_SECONDS", "25"))

HEARTBEAT_INTERVAL_SECONDS = float(os.environ.get("HEARTBEAT_INTERVAL_SECONDS", "5"))


class OffsetTracker:
    """Per-partition offset bookkeeping that only ever commits a safe floor.

    Workers finish out of order. Committing each completed offset as it
    arrives means that when offsets 10, 11, 12 are dispatched together and 12
    finishes first, `commit(13)` runs while 10 and 11 are still in flight -- a
    crash then resumes the group at 13 and those two messages are never
    redelivered (F3).

    So completion is recorded, not committed. What gets committed is the
    low-water mark: the highest offset below which every dispatched message
    has completed. Anything at or above the lowest still-running offset waits.
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._dispatched = defaultdict(set)
        self._completed = defaultdict(set)

    def dispatched(self, tp, offset: int):
        with self._lock:
            self._dispatched[tp].add(offset)

    def completed(self, tp, offset: int):
        with self._lock:
            self._completed[tp].add(offset)

    def in_flight(self) -> int:
        with self._lock:
            return sum(len(self._dispatched[tp] - self._completed[tp])
                       for tp in self._dispatched)

    def take_committable(self) -> dict:
        """Return {tp: OffsetAndMetadata} for the safe floor, and retire it.

        Offsets are only retired once they are being committed, so a failure
        to commit leaves them to be retried on the next drain.
        """
        with self._lock:
            commits = {}
            for tp, completed in list(self._completed.items()):
                if not completed:
                    continue

                still_running = self._dispatched[tp] - completed
                if still_running:
                    # Everything strictly below the lowest in-flight offset is
                    # safe; that offset itself is not.
                    floor = min(still_running)
                    committable = {o for o in completed if o < floor}
                else:
                    committable = set(completed)

                if not committable:
                    continue

                commits[tp] = OffsetAndMetadata(max(committable) + 1, None, -1)
                self._dispatched[tp] -= committable
                self._completed[tp] -= committable

            return commits


_offset_tracker = OffsetTracker()


def forward_signal_to_internal_endpoint(signal_payload: dict):
    api_key = os.environ.get("PACKET_CRM_API_KEY", "dev-secret-key")
    headers = {"X-API-Key": api_key}
    response = requests.post(
        kafkaConsumerInternalEndpoint,
        json=signal_payload,
        headers=headers,
        proxies={"http": None, "https": None},
        timeout=kafkaConsumerInternalTimeoutSec,
    )
    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        logger.error("HTTP error forwarding signal to the internal endpoint",
                     event_id=signal_payload.get("eventId"),
                     status_code=getattr(e.response, "status_code", None))
        raise
    return response


def _record_completion(message_offsets):
    """Mark every offset in `message_offsets` complete on the tracker.

    `message_offsets` carries the *next* offset to commit (offset + 1), which
    is the shape consumer.commit() wants; the tracker works in message
    offsets, hence the -1.
    """
    if not message_offsets:
        return
    for tp, offset_and_metadata in message_offsets.items():
        _offset_tracker.completed(tp, offset_and_metadata.offset - 1)


def _process_and_commit(signal_payload: dict, message_offsets: dict = None):
    event_id = signal_payload.get("eventId")
    success = False
    try:
        logger.info("Forwarding event to internal endpoint", event_id=event_id)
        response = forward_signal_to_internal_endpoint(signal_payload)
        logger.info("Forwarded event", event_id=event_id, status_code=response.status_code)
        success = True
    except requests.exceptions.Timeout:
        logger.error("Timeout forwarding Kafka message", event_id=event_id)
        storage = get_casebook_storage()
        # Both casebook.json and status.json move to FAILED_TIMEOUT together.
        # Writing only casebook.json left the API's late-result guard -- which
        # reads status.json -- unable to see this verdict, so a slow agent run
        # could overwrite it with COMPLETED while the DLQ message stayed
        # queued (F4).
        storage.save_terminal(event_id, {
            "packet_metadata": {"eid": event_id},
            "packet_status": {"status": "FAILED_TIMEOUT"},
            "resolution": {"synthesis": "Investigation exceeded maximum allowed time."}
        })
        from src.utils.dlq_publisher import publish_to_dlq
        publish_to_dlq(signal_payload, "Pipeline timed out (PACKET_TIMEOUT_SECONDS exceeded)")
        success = True  # Successfully handled via DLQ, so we should commit
    except Exception:
        logger.exception("Error forwarding Kafka message; will not commit offset", event_id=event_id)
    finally:
        if success:
            _record_completion(message_offsets)
            logger.info("Event completed; offset eligible for commit", event_id=event_id)
        _queue_semaphore.release()


def _handle_one_message(tp, msg):
    """Process a single polled Kafka message: validate, dedupe-check, enqueue.

    Caller must have already acquired `_queue_semaphore` for this message.

    No path commits directly any more. Every outcome -- dispatched, skipped,
    or poison-pilled -- is recorded on `_offset_tracker`, and the poll loop
    commits the resulting low-water mark (F3). A skip that committed
    immediately had the same batch-ordering hazard as an out-of-order
    completion: skipping offset 11 while offset 10 was still in flight would
    commit 12 and strand 10.
    """
    submitted = False

    # Track only this message's offset, never the whole polled batch -- a bare
    # consumer.commit() advances past every message in the batch, including
    # ones not yet enqueued (0.10).
    message_offsets = {tp: OffsetAndMetadata(msg.offset + 1, None, -1)}
    _offset_tracker.dispatched(tp, msg.offset)

    try:
        payload = msg.value.decode("utf-8", errors="replace")
        try:
            signal_payload = json.loads(payload)
            from src.models.schemas import MessagePayload
            # Validate payload structure (Poison-pill check)
            MessagePayload(**signal_payload)
        except Exception as validation_err:
            logger.error("Poison-pill payload detected", error=str(validation_err))
            from src.utils.dlq_publisher import publish_to_dlq
            publish_to_dlq(payload, f"Structural validation failed: {validation_err}")
            _record_completion(message_offsets)
            return

        # Check packetStatus if it's REJECTED
        summary = signal_payload.get("packetExecutionSummary", {})
        if summary.get("packetStatus") != "REJECTED":
            logger.info("Skipping non-rejected packet", event_id=signal_payload.get("eventId"))
            _record_completion(message_offsets)
            return

        # Dedupe check
        event_id = signal_payload.get("eventId")
        storage = get_casebook_storage()
        if storage.exists(event_id, terminal_only=True):
            logger.info("Skipping event; terminal casebook already exists", event_id=event_id)
            _record_completion(message_offsets)
            return

        logger.info("Received REJECTED packet; enqueueing for analysis", event_id=event_id)

        _worker_pool.submit(_process_and_commit, signal_payload, message_offsets)
        submitted = True
    except Exception:
        payload_sample = payload[:500] if 'payload' in locals() else 'Decode failed'
        logger.exception("Error processing Kafka message", payload_sample=payload_sample)
    finally:
        if not submitted:
            _queue_semaphore.release()


def _commit_ready_offsets():
    """Commit whatever the tracker says is safe. Never raises."""
    commits = _offset_tracker.take_committable()
    if not commits:
        return
    try:
        consumer.commit(commits)
        logger.debug("Committed offsets", commits=str(commits))
    except Exception as e:
        logger.error("Error committing offset", error=str(e))


def _write_heartbeat(heartbeat_file: Path):
    """Atomically stamp the heartbeat file.

    Atomic because /health reads this concurrently; a plain truncate-and-write
    can be read as an empty file mid-write.
    """
    try:
        tmp_path = heartbeat_file.with_suffix(".tmp")
        tmp_path.write_text(str(time.time()), encoding="utf-8")
        os.replace(tmp_path, heartbeat_file)
    except Exception as e:
        logger.warning("Failed to write heartbeat", error=str(e))


def _start_heartbeat_thread(heartbeat_file: Path) -> threading.Thread:
    """Stamp the heartbeat on its own ticker, independent of the poll loop.

    The heartbeat used to be written once per poll() cycle, but the poll loop
    blocks on `_queue_semaphore.acquire()` whenever all worker slots are busy
    -- for as long as an investigation takes, i.e. minutes. /health treats a
    heartbeat older than 30s as a dead consumer, so under exactly the
    sustained load the semaphore exists to manage, a liveness probe would
    restart a perfectly healthy pod and kill every in-flight packet (F9).
    """
    def _tick():
        while not _shutdown.is_set():
            _write_heartbeat(heartbeat_file)
            _shutdown.wait(HEARTBEAT_INTERVAL_SECONDS)

    thread = threading.Thread(target=_tick, name="consumer-heartbeat", daemon=True)
    thread.start()
    return thread


def request_shutdown(signum=None, frame=None):
    """Signal handler: stop polling and let the drain run (F12)."""
    logger.info("Shutdown requested; draining in-flight work", signal=signum)
    _shutdown.set()


def _install_signal_handlers():
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            signal.signal(sig, request_shutdown)
        except (ValueError, OSError):
            # Not the main thread (e.g. under a test runner) -- non-fatal.
            logger.debug("Could not install signal handler", signal=str(sig))


def _drain_and_commit():
    """Wait briefly for in-flight work, then commit whatever completed.

    Bounded: a packet mid-LLM-call may simply not finish in time. Its offset
    stays uncommitted, so Kafka redelivers it -- which is the correct
    at-least-once outcome, not a loss.
    """
    deadline = time.monotonic() + SHUTDOWN_DRAIN_SECONDS
    while time.monotonic() < deadline:
        remaining = _offset_tracker.in_flight()
        if remaining == 0:
            break
        logger.info("Draining in-flight investigations", remaining=remaining)
        time.sleep(0.5)

    _worker_pool.shutdown(wait=False, cancel_futures=True)

    if consumer is not None:
        _commit_ready_offsets()
        try:
            consumer.close()
        except Exception as e:
            logger.warning("Error closing consumer", error=str(e))

    logger.info("Consumer shutdown complete")


def consume_forever():
    global consumer
    if consumer is None:
        consumer = KafkaConsumer(
            kafkaConsumerTopicName,
            group_id=kafkaConsumerGroupId,
            bootstrap_servers=kafkaConsumerBrokers,
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            max_poll_records=int(os.environ.get("KAFKA_MAX_POLL_RECORDS", MAX_CONCURRENT_INVESTIGATIONS)),
            max_poll_interval_ms=int(os.environ.get("KAFKA_MAX_POLL_INTERVAL_MS", "900000")),
        )

    _install_signal_handlers()

    logger.info("Kafka consumer started", topic=kafkaConsumerTopicName, brokers=kafkaConsumerBrokers)

    heartbeat_file = Path(__file__).resolve().parent.parent.parent / "local_checkpoints" / "consumer_heartbeat.txt"
    os.makedirs(heartbeat_file.parent, exist_ok=True)
    _start_heartbeat_thread(heartbeat_file)

    try:
        while not _shutdown.is_set():
            # Commit whatever became safe since the last cycle.
            _commit_ready_offsets()

            records = consumer.poll(timeout_ms=5000)

            if not records:
                continue

            for tp, messages in records.items():
                for msg in messages:
                    if _shutdown.is_set():
                        # Stop taking new work; anything not dispatched simply
                        # isn't committed and will be redelivered.
                        break
                    # Block if the pool is full BEFORE parsing (backpressure).
                    _queue_semaphore.acquire()
                    _handle_one_message(tp, msg)
    finally:
        _drain_and_commit()
