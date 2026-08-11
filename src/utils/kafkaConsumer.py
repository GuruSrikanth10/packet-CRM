import os
import json
import logging
import requests
import threading
from concurrent.futures import ThreadPoolExecutor
from kafka import KafkaConsumer
from kafka.structs import OffsetAndMetadata
from pathlib import Path
from .env import get_required_env
from src.storage.factory import get_casebook_storage

logger = logging.getLogger(__name__)

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

def forward_signal_to_internal_endpoint(signal: dict):
    api_key = os.environ.get("PACKET_CRM_API_KEY", "dev-secret-key")
    headers = {"X-API-Key": api_key}
    response = requests.post(
        kafkaConsumerInternalEndpoint,
        json=signal,
        headers=headers,
        proxies={"http": None, "https": None},
        timeout=kafkaConsumerInternalTimeoutSec,
    )
    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        logger.error(f"HTTP Error forwarding signal: {e.response.text}")
        raise
    return response

def _process_and_commit(signal: dict):
    event_id = signal.get("eventId")
    try:
        logger.info("Forwarding event %s to internal endpoint", event_id)
        response = forward_signal_to_internal_endpoint(signal)
        logger.info("Forwarded event %s status=%s", event_id, response.status_code)
    except requests.exceptions.Timeout:
        logger.error(f"Timeout forwarding Kafka message Event ID: {event_id}")
        storage = get_casebook_storage()
        storage.save(event_id, {
            "packet_metadata": {"eid": event_id},
            "packet_status": {"status": "FAILED_TIMEOUT"},
            "resolution": {"synthesis": "Investigation exceeded maximum allowed time."}
        })
        from src.utils.dlq_publisher import publish_to_dlq
        publish_to_dlq(signal, "Pipeline timed out (PACKET_TIMEOUT_SECONDS exceeded)")
    except Exception:
        logger.exception(f"Error forwarding Kafka message Event ID: {event_id}")
    finally:
        _queue_semaphore.release()

def _handle_one_message(tp, msg):
    """Process a single polled Kafka message: validate, dedupe-check, enqueue,
    and commit. Extracted from consume_forever's loop body so it can be
    exercised directly in tests without a real KafkaConsumer.

    Caller must have already acquired `_queue_semaphore` for this message.
    """
    # Tracks whether _process_and_commit was handed the semaphore (it
    # releases it in its own `finally`). Without this flag, an exception
    # raised by consumer.commit() *after* a successful submit() would hit
    # the `except` below and release a second time, permanently raising the
    # concurrency ceiling (0.9).
    submitted = False

    # Commit only this message's offset, not the whole polled batch -- a
    # bare consumer.commit() advances past every message in the batch,
    # including ones later in it that haven't been enqueued yet, so a crash
    # mid-batch would silently lose them (0.10).
    message_offsets = {tp: OffsetAndMetadata(msg.offset + 1, None, -1)}

    try:
        payload = msg.value.decode("utf-8", errors="replace")
        try:
            signal = json.loads(payload)
            from src.models.schemas import MessagePayload
            # Validate payload structure (Poison-pill check)
            MessagePayload(**signal)
        except Exception as validation_err:
            logger.error(f"Poison-pill payload detected: {validation_err}")
            from src.utils.dlq_publisher import publish_to_dlq
            publish_to_dlq(payload, f"Structural validation failed: {validation_err}")
            consumer.commit(message_offsets)
            return

        # Check packetStatus if it's REJECTED
        summary = signal.get("packetExecutionSummary", {})
        if summary.get("packetStatus") != "REJECTED":
            print(f"Skipping non-rejected packet {signal.get('eventId')}")
            consumer.commit(message_offsets)
            return

        # Dedupe check
        event_id = signal.get("eventId")
        storage = get_casebook_storage()
        if storage.exists(event_id, terminal_only=True):
            print(f"[KAFKA] Skipping Event ID: {event_id}. Terminal casebook already exists.")
            consumer.commit(message_offsets)
            return

        print(f"\n[KAFKA] Received REJECTED packet with Event ID: {event_id}")
        print(f"[KAFKA] Enqueueing for agentic analysis...")

        _worker_pool.submit(_process_and_commit, signal)
        submitted = True

        # Since we are decoupling, we commit the offset right after enqueueing.
        # If the process crashes before completion, the DLQ / Checkpointer handles it.
        consumer.commit(message_offsets)
        print(f"[KAFKA] Enqueued and committed Event ID: {event_id}")
    except Exception:
        payload_sample = payload[:500] if 'payload' in locals() else 'Decode failed'
        logger.exception(f"Error processing Kafka message. Raw payload: {payload_sample}")
    finally:
        if not submitted:
            _queue_semaphore.release()


def consume_forever():
    global consumer
    if consumer is None:
        consumer = KafkaConsumer(
            kafkaConsumerTopicName,
            group_id=kafkaConsumerGroupId,
            bootstrap_servers=kafkaConsumerBrokers,
            auto_offset_reset="earliest",
            enable_auto_commit=False,
        )
    logger.info("Listening on topic=%s", kafkaConsumerTopicName)
    print("\n" + "="*50)
    print(f"KAFKA CONSUMER STARTED")
    print(f"Brokers: {kafkaConsumerBrokers}")
    print(f"Topic: {kafkaConsumerTopicName}")
    print("="*50 + "\n")

    heartbeat_file = Path(__file__).resolve().parent.parent.parent / "local_checkpoints" / "consumer_heartbeat.txt"
    os.makedirs(heartbeat_file.parent, exist_ok=True)

    import time
    while True:
        # We use poll() to have a chance to update the heartbeat periodically even when idle
        records = consumer.poll(timeout_ms=5000)

        with open(heartbeat_file, "w") as f:
            f.write(str(time.time()))

        if not records:
            continue

        for tp, messages in records.items():
            for msg in messages:
                # Block if queue is full BEFORE parsing (creates backpressure on polling)
                _queue_semaphore.acquire()
                _handle_one_message(tp, msg)
