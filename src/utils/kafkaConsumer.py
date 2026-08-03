import json
import logging
import requests
import threading
from concurrent.futures import ThreadPoolExecutor
from kafka import KafkaConsumer
from .env import get_required_env
from src.storage.factory import get_casebook_storage

logger = logging.getLogger(__name__)

kafkaConsumerBrokers = [
    b.strip() for b in get_required_env("KAFKA_CONSUMER_BROKERS", "localhost:9092").split(",") if b.strip()
]
kafkaConsumerTopicName = get_required_env("KAFKA_CONSUMER_TOPIC_NAME", "rejections")
kafkaConsumerGroupId = get_required_env("KAFKA_CONSUMER_GROUP_ID", "rejection-agents-group")
kafkaConsumerInternalEndpoint = get_required_env("KAFKA_CONSUMER_INTERNAL_ENDPOINT", "http://localhost:8000/process-rejection")
kafkaConsumerInternalTimeoutSec = float(get_required_env("KAFKA_CONSUMER_INTERNAL_TIMEOUT_SEC", "300"))

# Consumer will be instantiated lazily in consume_forever
consumer = None
MAX_CONCURRENT_INVESTIGATIONS = int(get_required_env("MAX_CONCURRENT_INVESTIGATIONS", "5"))
_worker_pool = ThreadPoolExecutor(max_workers=MAX_CONCURRENT_INVESTIGATIONS)
_queue_semaphore = threading.Semaphore(MAX_CONCURRENT_INVESTIGATIONS)

def forward_signal_to_internal_endpoint(signal: dict):
    response = requests.post(
        kafkaConsumerInternalEndpoint,
        json=signal,
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
    try:
        logger.info("Forwarding event %s to internal endpoint", signal.get("eventId"))
        response = forward_signal_to_internal_endpoint(signal)
        logger.info("Forwarded event %s status=%s", signal.get("eventId"), response.status_code)
    except Exception:
        logger.exception(f"Error forwarding Kafka message Event ID: {signal.get('eventId')}")
    finally:
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
    for msg in consumer:
        try:
            payload = msg.value.decode("utf-8", errors="replace")
            signal = json.loads(payload)
            
            # Check packetStatus if it's REJECTED
            summary = signal.get("packetExecutionSummary", {})
            if summary.get("packetStatus") != "REJECTED":
                print(f"Skipping non-rejected packet {signal.get('eventId')}")
                continue
            
            # Dedupe check
            event_id = signal.get("eventId")
            storage = get_casebook_storage()
            if storage.exists(event_id, terminal_only=True):
                print(f"[KAFKA] Skipping Event ID: {event_id}. Terminal casebook already exists.")
                consumer.commit()
                continue
                
            print(f"\n[KAFKA] Received REJECTED packet with Event ID: {event_id}")
            print(f"[KAFKA] Enqueueing for agentic analysis...")
            
            # Block if queue is full
            _queue_semaphore.acquire()
            _worker_pool.submit(_process_and_commit, signal)
            
            # Since we are decoupling, we commit the offset right after enqueueing.
            # If the process crashes before completion, the DLQ / Checkpointer handles it.
            consumer.commit()
            print(f"[KAFKA] Enqueued and committed Event ID: {event_id}")
        except Exception as e:
            payload_sample = payload[:500] if 'payload' in locals() else 'Decode failed'
            logger.exception(f"Error processing Kafka message. Raw payload: {payload_sample}")
