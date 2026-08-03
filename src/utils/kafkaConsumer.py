import json
import logging
import requests
from kafka import KafkaConsumer
from .env import get_required_env

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

def consume_forever():
    global consumer
    if consumer is None:
        consumer = KafkaConsumer(
            kafkaConsumerTopicName,
            group_id=kafkaConsumerGroupId,
            bootstrap_servers=kafkaConsumerBrokers,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
        )
    logger.info("Listening on topic=%s", kafkaConsumerTopicName)
    print("Kafka consumer starting...")
    for msg in consumer:
        try:
            payload = msg.value.decode("utf-8", errors="replace")
            signal = json.loads(payload)
            
            # Check packetStatus if it's REJECTED
            summary = signal.get("packetExecutionSummary", {})
            if summary.get("packetStatus") != "REJECTED":
                print(f"Skipping non-rejected packet {signal.get('eventId')}")
                continue
            
            logger.info("Received event %s", signal.get("eventId"))
            response = forward_signal_to_internal_endpoint(signal)
            logger.info("Forwarded event %s to internal endpoint status=%s", signal.get("eventId"), response.status_code)
        except Exception as e:
            payload_sample = payload[:500] if 'payload' in locals() else 'Decode failed'
            logger.exception(f"Error processing Kafka message. Raw payload: {payload_sample}")
