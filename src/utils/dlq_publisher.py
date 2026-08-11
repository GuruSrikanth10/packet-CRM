import json
import logging
from kafka import KafkaProducer
from src.utils.env import get_required_env

logger = logging.getLogger(__name__)

dlq_topic = get_required_env("KAFKA_DLQ_TOPIC", "rejected-packets-dlq")
brokers = [
    b.strip() for b in get_required_env("KAFKA_CONSUMER_BROKERS", "localhost:9092").split(",") if b.strip()
]

_producer = None

def get_producer():
    global _producer
    if _producer is None:
        try:
            _producer = KafkaProducer(
                bootstrap_servers=brokers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8")
            )
        except Exception as e:
            logger.error(f"Failed to initialize DLQ producer: {e}")
    return _producer

def publish_to_dlq(payload, error_message: str):
    producer = get_producer()
    if not producer:
        logger.error("DLQ Producer not available, dropping DLQ message.")
        return

    dlq_message = {
        "original_payload": payload,
        "dlq_error": error_message
    }

    # The poison-pill path passes the raw (undecoded-JSON) string payload,
    # not a dict -- payload.get(...) would raise AttributeError here and get
    # logged as a publish failure even after producer.send()/flush()
    # succeeded (1.13).
    event_id = payload.get("eventId", "unknown") if isinstance(payload, dict) else "unknown"

    try:
        producer.send(dlq_topic, dlq_message)
        producer.flush()
        logger.info(f"Successfully published to DLQ for event {event_id}")
    except Exception as e:
        logger.error(f"Failed to publish to DLQ: {e}")
