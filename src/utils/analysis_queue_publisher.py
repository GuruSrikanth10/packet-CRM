"""
Producer for the analysis queue (the fetch/analyze consumer split).

POST /fetch-logs publishes the original payload here once its logs are
fetched and persisted; the slow consumer subscribes to this topic and
forwards each message to POST /analyze-rejection. Mirrors dlq_publisher.py's
shape (cached producer, `acks="all"` so the last line of defence for handing
work to the slow consumer isn't a leader-only ack), but publishes the payload
unwrapped -- there is no error to envelope here, unlike a DLQ message.
"""
import json
from kafka import KafkaProducer
from src.utils.env import get_required_env
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

analysis_topic = get_required_env("PACKET_ANALYSIS_TOPIC_NAME", "packet-analysis-queue")
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
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",
                retries=3,
            )
        except Exception as e:
            logger.error("Failed to initialize analysis-queue producer", error=str(e))
    return _producer

def publish_to_analysis_queue(payload: dict) -> bool:
    """Publish a fetched packet's payload for the slow consumer to pick up.

    Returns True on success. Raises on failure rather than swallowing it --
    unlike the DLQ publisher (which is itself the last line of defence and has
    nowhere further to escalate to), a failed publish here must surface as a
    non-2xx response from POST /fetch-logs so the fast consumer does not
    commit the original topic's offset and Kafka redelivers the message (see
    forward_signal_to_internal_endpoint / _process_and_commit in
    kafkaConsumer.py, which already handle that on any internal-endpoint
    failure).
    """
    producer = get_producer()
    if not producer:
        raise RuntimeError("Analysis-queue producer is not available")

    event_id = payload.get("eventId", "unknown") if isinstance(payload, dict) else "unknown"

    producer.send(analysis_topic, payload)
    producer.flush()
    logger.info("Published to analysis queue", event_id=event_id, topic=analysis_topic)
    return True
