#!/usr/bin/env python3
"""Capture a corpus of real DLT messages for the Phase 0 decision gate.

See DLT_PLAN.md section 10, Phase 0. This tool only *captures* -- it makes no
judgements about what it captured. The measurements Phase 0 asks for (class
distribution, fingerprint cardinality, truncation rate) are computed by
`dlt_report.py --corpus` once Phases 1-2 exist to compute them with.

Deliberately never joins a consumer group. It `assign()`s partitions directly
and never commits, so it cannot rebalance anyone else's consumer, cannot
advance any group's offsets, and is safe to run against a live DLT while
developers are browsing the same topic in Kafka UI.

Usage:
    python -m src.tools.dlt_sample --limit 100
    python -m src.tools.dlt_sample --limit 500 --from earliest --output /tmp/corpus
    python -m src.tools.dlt_sample --limit 20 --no-redact   # never on real data
"""
import argparse
import json
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.log_pipeline import redaction  # noqa: E402

#: Headers whose values are structural Kafka/Spring metadata -- offsets,
#: partitions, epoch timestamps, topic names, Java FQCNs. They never carry
#: resident PII, and redacting them would corrupt the very fields the parser
#: keys on. A 12-digit epoch-second timestamp would otherwise be scrubbed as
#: an Aadhaar number.
STRUCTURAL_HEADERS = frozenset({
    "kafka_original-offset",
    "kafka_original-partition",
    "kafka_original-topic",
    "kafka_original-timestamp",
    "kafka_original-timestamp-type",
    "kafka_dlt-original-consumer-group",
    "kafka_exception-fqcn",
    "kafka_exception-cause-fqcn",
    "retry_topic-attempts",
    "retry_topic-original-timestamp",
    "retry_topic-backoff-timestamp",
    "__TypeId__",
})


def decode_headers(raw_headers) -> dict:
    """kafka-python hands back [(str, bytes)]; make it a plain str->str dict.

    A duplicate header key keeps the last value, which is what Spring's own
    consumer-side accessors do. A non-UTF-8 value is replaced rather than
    raising -- losing one header must never cost us the whole message.
    """
    out = {}
    for key, value in (raw_headers or []):
        name = key.decode("utf-8", errors="replace") if isinstance(key, bytes) else str(key)
        if value is None:
            out[name] = None
        elif isinstance(value, bytes):
            out[name] = value.decode("utf-8", errors="replace")
        else:
            out[name] = str(value)
    return out


def redact_headers(headers: dict) -> dict:
    """Redact free-text header values, leaving structural metadata intact."""
    out = {}
    for name, value in headers.items():
        if value is None or name in STRUCTURAL_HEADERS:
            out[name] = value
        else:
            out[name] = redaction.redact_text(value).text
    return out


def decode_payload(raw_value):
    """Best-effort payload decode. Returns (parsed, raw_text)."""
    if raw_value is None:
        return None, None
    text = raw_value.decode("utf-8", errors="replace") if isinstance(raw_value, bytes) else str(raw_value)
    try:
        return json.loads(text), text
    except (ValueError, TypeError):
        return None, text


def _seek_targets(consumer, topic: str, limit: int, start_from: str):
    """Assign every partition and position it so we read at most `limit`."""
    from kafka import TopicPartition

    partition_ids = consumer.partitions_for_topic(topic)
    if not partition_ids:
        raise SystemExit(f"Topic '{topic}' has no partitions visible to this client.")

    targets = [TopicPartition(topic, p) for p in sorted(partition_ids)]
    consumer.assign(targets)

    if start_from == "earliest":
        consumer.seek_to_beginning(*targets)
        return targets

    # Latest: walk back per-partition so the sample is the most recent traffic.
    per_partition = max(1, -(-limit // len(targets)))
    end_offsets = consumer.end_offsets(targets)
    beginning = consumer.beginning_offsets(targets)
    for tp in targets:
        start = max(beginning[tp], end_offsets[tp] - per_partition)
        consumer.seek(tp, start)
    return targets


def capture(topic: str, brokers: list, limit: int, start_from: str, do_redact: bool) -> list:
    from kafka import KafkaConsumer

    consumer = KafkaConsumer(
        bootstrap_servers=brokers,
        enable_auto_commit=False,
        group_id=None,
        consumer_timeout_ms=15000,
    )
    try:
        _seek_targets(consumer, topic, limit, start_from)

        captured = []
        for msg in consumer:
            headers = decode_headers(msg.headers)
            if do_redact:
                headers = redact_headers(headers)

            parsed, raw_text = decode_payload(msg.value)
            if do_redact and raw_text is not None:
                raw_text = redaction.redact_text(raw_text).text
                try:
                    parsed = json.loads(raw_text)
                except (ValueError, TypeError):
                    parsed = None

            captured.append({
                "_source": {
                    "dlt_topic": topic,
                    "dlt_partition": msg.partition,
                    "dlt_offset": msg.offset,
                    "dlt_timestamp": msg.timestamp,
                    "redacted": do_redact,
                },
                "headers": headers,
                "payload": parsed,
                "payload_raw": None if parsed is not None else raw_text,
            })
            if len(captured) >= limit:
                break
        return captured
    finally:
        consumer.close()


def summarise(captured: list) -> dict:
    """The few things measurable without Phases 1-2."""
    header_names = {}
    missing_stacktrace = 0
    for item in captured:
        for name in item["headers"]:
            header_names[name] = header_names.get(name, 0) + 1
        if not item["headers"].get("kafka_exception-stacktrace"):
            missing_stacktrace += 1
    return {
        "captured": len(captured),
        "header_names": dict(sorted(header_names.items(), key=lambda kv: -kv[1])),
        "missing_stacktrace": missing_stacktrace,
    }


def main():
    parser = argparse.ArgumentParser(description="Capture DLT messages for the Phase 0 corpus")
    parser.add_argument("--topic", default=os.environ.get("DLT_CONSUMER_TOPIC_NAME", "packet-dlt"))
    parser.add_argument("--brokers", default=os.environ.get("KAFKA_CONSUMER_BROKERS", "localhost:9092"))
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--from", dest="start_from", choices=("latest", "earliest"), default="latest")
    parser.add_argument("--output", default="tests/fixtures/dlt/corpus")
    parser.add_argument("--no-redact", dest="redact", action="store_false",
                        help="Skip PII redaction. Never use against real data.")
    parser.set_defaults(redact=True)
    args = parser.parse_args()

    brokers = [b.strip() for b in args.brokers.split(",") if b.strip()]
    print(f"Capturing up to {args.limit} messages from '{args.topic}' ({args.start_from}) ...")

    captured = capture(args.topic, brokers, args.limit, args.start_from, args.redact)
    if not captured:
        print("No messages captured. Is the topic empty, or the name wrong?")
        return

    out_dir = Path(args.output)
    out_dir.mkdir(parents=True, exist_ok=True)
    for item in captured:
        src = item["_source"]
        name = f"{src['dlt_partition']:04d}-{src['dlt_offset']:012d}.json"
        (out_dir / name).write_text(json.dumps(item, indent=2, ensure_ascii=False), encoding="utf-8")

    summary = summarise(captured)
    (out_dir / "_summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")

    print(f"\nWrote {summary['captured']} messages to {out_dir}/")
    if not args.redact:
        print("WARNING: captured WITHOUT redaction. Do not commit this corpus.")
    if summary["missing_stacktrace"]:
        print(f"WARNING: {summary['missing_stacktrace']} message(s) carry no stacktrace header.")
    print("\nHeader names seen:")
    for name, count in summary["header_names"].items():
        print(f"  {count:5d}  {name}")
    print("\nNext: run `dlt_report.py --corpus` once Phases 1-2 land to fill "
          "DLT_PLAN.md section 11.")


if __name__ == "__main__":
    main()
