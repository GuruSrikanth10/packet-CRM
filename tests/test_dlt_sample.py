"""
Phase 0 of DLT_PLAN.md -- corpus capture tooling.

Only the pure parts are tested here: header decoding, selective redaction, and
payload decoding. The Kafka leg needs a broker and is exercised by running the
tool, not by a unit test.

The redaction test matters more than it looks. `kafka_original-timestamp` is a
13-digit epoch-millisecond value and `retry_topic-attempts` is a small integer,
but a 12-digit structural value would be scrubbed as an Aadhaar number by the
default patterns -- corrupting exactly the fields the Phase 1 parser keys on.
"""
import json
from pathlib import Path

import pytest

from src.tools.dlt_sample import (
    STRUCTURAL_HEADERS,
    decode_headers,
    decode_payload,
    redact_headers,
    summarise,
)

FIXTURE = Path(__file__).parent / "fixtures" / "dlt" / "reference_business_exception.json"


@pytest.fixture(autouse=True)
def _isolate(monkeypatch):
    for var in ("K8S_REDACT_ENABLED", "K8S_REDACT_EXTRA_PATTERNS"):
        monkeypatch.delenv(var, raising=False)
    yield


@pytest.fixture
def reference():
    return json.loads(FIXTURE.read_text(encoding="utf-8"))


# ======================================================================
# Header decoding
# ======================================================================

def test_bytes_headers_decode_to_str():
    decoded = decode_headers([(b"kafka_original-offset", b"3352")])
    assert decoded == {"kafka_original-offset": "3352"}


def test_non_utf8_header_does_not_raise():
    """Losing one header must never cost us the whole message."""
    decoded = decode_headers([(b"weird", b"\xff\xfe not utf-8")])
    assert "weird" in decoded
    assert isinstance(decoded["weird"], str)


def test_none_header_value_is_preserved():
    assert decode_headers([(b"empty", None)]) == {"empty": None}


def test_duplicate_header_keeps_the_last_value():
    decoded = decode_headers([(b"k", b"first"), (b"k", b"second")])
    assert decoded["k"] == "second"


def test_missing_headers_yield_empty_dict():
    assert decode_headers(None) == {}
    assert decode_headers([]) == {}


# ======================================================================
# Redaction
# ======================================================================

def test_structural_headers_survive_redaction_untouched(reference):
    """The parser keys on these. Scrubbing them would break Phase 1."""
    redacted = redact_headers(reference["headers"])
    for name in STRUCTURAL_HEADERS:
        if name in reference["headers"]:
            assert redacted[name] == reference["headers"][name], (
                f"structural header {name} was altered by redaction"
            )


def test_twelve_digit_structural_value_is_not_scrubbed_as_aadhaar():
    """A 12-digit epoch-second timestamp matches the Aadhaar pattern exactly."""
    headers = {"kafka_original-timestamp": "178686480519"}
    assert len(headers["kafka_original-timestamp"]) == 12
    assert redact_headers(headers)["kafka_original-timestamp"] == "178686480519"


def test_free_text_headers_are_redacted():
    headers = {"kafka_exception-message": "failed for uid 123456789012"}
    assert "[REDACTED:AADHAAR]" in redact_headers(headers)["kafka_exception-message"]


def test_unknown_header_is_redacted_not_trusted():
    """Anything not on the structural list is treated as free text."""
    headers = {"some-new-header": "contact 9876543210"}
    assert "[REDACTED:MOBILE]" in redact_headers(headers)["some-new-header"]


def test_none_valued_header_survives_redaction():
    assert redact_headers({"empty": None}) == {"empty": None}


# ======================================================================
# Payload decoding
# ======================================================================

def test_json_payload_parses():
    parsed, raw = decode_payload(b'{"refId": "abc"}')
    assert parsed == {"refId": "abc"}
    assert raw == '{"refId": "abc"}'


def test_unparseable_payload_returns_raw_text():
    parsed, raw = decode_payload(b"not json at all")
    assert parsed is None
    assert raw == "not json at all"


def test_none_payload():
    assert decode_payload(None) == (None, None)


# ======================================================================
# Summary
# ======================================================================

def test_summarise_counts_headers_and_flags_missing_stacktrace(reference):
    corpus = [
        {"headers": reference["headers"]},
        {"headers": {"kafka_original-topic": "T"}},
    ]
    summary = summarise(corpus)
    assert summary["captured"] == 2
    assert summary["missing_stacktrace"] == 1
    assert summary["header_names"]["kafka_original-topic"] == 2


# ======================================================================
# The fixture itself
# ======================================================================

def test_reference_fixture_is_intact(reference):
    """Guards the fixture against an editing accident -- every later phase
    binds its trap regressions to this exact text."""
    trace = reference["headers"]["kafka_exception-stacktrace"]
    assert len(trace) == 8059
    assert trace.count("\nCaused by: ") == 4
    assert trace.rstrip().endswith("... 43 more")
    assert "UID_ORIGIN_TRACKER_DATA_NOT_FOUND" in trace
