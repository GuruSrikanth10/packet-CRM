"""Phase 7 of DLT_PLAN.md -- per-fingerprint group records.

A *group* is the durable record for one failure mode: how often it has been
seen, when it started, which cases belong to it, and the recommendation (if
any) an earlier investigation produced for it.

At ~2,000 messages/day with tens of distinct fingerprints, the group is what
turns a per-message cost into a per-*bug* cost. It is also the unit an operator
actually wants to read: "this bug has hit 431 packets since Tuesday" is a more
useful sentence than 431 individual casebooks.

`members` is capped. An uncapped list on a fingerprint seeing 400 hits/day
would grow without bound; `occurrence_count` keeps counting past the cap.
"""
import json
import os
import threading
import time
from typing import Optional

from filelock import FileLock

from src.dlt.case_storage import get_group_storage
from src.utils.logging_config import get_logger
from src.utils.paths import LOCAL_CHECKPOINTS_DIR

logger = get_logger(__name__)

DEFAULT_MEMBER_CAP = 200

#: Recommendation lifecycle. Nothing writes `final` in v1 -- there is no
#: review mechanism yet, so every reused recommendation stays explicitly
#: marked unreviewed (DLT_PLAN.md section 2).
STATE_NONE = "none"
STATE_DRAFT = "draft"
STATE_FINAL = "final"

_lock = threading.Lock()


def member_cap() -> int:
    try:
        return max(1, int(os.environ.get("DLT_GROUP_MEMBER_CAP",
                                         str(DEFAULT_MEMBER_CAP))))
    except (ValueError, TypeError):
        return DEFAULT_MEMBER_CAP


def _file_lock(fingerprint: str) -> FileLock:
    """Cross-process guard so two workers cannot lose an increment.

    The in-process lock is not enough: the DLT analysis role is meant to scale
    out to several pods, and on a shared filesystem they contend for the same
    group file. Mirrors how `pending_rules.jsonl` is already guarded.
    """
    lock_dir = LOCAL_CHECKPOINTS_DIR / "dlt_group_locks"
    lock_dir.mkdir(parents=True, exist_ok=True)
    return FileLock(str(lock_dir / f"{fingerprint}.lock"), timeout=10)


def _blank(fingerprint: str) -> dict:
    return {
        "fingerprint": fingerprint,
        "signature": "",
        "failure_class": "U",
        "business_code": None,
        "first_seen": None,
        "last_seen": None,
        "occurrence_count": 0,
        "members": [],
        "recommendation": None,
        "recommendation_state": STATE_NONE,
        "corroboration_history": {},
    }


def load_group(fingerprint: str) -> Optional[dict]:
    """Read a group record, or None when the fingerprint is novel."""
    if not fingerprint:
        return None
    try:
        return get_group_storage().load(fingerprint, filename="group.json")
    except Exception as e:
        logger.warning("Could not load DLT group; treating as novel",
                       fingerprint=fingerprint[:16], error=f"{type(e).__name__}: {e}")
        return None


def save_group(group: dict) -> None:
    get_group_storage().save(group["fingerprint"], group, filename="group.json")


def record_occurrence(fingerprint: str,
                      case_id: str,
                      signature: str = "",
                      failure_class: str = "U",
                      business_code: Optional[str] = None,
                      corroboration: Optional[str] = None) -> dict:
    """Register one case against its fingerprint and return the group.

    Idempotent per case: a redelivered case that is already a member does not
    double-count. Without that, a redrive would inflate every occurrence count
    and make the cost model look better than it is.
    """
    now = time.time()
    with _lock, _file_lock(fingerprint):
        group = load_group(fingerprint) or _blank(fingerprint)

        already_member = case_id in group.get("members", [])
        if not already_member:
            group["occurrence_count"] = int(group.get("occurrence_count", 0)) + 1
            members = list(group.get("members", []))
            members.append(case_id)
            group["members"] = members[-member_cap():]

        group["signature"] = signature or group.get("signature") or ""
        group["failure_class"] = failure_class or group.get("failure_class") or "U"
        group["business_code"] = business_code or group.get("business_code")
        group["first_seen"] = group.get("first_seen") or now
        group["last_seen"] = now

        if corroboration and not already_member:
            history = dict(group.get("corroboration_history") or {})
            history[corroboration] = int(history.get(corroboration, 0)) + 1
            group["corroboration_history"] = history

        save_group(group)
        return group


def attach_recommendation(fingerprint: str, recommendation: dict,
                          state: str = STATE_DRAFT) -> dict:
    """Record the recommendation an investigation produced for this group."""
    with _lock, _file_lock(fingerprint):
        group = load_group(fingerprint) or _blank(fingerprint)
        group["recommendation"] = recommendation
        group["recommendation_state"] = state
        save_group(group)
        return group


def has_usable_recommendation(group: Optional[dict]) -> bool:
    return bool(group
                and group.get("recommendation")
                and group.get("recommendation_state") in (STATE_DRAFT, STATE_FINAL))


def list_groups() -> list:
    """Every group record, newest activity first. For the operator CLI."""
    storage = get_group_storage()
    groups = []
    try:
        identifiers = storage.list_events()
    except Exception as e:
        logger.warning("Could not list DLT groups", error=f"{type(e).__name__}: {e}")
        return []

    for fingerprint in identifiers:
        try:
            group = storage.load(fingerprint, filename="group.json")
        except Exception:
            continue
        if group:
            groups.append(group)
    return sorted(groups, key=lambda g: g.get("last_seen") or 0, reverse=True)


def as_json(group: dict) -> str:
    return json.dumps(group, indent=2, ensure_ascii=False, sort_keys=True)
