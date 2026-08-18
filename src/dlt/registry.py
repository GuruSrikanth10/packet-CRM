"""Phase 2 of DLT_PLAN.md -- the BusinessException code registry.

Maps an enumerated error code (`UID_ORIGIN_TRACKER_DATA_NOT_FOUND`) to the
one-line description the organisation publishes for it.

**A seed, not an answer.** One line is enough to anchor and classify; it is not
enough to recommend. Phase 8's prompt treats a description as context and is
explicitly forbidden from inventing detail beyond it -- with no source and no
database access, a per-packet cause would be invention (DLT_PLAN.md 9.1).

Every failure mode here is a *miss*, never an exception: a missing file, an
unreadable file, an unparseable row, an unknown code. A registry problem must
degrade the confidence of a finding, not cost us the message. Risk R7 in the
plan is that the file arrives in an unexpected format -- which is why the whole
loader is isolated behind `lookup()`.
"""
import csv
import os
import threading
from pathlib import Path
from typing import Optional

from src.utils.logging_config import get_logger
from src.utils.paths import REPO_ROOT

logger = get_logger(__name__)

DEFAULT_REGISTRY_FILENAME = "business_errors.csv"

#: Accepted spellings for the code column, lower-cased and stripped.
_CODE_COLUMNS = ("code", "reason_code", "reasoncode", "error_code", "errorcode",
                 "business_code", "businesscode", "key")
#: Accepted spellings for the description column.
_DESC_COLUMNS = ("description", "desc", "message", "reason", "text", "detail",
                 "error_description")

_cache: dict = {}
_cache_lock = threading.Lock()


def registry_path() -> Path:
    """Where the registry lives. Relative values resolve against the repo root."""
    raw = os.environ.get("DLT_REGISTRY_PATH", "").strip() or DEFAULT_REGISTRY_FILENAME
    path = Path(raw)
    return path if path.is_absolute() else REPO_ROOT / path


def _pick_column(fieldnames, candidates) -> Optional[str]:
    normalised = {(name or "").strip().lower(): name for name in (fieldnames or [])}
    for candidate in candidates:
        if candidate in normalised:
            return normalised[candidate]
    return None


def _parse(path: Path) -> dict:
    """Read the CSV into {code: description}.

    Tries named columns first, then falls back to positional (first column is
    the code, second the description), skipping a header row if the first cell
    looks like one. The format is operator-supplied and not yet seen, so the
    reader is deliberately forgiving.
    """
    entries = {}
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        sample = handle.read(8192)
        handle.seek(0)
        if not sample.strip():
            return entries

        reader = csv.DictReader(handle)
        code_col = _pick_column(reader.fieldnames, _CODE_COLUMNS)
        desc_col = _pick_column(reader.fieldnames, _DESC_COLUMNS)

        if code_col:
            for row in reader:
                code = (row.get(code_col) or "").strip()
                if not code:
                    continue
                description = (row.get(desc_col) or "").strip() if desc_col else ""
                entries[code] = description
            return entries

        # No recognisable header -- treat it as positional data.
        handle.seek(0)
        for index, row in enumerate(csv.reader(handle)):
            if not row:
                continue
            code = (row[0] or "").strip()
            if not code:
                continue
            if index == 0 and code.lower() in _CODE_COLUMNS:
                continue
            entries[code] = (row[1] or "").strip() if len(row) > 1 else ""
    return entries


def load_registry(path: Optional[Path] = None) -> dict:
    """Return {code: description}, cached on the file's identity and mtime.

    Keying the cache on (path, mtime, size) means an operator can drop in an
    updated registry without restarting the consumers, while the steady state
    costs one `stat`.
    """
    target = Path(path) if path else registry_path()

    try:
        stat = target.stat()
        key = (str(target), stat.st_mtime_ns, stat.st_size)
    except OSError:
        logger.warning("BusinessException registry not readable; every lookup "
                       "will miss", path=str(target))
        return {}

    with _cache_lock:
        cached = _cache.get(key)
        if cached is not None:
            return cached

    try:
        entries = _parse(target)
    except (OSError, csv.Error, UnicodeDecodeError, IndexError) as exc:
        logger.warning("BusinessException registry could not be parsed; every "
                       "lookup will miss", path=str(target), error=str(exc))
        entries = {}
    else:
        logger.info("Loaded BusinessException registry",
                    path=str(target), codes=len(entries))

    with _cache_lock:
        _cache[key] = entries
    return entries


def lookup(code: Optional[str], path: Optional[Path] = None) -> Optional[str]:
    """Description for a code, or None.

    Case-exact after trimming: these are enumerated constants, and quietly
    matching `uid_origin_tracker_data_not_found` against a differently-cased
    entry would hide a real registry mismatch.
    """
    if not code:
        return None
    return load_registry(path).get(str(code).strip())


def clear_cache() -> None:
    """Drop the cached registry. For tests and for operator CLIs."""
    with _cache_lock:
        _cache.clear()
