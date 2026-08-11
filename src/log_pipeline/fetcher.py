"""
Stage 1 -- Elasticsearch Fetch Layer.

Improvements over the old fetch_elastic_logs:
  - Source-filtered to only @timestamp, level, message (cuts ES payload).
  - search_after with _seq_no tiebreaker for stable pagination.
  - Optional must_not filter driven by catalog boilerplate phrases.
  - Returns structured dicts, not raw formatted strings.
"""
import os
import json
from typing import Optional

from src.log_pipeline.catalog import TemplateCatalog
from src.utils.env import get_bool_env
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

# Hard cap on total documents pulled per event_id -- without this, a noisy
# event can pull an unbounded number of rows into memory (1.10).
LOG_MAX_DOCUMENTS = int(os.environ.get("LOG_MAX_DOCUMENTS", "50000"))


def fetch_logs(event_id: str, catalog: Optional[TemplateCatalog] = None) -> list[dict]:
    """Fetch logs from Elasticsearch for a given event_id.

    Returns a list of dicts with keys: timestamp, level, message, app_name.
    Sorted by @timestamp ASC with _seq_no tiebreaker.
    """
    log = logger.bind(event_id=event_id)
    log.info("[FETCHER] Fetching logs")

    # --- Testing/Mock Mode: Load logs from a local CSV file ---
    mock_file = os.environ.get("ES_MOCK_FILE")
    if mock_file:
        log.info(f"[FETCHER] ES_MOCK_FILE set. Loading logs from {mock_file}.")
        logs = []
        try:
            with open(mock_file, 'r', encoding='utf-8', errors='replace') as f:
                # Skip the header line
                next(f, None)
                for line in f:
                    # Extract the JSON object from the line
                    start_idx = line.find('{')
                    end_idx = line.rfind('}')
                    if start_idx != -1 and end_idx != -1 and end_idx > start_idx:
                        log_json_str = line[start_idx:end_idx+1]
                        
                        # Kibana CSV exports often escape quotes as ""
                        if '""' in log_json_str:
                            log_json_str = log_json_str.replace('""', '"')
                            
                        # Filter by event_id if provided (skips logs not matching the event)
                        if event_id and event_id not in log_json_str:
                            continue
                            
                        try:
                            source = json.loads(log_json_str)
                            logs.append({
                                "timestamp": source.get("@timestamp", "UNKNOWN_TIME"),
                                "level": source.get("level", "INFO"),
                                "message": source.get("message", str(source)),
                                "app_name": source.get("application_name", "unknown-service"),
                            })
                        except json.JSONDecodeError as e:
                            log.warning(f"[FETCHER] Skipped line due to JSON decode error: {e}")
                            log.warning(f"[FETCHER] Problematic JSON: {log_json_str[:150]}...")
                            continue
            # Sort by timestamp to mimic Elasticsearch's ascending order
            logs.sort(key=lambda x: x["timestamp"])
            log.info(f"[FETCHER] Loaded {len(logs)} logs from file.")
            return logs
        except Exception as e:
            log.error(f"[FETCHER] Error loading mock file: {e}")
            return []

    es_host = os.environ.get("ES_HOST")
    es_user = os.environ.get("ES_USERNAME")
    es_pass = os.environ.get("ES_PASSWORD")
    index_pattern = os.environ.get("ES_INDEX_PATTERN", "logs-*")

    if not es_host:
        log.info("[FETCHER] ES_HOST not set. Returning mock logs.")
        return [
            {"timestamp": "MOCK", "level": "ERROR", "message": f"[MOCK] connection timeout for {event_id}", "app_name": "mock-service"}
        ]

    from elasticsearch import Elasticsearch

    auth_args = {}
    if es_user and es_pass:
        auth_args["basic_auth"] = (es_user, es_pass)

    # Certificate verification defaults ON -- it was previously disabled
    # unconditionally with no override, and no request timeout was set (1.9).
    es_verify_certs = get_bool_env("ES_VERIFY_CERTS", True)
    es_request_timeout = float(os.environ.get("ES_REQUEST_TIMEOUT_SECONDS", "30"))
    es_client = Elasticsearch(
        es_host,
        verify_certs=es_verify_certs,
        request_timeout=es_request_timeout,
        **auth_args,
    )

    # Build query --------------------------------------------------------
    must_clauses = [
        {"query_string": {"query": f'"{event_id}"'}}
    ]
    filter_clauses = [
        {"term": {"application_name.keyword": "enu-biometric"}}
    ]

    # Stage 1 enhancement: must_not from catalog boilerplate
    must_not_clauses = []
    if catalog:
        for phrase in catalog.get_boilerplate_phrases():
            must_not_clauses.append({"match_phrase": {"message": phrase}})

    query = {
        "bool": {
            "must": must_clauses,
            "filter": filter_clauses,
        }
    }
    if must_not_clauses:
        query["bool"]["must_not"] = must_not_clauses

    # Source-filter: only pull the fields we need
    source_fields = ["@timestamp", "level", "message", "application_name"]

    # Stable sort with _id tiebreaker (broadly compatible across ES versions)
    sort_criteria = [
        {"@timestamp": {"order": "asc"}},
        {"_id": {"order": "asc"}},
    ]

    # Paginate with search_after ---------------------------------------
    logs = []
    search_after_values = None
    page_size = 500

    while True:
        search_kwargs = {
            "index": index_pattern,
            "size": page_size,
            "sort": sort_criteria,
            "query": query,
            "_source": source_fields,
            "seq_no_primary_term": True,
        }
        if search_after_values:
            search_kwargs["search_after"] = search_after_values

        response = es_client.search(**search_kwargs)
        hits = response.get("hits", {}).get("hits", [])

        if not hits:
            break

        for hit in hits:
            source = hit["_source"]
            logs.append({
                "timestamp": source.get("@timestamp", "UNKNOWN_TIME"),
                "level": source.get("level", "INFO"),
                "message": source.get("message", str(source)),
                "app_name": source.get("application_name", "unknown-service"),
            })
            search_after_values = hit["sort"]

        if len(logs) >= LOG_MAX_DOCUMENTS:
            log.warning(f"[FETCHER] Hit LOG_MAX_DOCUMENTS cap ({LOG_MAX_DOCUMENTS}); truncating results.")
            logs = logs[:LOG_MAX_DOCUMENTS]
            break

        # A short page means this was the last one -- no point issuing one
        # more search_after request just to receive an empty page (1.10).
        if len(hits) < page_size:
            break

    log.info(f"[FETCHER] Fetched {len(logs)} logs from Elasticsearch.")
    return logs
