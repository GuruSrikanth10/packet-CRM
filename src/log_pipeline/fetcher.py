"""
Stage 1 -- Elasticsearch Fetch Layer.

Improvements over the old fetch_elastic_logs:
  - Source-filtered to only @timestamp, level, message (cuts ES payload).
  - search_after with _seq_no tiebreaker for stable pagination.
  - Optional must_not filter driven by catalog boilerplate phrases.
  - Returns structured dicts, not raw formatted strings.
"""
import os
from typing import Optional

from src.log_pipeline.catalog import TemplateCatalog


def fetch_logs(event_id: str, catalog: Optional[TemplateCatalog] = None) -> list[dict]:
    """Fetch logs from Elasticsearch for a given event_id.

    Returns a list of dicts with keys: timestamp, level, message, app_name.
    Sorted by @timestamp ASC with _seq_no tiebreaker.
    """
    print(f"\n[FETCHER] Fetching logs for: {event_id}")

    es_host = os.environ.get("ES_HOST")
    es_user = os.environ.get("ES_USERNAME")
    es_pass = os.environ.get("ES_PASSWORD")
    index_pattern = os.environ.get("ES_INDEX_PATTERN", "logs-*")

    if not es_host:
        print("[FETCHER] ES_HOST not set. Returning mock logs.")
        return [
            {"timestamp": "MOCK", "level": "ERROR", "message": f"[MOCK] connection timeout for {event_id}", "app_name": "mock-service"}
        ]

    from elasticsearch import Elasticsearch

    auth_args = {}
    if es_user and es_pass:
        auth_args["basic_auth"] = (es_user, es_pass)

    es_client = Elasticsearch(es_host, verify_certs=False, **auth_args)

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

    print(f"[FETCHER] Fetched {len(logs)} logs from Elasticsearch.")
    return logs
