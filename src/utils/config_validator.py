import os
import sys
from pathlib import Path
import logging

from src.utils.env import get_bool_env

logger = logging.getLogger(__name__)

def _collect_llm_key_errors() -> list:
    """Return validation errors for whichever LLM provider get_llm() will
    actually select. get_llm() picks a provider via USE_HF /
    MOCK_LLM_WITH_MISTRAL and never reads OPENAI_API_KEY, so hard-failing on
    that unconditionally blocked startup on every provider except a literal
    OpenAI endpoint (1.3). Split out as a pure function so it's testable
    without the pytest-skip in validate_config() below getting in the way.
    """
    errors = []
    use_hf = get_bool_env("USE_HF", False)
    use_mistral = get_bool_env("MOCK_LLM_WITH_MISTRAL", False)
    if use_hf:
        if not os.environ.get("HF_TOKEN"):
            errors.append("USE_HF=true requires HF_TOKEN to be set.")
    elif use_mistral:
        if not os.environ.get("LLM_API_KEY_COMPLEX"):
            errors.append("MOCK_LLM_WITH_MISTRAL=true requires LLM_API_KEY_COMPLEX to be set.")
    else:
        if not os.environ.get("LLM_API_KEY_COMPLEX"):
            errors.append("LLM_API_KEY_COMPLEX must be set for the OpenAI-compatible provider.")
    return errors


def validate_config():
    """Fail-fast startup check to ensure required configurations and permissions are valid."""
    errors = []

    # 1. The API key the *selected* LLM provider actually reads is set.
    if "pytest" not in sys.modules:
        errors.extend(_collect_llm_key_errors())

    # 2. KAFKA_CONSUMER_BROKERS list is non-empty
    kafka_brokers_env = os.environ.get("KAFKA_CONSUMER_BROKERS", "localhost:9092")
    brokers = [b.strip() for b in kafka_brokers_env.split(",") if b.strip()]
    if not brokers:
        errors.append("KAFKA_CONSUMER_BROKERS must contain at least one valid broker.")

    # 3. PACKET_CRM_API_KEY is explicitly set in prod
    api_key = os.environ.get("PACKET_CRM_API_KEY")
    env = os.environ.get("ENV", "dev").lower()
    if env == "prod":
        if not api_key or api_key == "dev-secret-key":
            errors.append("PACKET_CRM_API_KEY must be explicitly set to a secure value in production (cannot be empty or 'dev-secret-key').")

    # 4. Storage and SQLite checkpointer paths are writable
    try:
        from src.storage.factory import get_casebook_storage
        storage = get_casebook_storage()
        if hasattr(storage, "base_dir"):
            storage_dir = Path(storage.base_dir)
            os.makedirs(storage_dir, exist_ok=True)
            if not os.access(storage_dir, os.W_OK):
                errors.append(f"CasebookStorage base_dir ({storage_dir}) is not writable.")
    except Exception as e:
        errors.append(f"Failed to validate CasebookStorage path: {e}")

    try:
        checkpoints_dir = Path(__file__).resolve().parent.parent.parent / "local_checkpoints"
        os.makedirs(checkpoints_dir, exist_ok=True)
        if not os.access(checkpoints_dir, os.W_OK):
            errors.append(f"SQLite checkpoints directory ({checkpoints_dir}) is not writable.")
    except Exception as e:
        errors.append(f"Failed to validate checkpointer path: {e}")

    if errors:
        for err in errors:
            logger.error(f"Startup Validation Error: {err}")
            print(f"Startup Validation Error: {err}", file=sys.stderr)
        sys.exit(1)

    logger.info("Configuration validation passed.")