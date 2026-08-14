import logging
import os
import structlog
import sys


def _resolve_level() -> int:
    """Log level from LOG_LEVEL, defaulting to INFO.

    This was hardcoded to INFO with no override, so raising verbosity to debug
    a production issue meant editing code (F21).
    """
    raw = os.environ.get("LOG_LEVEL", "INFO").strip().upper()
    return getattr(logging, raw, logging.INFO)


def setup_logging():
    # Only configure once
    if structlog.is_configured():
        return

    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=_resolve_level(),
    )
    
    structlog.configure(
        processors=[
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer()
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )
    
def get_logger(name=__name__):
    if not structlog.is_configured():
        setup_logging()
    return structlog.get_logger(name)
