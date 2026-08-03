import pybreaker
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import requests
import urllib3.exceptions
from elasticsearch import ConnectionError as ESConnectionError
from sqlalchemy.exc import OperationalError

# Circuit breaker trips after 3 consecutive failures, resets after 60 seconds
db_breaker = pybreaker.CircuitBreaker(fail_max=3, reset_timeout=60)
es_breaker = pybreaker.CircuitBreaker(fail_max=3, reset_timeout=60)
llm_breaker = pybreaker.CircuitBreaker(fail_max=3, reset_timeout=60)

# Transient exceptions to retry
TRANSIENT_EXCEPTIONS = (
    requests.exceptions.RequestException,
    urllib3.exceptions.HTTPError,
    ESConnectionError,
    OperationalError,
    TimeoutError
)

def get_retry_decorator():
    return retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type(TRANSIENT_EXCEPTIONS)
    )

retry_transient = get_retry_decorator()
