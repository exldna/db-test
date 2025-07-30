"""
Infrastructure-specific decorators, providing cross-cutting concerns like
retry logic for network operations.
"""

import logging

import httpx
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)

logger = logging.getLogger(__name__)

# --- Constants for Retry Logic ---
# In the future, this could be moved to settings.toml
_RETRY_ATTEMPTS = 3
_RETRY_MIN_WAIT_SECONDS = 1
_RETRY_MAX_WAIT_SECONDS = 10


def _log_before_retry(retry_state):
    """Log the retry attempt with details about the exception and wait time."""
    exception = retry_state.outcome.exception()
    next_attempt_in = retry_state.next_action.sleep
    logger.warning(
        f"Retrying {retry_state.fn.__name__} in {next_attempt_in:.2f}s due to "
        f"{type(exception).__name__} (attempt {retry_state.attempt_number})..."
    )


# A pre-configured decorator for async network operations
retry_on_network_error = retry(
    stop=stop_after_attempt(_RETRY_ATTEMPTS),
    wait=wait_exponential(
        multiplier=1,
        min=_RETRY_MIN_WAIT_SECONDS,
        max=_RETRY_MAX_WAIT_SECONDS,
    ),
    retry=retry_if_exception_type(
        (httpx.ConnectError, httpx.TimeoutException, httpx.HTTPStatusError)
    ),
    before_sleep=_log_before_retry,
)
