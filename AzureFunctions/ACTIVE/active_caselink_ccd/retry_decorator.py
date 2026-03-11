import time
import random
import logging
from functools import wraps

logger = logging.getLogger(__name__)


def retry_on_result(
    max_retries: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    jitter: bool = True,
    retry_on=None,
):
    """
    Retry decorator with exponential backoff for sync functions intended
    for use with asyncio.to_thread. Retries only when retry_on
    predicate returns True — exceptions are not caught.

    Args:
        max_retries:      Number of retry attempts after the first failure.
        base_delay:       Initial delay in seconds (doubles each attempt).
        max_delay:        Upper cap on delay in seconds.
        jitter:           Randomise delay to 50-100% of computed value.
        retry_on:         Optional callable(result) -> bool. If provided, a
                          return value for which this returns True is treated as
                          a retryable failure (the result is returned as-is
                          after all attempts are exhausted).

    Usage:
        @retry_on_result(max_retries=3, retry_on=lambda r: r.get("Status") == "ERROR")
        def my_blocking_call():
            ...

        result = await asyncio.to_thread(my_blocking_call)
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_result = None

            for attempt in range(max_retries + 1):
                result = func(*args, **kwargs)

                if retry_on is not None and retry_on(result):
                    last_result = result

                    if attempt == max_retries:
                        logger.error(
                            f"{func.__name__} returned a retryable result after "
                            f"{max_retries + 1} attempts. Returning last result."
                        )
                        return last_result

                    delay = min(base_delay * (2 ** attempt), max_delay)
                    if jitter:
                        delay *= 0.5 + random.random() * 0.5

                    logger.warning(
                        f"{func.__name__} returned a retryable result "
                        f"(attempt {attempt + 1}/{max_retries + 1}). "
                        f"Retrying in {delay:.2f}s..."
                    )
                    time.sleep(delay)
                    continue

                return result

            return last_result

        return wrapper
    return decorator
