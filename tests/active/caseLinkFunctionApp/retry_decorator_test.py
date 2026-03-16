import pytest
from unittest.mock import patch

from AzureFunctions.ACTIVE.active_caselink_ccd.retry_decorator import retry_on_result

SLEEP_PATH = "AzureFunctions.ACTIVE.active_caselink_ccd.retry_decorator.time.sleep"

RETRYABLE = lambda r: isinstance(r, dict) and r.get("Status") == "ERROR"


def make_results_fn(*results):
    """Return a function that yields successive return values."""
    values = list(results)

    def fn(*args, **kwargs):
        fn.call_count += 1
        return values.pop(0) if values else None

    fn.call_count = 0
    fn.__name__ = "fn"
    fn.__doc__ = None
    return fn


# ---------------------------------------------------------------------------
# Basic behaviour
# ---------------------------------------------------------------------------

def test_success_on_first_attempt_no_sleep():
    """Function returns a non-retryable result first try — no retries, no sleep."""
    fn = make_results_fn("ok")
    decorated = retry_on_result(max_retries=3, retry_on=RETRYABLE)(fn)

    with patch(SLEEP_PATH) as mock_sleep:
        result = decorated()

    assert result == "ok"
    assert fn.call_count == 1
    mock_sleep.assert_not_called()


def test_no_retry_when_retry_on_is_none():
    """With retry_on=None, an ERROR result is returned immediately without retrying."""
    fn = make_results_fn({"Status": "ERROR"})
    decorated = retry_on_result(max_retries=3, retry_on=None)(fn)

    with patch(SLEEP_PATH) as mock_sleep:
        result = decorated()

    assert result == {"Status": "ERROR"}
    assert fn.call_count == 1
    mock_sleep.assert_not_called()


def test_retries_on_error_result_then_succeeds():
    """Function returns ERROR twice then SUCCESS; called 3 times total."""
    fn = make_results_fn({"Status": "ERROR"}, {"Status": "ERROR"}, {"Status": "SUCCESS"})
    decorated = retry_on_result(max_retries=3, retry_on=RETRYABLE, base_delay=1.0, jitter=False)(fn)

    with patch(SLEEP_PATH):
        result = decorated()

    assert result == {"Status": "SUCCESS"}
    assert fn.call_count == 3


def test_returns_last_error_result_after_all_retries_exhausted():
    """After max_retries+1 attempts all returning ERROR, the last result is returned."""
    fn = make_results_fn(*[{"Status": "ERROR"} for _ in range(3)])
    decorated = retry_on_result(max_retries=2, retry_on=RETRYABLE, jitter=False)(fn)

    with patch(SLEEP_PATH):
        result = decorated()

    assert result["Status"] == "ERROR"
    assert fn.call_count == 3


def test_total_call_count_equals_max_retries_plus_one():
    """With max_retries=N, function is called N+1 times total."""
    fn = make_results_fn(*[{"Status": "ERROR"} for _ in range(5)])
    decorated = retry_on_result(max_retries=4, retry_on=RETRYABLE, jitter=False)(fn)

    with patch(SLEEP_PATH):
        decorated()

    assert fn.call_count == 5


def test_zero_retries_returns_error_result_immediately():
    """max_retries=0 means no retries; ERROR result returned after one attempt."""
    fn = make_results_fn({"Status": "ERROR"})
    decorated = retry_on_result(max_retries=0, retry_on=RETRYABLE)(fn)

    with patch(SLEEP_PATH) as mock_sleep:
        result = decorated()

    assert result["Status"] == "ERROR"
    assert fn.call_count == 1
    mock_sleep.assert_not_called()


def test_exceptions_are_not_caught():
    """Exceptions raised inside the wrapped function propagate immediately."""
    def fn():
        raise ValueError("boom")

    decorated = retry_on_result(max_retries=3, retry_on=RETRYABLE)(fn)

    with patch(SLEEP_PATH):
        with pytest.raises(ValueError, match="boom"):
            decorated()


# ---------------------------------------------------------------------------
# Sleep / delay behaviour
# ---------------------------------------------------------------------------

def test_no_sleep_after_final_failed_attempt():
    """sleep() is not called after the last failed attempt."""
    fn = make_results_fn(*[{"Status": "ERROR"}] * 3)
    decorated = retry_on_result(max_retries=2, retry_on=RETRYABLE, base_delay=1.0, jitter=False)(fn)

    with patch(SLEEP_PATH) as mock_sleep:
        decorated()

    # 3 attempts → sleep after attempt 0 and 1 only
    assert mock_sleep.call_count == 2


def test_exponential_backoff_delay_without_jitter():
    """Delay doubles each attempt: base, 2*base, 4*base..."""
    fn = make_results_fn(*[{"Status": "ERROR"}] * 4)
    decorated = retry_on_result(max_retries=3, retry_on=RETRYABLE, base_delay=1.0, max_delay=999, jitter=False)(fn)

    with patch(SLEEP_PATH) as mock_sleep:
        decorated()

    actual_delays = [c.args[0] for c in mock_sleep.call_args_list]
    assert actual_delays == [1.0, 2.0, 4.0]


def test_delay_capped_at_max_delay():
    """Delay never exceeds max_delay."""
    fn = make_results_fn(*[{"Status": "ERROR"}] * 6)
    decorated = retry_on_result(max_retries=5, retry_on=RETRYABLE, base_delay=10.0, max_delay=15.0, jitter=False)(fn)

    with patch(SLEEP_PATH) as mock_sleep:
        decorated()

    for c in mock_sleep.call_args_list:
        assert c.args[0] <= 15.0


def test_jitter_delay_within_expected_range():
    """With jitter=True, sleep delay is between 50% and 100% of computed value."""
    fn = make_results_fn({"Status": "ERROR"}, {"Status": "SUCCESS"})
    decorated = retry_on_result(max_retries=1, retry_on=RETRYABLE, base_delay=4.0, max_delay=999, jitter=True)(fn)

    with patch(SLEEP_PATH) as mock_sleep:
        decorated()

    actual_delay = mock_sleep.call_args_list[0].args[0]
    # base_delay * 2^0 = 4.0; jitter range: [2.0, 4.0]
    assert 2.0 <= actual_delay <= 4.0


# ---------------------------------------------------------------------------
# Argument forwarding & metadata
# ---------------------------------------------------------------------------

def test_passes_args_and_kwargs_to_wrapped_function():
    """Positional and keyword arguments are forwarded correctly."""
    received = {}

    def fn(*args, **kwargs):
        received["args"] = args
        received["kwargs"] = kwargs
        return "result"

    decorated = retry_on_result()(fn)

    with patch(SLEEP_PATH):
        result = decorated(1, 2, key="val")

    assert result == "result"
    assert received["args"] == (1, 2)
    assert received["kwargs"] == {"key": "val"}


def test_preserves_function_name_and_docstring():
    """@wraps correctly preserves __name__ and __doc__."""
    @retry_on_result()
    def my_func():
        """My docstring."""

    assert my_func.__name__ == "my_func"
    assert my_func.__doc__ == "My docstring."
