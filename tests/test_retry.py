import logging
import pytest
from tenacity import retry, stop_after_attempt, wait_fixed, RetryError

def _log_retry(retry_state) -> None:
    exc = retry_state.outcome.exception()
    attempt = retry_state.attempt_number
    func_name = retry_state.fn.__name__ if hasattr(retry_state, "fn") else "operation"
    logging.warning(
        f"Retry {attempt} for {func_name} due to {type(exc).__name__}" if exc else f"Retry {attempt} for {func_name}"
    )


def test_retry_logs_success(caplog):
    attempts = {"n": 0}

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(0), before_sleep=_log_retry, reraise=True)
    def flaky():
        attempts["n"] += 1
        if attempts["n"] < 3:
            raise ValueError("temp")
        return "ok"

    with caplog.at_level(logging.WARNING):
        assert flaky() == "ok"

    retry_logs = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(retry_logs) == 2
    assert all("Retry" in r.getMessage() for r in retry_logs)


def test_retry_final_failure(caplog):
    attempts = {"n": 0}

    @retry(stop=stop_after_attempt(2), wait=wait_fixed(0), before_sleep=_log_retry, reraise=True)
    def always_fail():
        attempts["n"] += 1
        raise ValueError("perm")

    with caplog.at_level(logging.ERROR):
        with pytest.raises(ValueError):
            try:
                always_fail()
            except ValueError:
                logging.error("Operation failed after retries")
                raise

    error_logs = [r for r in caplog.records if r.levelno == logging.ERROR]
    assert any("Operation failed after retries" in r.getMessage() for r in error_logs)
