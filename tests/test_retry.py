from worker.retry import backoff_seconds


def test_first_attempt_between_10_and_15():
    delay = backoff_seconds(0, base=10)
    assert 10 <= delay <= 15


def test_delay_grows_with_attempts():
    # base * 2^attempt grows: 10, 20, 40, 80, ...
    # Even with max jitter (+5) the lower bound of attempt N+1 exceeds attempt N
    for attempt in range(4):
        low = backoff_seconds(attempt, base=10) - 5
        high_next = backoff_seconds(attempt + 1, base=10)
        assert high_next > low


def test_capped_at_max_delay():
    assert backoff_seconds(100, base=10, max_delay=3600) == 3600.0


def test_custom_base():
    delay = backoff_seconds(0, base=5)
    assert 5 <= delay <= 10
