import random


def backoff_seconds(attempt: int, base: int = 10, max_delay: int = 3600) -> float:
    """Exponential backoff with jitter: min(base * 2^attempt + U(0,5), max_delay)."""
    return min(base * (2**attempt) + random.uniform(0, 5), float(max_delay))
