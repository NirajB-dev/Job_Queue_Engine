from __future__ import annotations

import logging
import signal
import time

import redis as redis_lib

from core.queue import RedisQueue

logger = logging.getLogger(__name__)

POLL_INTERVAL = 1  # seconds


class Scheduler:
    def __init__(self, redis_client: redis_lib.Redis) -> None:
        self._queue = RedisQueue(redis_client)
        self._running = True

    def _tick(self) -> int:
        """Promote all due delayed jobs; returns count promoted."""
        return self._queue.promote_delayed()

    def run(self) -> None:
        signal.signal(signal.SIGTERM, self._handle_sigterm)
        logger.info("Scheduler started (poll interval %ds)", POLL_INTERVAL)
        while self._running:
            try:
                promoted = self._tick()
                if promoted:
                    logger.info("Promoted %d delayed job(s) to live queues", promoted)
            except Exception:
                logger.exception("Error during delayed-job promotion")
            time.sleep(POLL_INTERVAL)
        logger.info("Scheduler stopped")

    def _handle_sigterm(self, sig, frame) -> None:
        logger.info("SIGTERM received — stopping scheduler")
        self._running = False


def main() -> None:
    from core.config import settings

    logging.basicConfig(level=logging.INFO)
    redis_client = redis_lib.from_url(settings.effective_redis_url)
    Scheduler(redis_client).run()


if __name__ == "__main__":
    main()
