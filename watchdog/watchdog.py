from __future__ import annotations

import logging
import signal
import time

import psycopg2
import redis as redis_lib

from core.queue import RedisQueue
from core.repository import JobRepository

logger = logging.getLogger(__name__)

HEARTBEAT_THRESHOLD = 60   # seconds before a worker is considered stale
POLL_INTERVAL = 30          # seconds between watchdog scans


class Watchdog:
    def __init__(
        self,
        conn: psycopg2.extensions.connection,
        redis_client: redis_lib.Redis,
        threshold_seconds: int = HEARTBEAT_THRESHOLD,
        poll_interval: int = POLL_INTERVAL,
    ) -> None:
        self._repo = JobRepository(conn)
        self._queue = RedisQueue(redis_client)
        self._threshold = threshold_seconds
        self._poll_interval = poll_interval
        self._running = True

    def _tick(self) -> int:
        """Scan for stale workers, reclaim their running jobs, return count reclaimed."""
        stale_workers = self._queue.get_stale_workers(self._threshold)
        total_reclaimed = 0

        for worker_id in stale_workers:
            jobs = self._repo.claim_stale_jobs(worker_id)
            for job in jobs:
                # Re-enqueue immediately (run_at = now); scheduler is not involved
                self._queue.enqueue(job.id, job.priority, run_at=time.time())
                logger.warning(
                    "Reclaimed job %s (type=%s) from stale worker %s",
                    job.id,
                    job.type,
                    worker_id,
                )
            # Remove the heartbeat key so this worker is not reported again
            self._queue.deregister_worker(worker_id)
            if jobs:
                logger.warning(
                    "Reclaimed %d job(s) from stale worker %s",
                    len(jobs),
                    worker_id,
                )
            total_reclaimed += len(jobs)

        return total_reclaimed

    def run(self) -> None:
        signal.signal(signal.SIGTERM, self._handle_sigterm)
        logger.info(
            "Watchdog started (threshold=%ds, poll=%ds)",
            self._threshold,
            self._poll_interval,
        )
        while self._running:
            try:
                reclaimed = self._tick()
                if reclaimed:
                    logger.info("Watchdog reclaimed %d job(s) this tick", reclaimed)
            except Exception:
                logger.exception("Error during watchdog tick")
            time.sleep(self._poll_interval)
        logger.info("Watchdog stopped")

    def _handle_sigterm(self, sig, frame) -> None:
        logger.info("SIGTERM received — stopping watchdog")
        self._running = False


def main() -> None:
    from core.config import settings

    logging.basicConfig(level=logging.INFO)
    conn = psycopg2.connect(settings.database_url)
    redis_client = redis_lib.from_url(settings.effective_redis_url)
    Watchdog(conn, redis_client).run()


if __name__ == "__main__":
    main()
