import logging
import signal
import time

logger = logging.getLogger(__name__)

_running = True


def _handle_sigterm(sig, frame):
    global _running
    logger.info("SIGTERM received, shutting down")
    _running = False


def main() -> None:
    signal.signal(signal.SIGTERM, _handle_sigterm)
    logger.info("Watchdog started (stub — heartbeat monitor implemented in step 6)")
    while _running:
        time.sleep(5)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
