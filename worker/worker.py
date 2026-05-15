import logging

logger = logging.getLogger(__name__)


def main() -> None:
    logger.info("Worker started")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
