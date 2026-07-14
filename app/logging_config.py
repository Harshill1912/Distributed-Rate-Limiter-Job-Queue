"""Central logging setup — call setup_logging() once at each entrypoint."""
import logging
import os
import sys


def setup_logging(level: str | int | None = None) -> None:
    level = level or os.getenv("LOG_LEVEL", "INFO")
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)-7s %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stdout,
    )
