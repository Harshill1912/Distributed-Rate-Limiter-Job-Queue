"""Shared Redis client. Host/port come from the environment so the same code
runs locally (localhost) and in Docker (service name 'redis')."""
import os
import logging

import redis

logger = logging.getLogger(__name__)

r = redis.Redis(
    host=os.getenv("REDIS_HOST", "localhost"),
    port=int(os.getenv("REDIS_PORT", "6379")),
    db=int(os.getenv("REDIS_DB", "0")),
    decode_responses=True,
)


def test_connection() -> bool:
    try:
        r.ping()
        logger.info("Connected to Redis at %s:%s", r.connection_pool.connection_kwargs.get("host"),
                    r.connection_pool.connection_kwargs.get("port"))
        return True
    except redis.ConnectionError:
        logger.error("Failed to connect to Redis")
        return False


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    test_connection()
