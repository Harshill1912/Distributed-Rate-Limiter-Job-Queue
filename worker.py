"""
Job queue worker with at-least-once delivery.

Reliability model:
  * Jobs are taken with a RELIABLE queue pattern — BLMOVE atomically moves a job
    from the main queue to a per-process "processing" list. The job is only
    removed from "processing" once it has been handled. If the worker crashes
    mid-job, the job survives in "processing" and is recovered on the next start
    (moved back to the queue). This gives at-least-once delivery instead of the
    job-loss you get with a plain BLPOP.
  * Failed jobs are retried with EXPONENTIAL BACKOFF via a Redis sorted set
    (score = ready-at timestamp), not re-queued instantly.
  * After MAX_RETRIES, jobs go to a dead-letter queue for inspection.
"""
import logging
import os
import random
import time
from datetime import datetime, timezone

import redis

from app.logging_config import setup_logging
from app.metrics import record_latency
from app.redis_client import r
from app.job_queue.job_model import Job
from app.job_queue.producer import QUEUE_KEY
from app.db.database import SessionLocal, init_db
from app.db.models import JobRecord

logger = logging.getLogger("worker")

PROCESSING_KEY = os.getenv("PROCESSING_KEY", "job_queue:processing")
RETRY_ZSET = os.getenv("RETRY_ZSET", "job_queue:retry")
DEAD_LETTER_KEY = os.getenv("DEAD_LETTER_KEY", "dead_letter_queue")
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
BASE_BACKOFF = float(os.getenv("BASE_BACKOFF_SECONDS", "2"))
MAX_BACKOFF = float(os.getenv("MAX_BACKOFF_SECONDS", "60"))
# Simulated failure rate so the retry / dead-letter paths are demonstrable.
FAIL_RATE = float(os.getenv("SIMULATED_FAIL_RATE", "0.3"))


def process_job(job_data: dict) -> None:
    logger.info("Processing job %s | task=%s | attempt=%s",
                job_data["job_id"], job_data["task_type"], job_data["retry_count"] + 1)
    if random.random() < FAIL_RATE:               # simulated unreliable work
        raise RuntimeError(f"Simulated failure for job {job_data['job_id']}")
    logger.info("Job %s completed", job_data["job_id"])


def log_job(job: Job, status: str) -> None:
    """Persist the job outcome to PostgreSQL (upsert by job_id)."""
    db = SessionLocal()
    try:
        record = db.query(JobRecord).filter(JobRecord.job_id == job.job_id).first()
        completed = datetime.now(timezone.utc) if status in ("completed", "failed") else None
        if record:
            record.status = status
            record.retry_count = job.retry_count
            record.completed_at = completed
        else:
            db.add(JobRecord(job_id=job.job_id, task_type=job.task_type, status=status,
                             retry_count=job.retry_count, completed_at=completed))
        db.commit()
        logger.info("Job %s logged as '%s'", job.job_id, status)
    except Exception as e:
        logger.warning("DB logging failed for %s: %s", job.job_id, e)
        db.rollback()
    finally:
        db.close()


def _backoff_seconds(retry_count: int) -> float:
    return min(MAX_BACKOFF, BASE_BACKOFF * (2 ** (retry_count - 1)))


def _recover_orphans() -> None:
    """Move any jobs left in the processing list (from a crashed worker) back
    onto the main queue so they are retried."""
    recovered = 0
    while r.lmove(PROCESSING_KEY, QUEUE_KEY, "LEFT", "RIGHT"):
        recovered += 1
    if recovered:
        logger.warning("Recovered %d orphaned in-flight job(s) from a previous run", recovered)


def _promote_due_retries() -> None:
    """Move retries whose backoff has elapsed back onto the main queue."""
    now = time.time()
    for raw in r.zrangebyscore(RETRY_ZSET, 0, now):
        if r.zrem(RETRY_ZSET, raw):     # only the winner re-queues (safe if many workers)
            r.rpush(QUEUE_KEY, raw)


def handle(raw: str) -> None:
    job = Job.model_validate_json(raw)
    try:
        t0 = time.perf_counter()
        process_job(job.model_dump())
        record_latency("job_process", (time.perf_counter() - t0) * 1000)
        record_latency("job_e2e", (time.time() - job.enqueued_at) * 1000)  # enqueue -> done
        log_job(job, "completed")
    except Exception as e:
        job.retry_count += 1
        if job.retry_count < MAX_RETRIES:
            delay = _backoff_seconds(job.retry_count)
            logger.warning("Job %s failed (%s) - retry %d/%d in %.0fs",
                           job.job_id, e, job.retry_count, MAX_RETRIES, delay)
            log_job(job, "retrying")
            r.zadd(RETRY_ZSET, {job.model_dump_json(): time.time() + delay})
        else:
            logger.error("Job %s exceeded %d retries - dead-lettering", job.job_id, MAX_RETRIES)
            job.status = "failed"
            log_job(job, "failed")
            r.rpush(DEAD_LETTER_KEY, job.model_dump_json())


def run_worker() -> None:
    setup_logging()
    init_db()
    logger.info("Worker started (max_retries=%d, base_backoff=%.0fs)", MAX_RETRIES, BASE_BACKOFF)
    _recover_orphans()
    while True:
        try:
            _promote_due_retries()
            # reliable pop: atomically move queue head -> processing tail.
            raw = r.blmove(QUEUE_KEY, PROCESSING_KEY, 5, "LEFT", "RIGHT")
            if raw is None:
                continue
            try:
                handle(raw)
            finally:
                # remove the in-flight copy; if we crash before here it is recovered.
                r.lrem(PROCESSING_KEY, 1, raw)
        except redis.exceptions.RedisError as e:
            logger.warning("Redis error: %s — retrying in 2s", e)
            time.sleep(2)


if __name__ == "__main__":
    run_worker()
