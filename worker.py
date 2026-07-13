import sys
sys.stdout.reconfigure(encoding='utf-8')
sys.stderr.reconfigure(encoding='utf-8')

import random
from datetime import datetime
from app.redis_client import r
from app.job_queue.job_model import Job
from app.job_queue.producer import QUEUE_KEY
from app.db.database import SessionLocal
from app.db.models import JobRecord

MAX_RETRIES = 3
DEAD_LETTER_KEY = "dead_letter_queue"


def process_job(job_data: dict):
    print(f"Processing job: {job_data['job_id']} | task: {job_data['task_type']} | attempt: {job_data['retry_count'] + 1}")

    if random.random() < 0.4:
        raise Exception(f"Simulated failure for job {job_data['job_id']}")

    print(f"✅ Job {job_data['job_id']} completed successfully")


def log_job(job: Job, status: str):
    """
    Permanently log job outcome to PostgreSQL.
    """
    db = SessionLocal()
    try:
        # Check if record already exists (from a previous attempt)
        existing = db.query(JobRecord).filter(JobRecord.job_id == job.job_id).first()

        if existing:
            # Update existing record
            existing.status = status
            existing.retry_count = job.retry_count
            existing.completed_at = datetime.utcnow()
        else:
            # Create new record
            record = JobRecord(
                job_id=job.job_id,
                task_type=job.task_type,
                status=status,
                retry_count=job.retry_count,
                completed_at=datetime.utcnow() if status != "pending" else None
            )
            db.add(record)

        db.commit()
        print(f"📝 Job {job.job_id} logged to DB as '{status}'")

    except Exception as e:
        print(f"⚠️ DB logging failed: {e}")
        db.rollback()
    finally:
        db.close()


def run_worker():
    print("Worker started, waiting for jobs...")
    while True:
        job_data = r.blpop(QUEUE_KEY, timeout=0)
        if job_data:
            job_json = job_data[1]
            job = Job.model_validate_json(job_json)

            try:
                process_job(job.model_dump())
                log_job(job, "completed")

            except Exception as e:
                print(f"❌ Job failed: {e}")
                job.retry_count += 1

                if job.retry_count < MAX_RETRIES:
                    print(f"🔄 Retrying job {job.job_id} (attempt {job.retry_count + 1} of {MAX_RETRIES})")
                    log_job(job, "retrying")
                    r.rpush(QUEUE_KEY, job.model_dump_json())
                else:
                    print(f"💀 Job {job.job_id} exceeded max retries — moving to dead-letter queue")
                    job.status = "failed"
                    log_job(job, "failed")
                    r.rpush(DEAD_LETTER_KEY, job.model_dump_json())


if __name__ == "__main__":
    run_worker()