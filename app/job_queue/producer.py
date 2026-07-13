import uuid
from app.redis_client import r
from app.job_queue.job_model import Job
from app.job_queue.idempotency import is_duplicate, mark_processed

QUEUE_KEY = "job_queue"


def submit_job(task_type: str, payload: dict) -> str:
    job_id = str(uuid.uuid4())

    # Check for duplicate at submission time, not processing time
    if is_duplicate(job_id):
        print(f"⚠️ Duplicate submission detected for job {job_id} — ignoring")
        return job_id

    job = Job(job_id=job_id, task_type=task_type, payload=payload)
    r.rpush(QUEUE_KEY, job.model_dump_json())

    # Mark as "seen" immediately at submission
    mark_processed(job_id)
    return job_id