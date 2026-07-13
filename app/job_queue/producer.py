import uuid
import hashlib
import json
from app.redis_client import r
from app.job_queue.job_model import Job
from app.job_queue.idempotency import is_duplicate

QUEUE_KEY = "job_queue"


def submit_job(task_type: str, payload: dict) -> str:
    # Generate a unique hash based on task_type and payload contents
    payload_str = json.dumps(payload, sort_keys=True)
    idempotency_key = hashlib.md5(f"{task_type}:{payload_str}".encode('utf-8')).hexdigest()

    # Check for duplicate submission using the idempotency signature
    if is_duplicate(idempotency_key):
        raise ValueError(f"Duplicate submission detected for task '{task_type}' with payload '{payload_str}' (Idempotency Key: {idempotency_key})")

    job_id = str(uuid.uuid4())
    job = Job(job_id=job_id, task_type=task_type, payload=payload)
    r.rpush(QUEUE_KEY, job.model_dump_json())

    return job_id