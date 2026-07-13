import uuid
from app.redis_client import r
from app.job_queue.job_model import Job

QUEUE_KEY = "job_queue"

def submit_job(task_type: str, payload: dict):
    job_id = str(uuid.uuid4())
    job = Job(job_id=job_id, task_type=task_type, payload=payload)
    r.rpush(QUEUE_KEY, job.model_dump_json())
    return job_id