from app.redis_client import r
from app.job_queue.job_model import Job
from app.job_queue.producer import QUEUE_KEY


def process_job(job_data: dict):
    print(f"Processing job: {job_data['job_id']} with task type: {job_data['task_type']}")


def run_worker():
    print("Worker started, waiting for jobs...")
    while True:
        job_data = r.blpop(QUEUE_KEY, timeout=0)
        if job_data:
            job_json = job_data[1]
            job = Job.model_validate_json(job_json)
            process_job(job.model_dump())


if __name__ == "__main__":
    run_worker()