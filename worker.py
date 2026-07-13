import random
from app.redis_client import r
from app.job_queue.job_model import Job
from app.job_queue.producer import QUEUE_KEY

MAX_RETRIES = 3
DEAD_LETTER_KEY = "dead_letter_queue"


def process_job(job_data: dict):
    print(f"Processing job: {job_data['job_id']} | task: {job_data['task_type']} | attempt: {job_data['retry_count'] + 1}")

    # Simulate random failure — fails 40% of the time
    if random.random() < 0.4:
        raise Exception(f"Simulated failure for job {job_data['job_id']}")

    print(f"✅ Job {job_data['job_id']} completed successfully")


def run_worker():
    print("Worker started, waiting for jobs...")
    while True:
        job_data = r.blpop(QUEUE_KEY, timeout=0)
        if job_data:
            job_json = job_data[1]
            job = Job.model_validate_json(job_json)

            try:
                process_job(job.model_dump())

            except Exception as e:
                print(f"❌ Job failed: {e}")
                job.retry_count += 1

                if job.retry_count < MAX_RETRIES:
                    print(f"🔄 Retrying job {job.job_id} (attempt {job.retry_count + 1} of {MAX_RETRIES})")
                    r.rpush(QUEUE_KEY, job.model_dump_json())
                else:
                    print(f" Job {job.job_id} exceeded max retries — moving to dead-letter queue")
                    job.status = "failed"
                    r.rpush(DEAD_LETTER_KEY, job.model_dump_json())


if __name__ == "__main__":
    run_worker()