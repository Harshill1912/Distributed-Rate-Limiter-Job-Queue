import time

from pydantic import BaseModel, Field


class Job(BaseModel):
    job_id: str
    task_type: str
    payload: dict
    status: str = "pending"
    retry_count: int = 0
    # wall-clock time the job was enqueued; used to measure end-to-end latency.
    enqueued_at: float = Field(default_factory=time.time)
