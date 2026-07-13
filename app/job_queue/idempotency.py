from app.redis_client import r

EXPIRY_SECONDS = 86400  # 24 hours


def is_duplicate(job_id: str) -> bool:
    """
    Atomically check-and-set the job_id.
    Returns True if duplicate (key already existed),
    Returns False if new job (key didn't exist, now set).
    """
    key = f"processed:{job_id}"
    # SET key "processed" EX 86400 NX
    # Returns None if key already existed (duplicate)
    # Returns True if key was newly set (not duplicate)
    result = r.set(key, "processed", ex=EXPIRY_SECONDS, nx=True)
    return result is None  # None means key existed = duplicate


def mark_processed(job_id: str) -> None:
    """
    No longer needed separately — is_duplicate() now handles
    both check AND mark atomically in one Redis operation.
    Keep this as a no-op for backward compatibility.
    """
    pass