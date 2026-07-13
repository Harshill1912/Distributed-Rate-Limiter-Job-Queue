import random
from locust import HttpUser, task, between

class RateLimiterUser(HttpUser):
    # Simulate a delay between 10ms and 100ms to generate high concurrency
    wait_time = between(0.01, 0.1)

    @task
    def check_rate_limit(self):
        # Pick from a pool of 100 users to test rate limiting and allowance states
        user_id = f"user_{random.randint(1, 100)}"
        # We allow 429 status code as a normal behavior for rate limiting tests
        with self.client.get(f"/check/rate_limit/{user_id}", catch_response=True) as response:
            if response.status_code in [200, 429]:
                response.success()
            else:
                response.failure(f"Unexpected status code: {response.status_code}")
