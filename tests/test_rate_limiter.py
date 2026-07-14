from fastapi.testclient import TestClient
from app.main import app
from app.redis_client import r

client = TestClient(app)

def test_rate_limiter_flow():
    user_id = "test_user_pytest"
    r.delete(f"rate_limiter:{user_id}")   # start from a full bucket

    # 1. First 5 requests should be allowed (MAX_TOKENS = 5)
    for _ in range(5):
        response = client.get(f"/check/rate_limit/{user_id}")
        assert response.status_code == 200
        assert response.json() == {"message": "Request allowed"}

    # 2. 6th request should be rate-limited (status code 429)
    response = client.get(f"/check/rate_limit/{user_id}")
    assert response.status_code == 429
    assert response.json()["detail"] == "Rate limit exceeded. Please try again later."
