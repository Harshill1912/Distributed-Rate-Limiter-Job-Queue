from fastapi import FastAPI, HTTPException
from app.rate_limiter.limiter import is_allowed 

app=FastAPI()

@app.get("/check/rate_limit/{user_id}")
def check_rate_limit(user_id: str):
    allowed = is_allowed(user_id)
    if allowed:
        return {"message": "Request allowed"}
    else:
        raise HTTPException(status_code=429, detail="Rate limit exceeded. Please try again later.")