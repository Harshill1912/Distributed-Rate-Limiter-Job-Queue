import time
from app.redis_client import r;


MAX_TOKENS=5
REFILL_RATE=1/12

def is_allowed(user_id : str) -> bool:
    key=f"rate_limiter:{user_id}"
    now=time.time()

    data=r.hgetall(key)

    if data:
        tokens=float(data['tokens'])
        last_refill=float(data['last_refill'])
    else :
        tokens=MAX_TOKENS
        last_refill=now

    #st-01 how much time has passed since the last check
    elapsed=now - last_refill

    #st-02 calculate token earned in that time
    tokens_earneed=elapsed * REFILL_RATE
    tokens=min(MAX_TOKENS, tokens + tokens_earneed)

    #st-03 decider to allow or block
    if(tokens >=1) :
        tokens -=1
        allowed=True
    else :
        allowed=False

    #st-04 update the redis with new values
    r.hset(key,mapping={"tokens":tokens,"last_refill":now})

    return allowed


if __name__ == "__main__":
    user = "test_user_1"
    for i in range(7):
        result = is_allowed(user)
        print(f"Request {i+1}: {'✅ Allowed' if result else '❌ Blocked'}")

