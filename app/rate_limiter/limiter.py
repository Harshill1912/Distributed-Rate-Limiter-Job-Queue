"""
Distributed token-bucket rate limiter.

The check-and-decrement runs as a single Redis Lua script, so it is ATOMIC:
Redis executes the whole script without interleaving other commands. This closes
the read-modify-write race that a naive HGETALL -> compute -> HSET has, where two
concurrent requests for the same user read the same token count and both get
allowed (letting callers exceed the limit under load).

Config is read from the environment so limits are tunable without code changes.
"""
import os
import time

from app.redis_client import r

# tokens per bucket (burst size) and refill speed (tokens per second).
MAX_TOKENS = int(os.getenv("RATE_LIMIT_MAX_TOKENS", "5"))
REFILL_RATE = float(os.getenv("RATE_LIMIT_REFILL_RATE", str(1 / 12)))  # ~1 token / 12s
KEY_TTL = int(os.getenv("RATE_LIMIT_KEY_TTL", "3600"))                 # expire idle buckets

# KEYS[1]=bucket  ARGV: now, max_tokens, refill_rate, ttl  -> returns 1 (allow) / 0 (block)
_TOKEN_BUCKET_LUA = """
local key = KEYS[1]
local now = tonumber(ARGV[1])
local max_tokens = tonumber(ARGV[2])
local refill_rate = tonumber(ARGV[3])
local ttl = tonumber(ARGV[4])

local bucket = redis.call('HMGET', key, 'tokens', 'last_refill')
local tokens = tonumber(bucket[1])
local last_refill = tonumber(bucket[2])

if tokens == nil then
    tokens = max_tokens
    last_refill = now
end

-- refill for the time elapsed since we last saw this bucket
local elapsed = now - last_refill
tokens = math.min(max_tokens, tokens + elapsed * refill_rate)

local allowed = 0
if tokens >= 1 then
    tokens = tokens - 1
    allowed = 1
end

redis.call('HSET', key, 'tokens', tokens, 'last_refill', now)
redis.call('EXPIRE', key, ttl)
return allowed
"""

# register_script computes the SHA locally and uses EVALSHA (falling back to EVAL);
# it does NOT open a connection here, so import stays cheap and Redis-independent.
_token_bucket = r.register_script(_TOKEN_BUCKET_LUA)


def is_allowed(user_id: str) -> bool:
    """Return True if this request is within the user's rate limit, else False.

    Atomic: safe under high concurrency for the same user_id.
    """
    key = f"rate_limiter:{user_id}"
    allowed = _token_bucket(keys=[key], args=[time.time(), MAX_TOKENS, REFILL_RATE, KEY_TTL])
    return bool(allowed)


if __name__ == "__main__":
    user = "test_user_1"
    for i in range(MAX_TOKENS + 2):
        print(f"Request {i + 1}: {'ALLOWED' if is_allowed(user) else 'BLOCKED'}")
