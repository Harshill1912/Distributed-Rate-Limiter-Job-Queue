import redis

r=redis.Redis(host="localhost",port=6379,decode_responses=True)

def test_connection():
    try :
        r.ping()
        print("connected successfully to redis")
    except redis.ConnectionError:
        print("failed to connect to redis")


if __name__ == "__main__":
    test_connection()

