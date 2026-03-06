"""
clear_redis.py
Deletes the ff2_ccu_data key from Redis so seed_redis.py can start fresh.
Run this ONCE via Railway start command, then run seed_redis.py, then switch back to the ticker.
"""
import os, requests, json

REDIS_URL   = os.environ["UPSTASH_REDIS_REST_URL"]
REDIS_TOKEN = os.environ["UPSTASH_REDIS_REST_TOKEN"]
REDIS_KEY   = "ff2_ccu_data"

r = requests.get(
    f"{REDIS_URL}/del/{REDIS_KEY}",
    headers={"Authorization": f"Bearer {REDIS_TOKEN}"},
    timeout=10,
)
r.raise_for_status()
print(f"Deleted key '{REDIS_KEY}' — result: {r.json()}")
