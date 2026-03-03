"""
inspect_redis.py
Prints a summary of what's currently stored in Redis.
Run via Railway start command to debug seed issues.
"""
import os, requests, json

REDIS_URL   = os.environ["UPSTASH_REDIS_REST_URL"]
REDIS_TOKEN = os.environ["UPSTASH_REDIS_REST_TOKEN"]
REDIS_KEY   = "ff2_ccu_data"

r = requests.get(
    f"{REDIS_URL}/get/{REDIS_KEY}",
    headers={"Authorization": f"Bearer {REDIS_TOKEN}"},
    timeout=10,
)
r.raise_for_status()
result = r.json().get("result")

print(f"Raw type: {type(result)}")
print(f"Raw value (first 300 chars): {str(result)[:300]}")

if isinstance(result, str):
    data = json.loads(result)
    print(f"\nATH: {data.get('ath')}")
    print(f"Ticks: {len(data.get('ticks', []))}")
    stats = data.get("stats", {})
    for group in ("weekday", "weekend"):
        print(f"\n{group.upper()}")
        for h in sorted(stats.get(group, {}), key=int):
            vals = stats[group][h]
            print(f"  {int(h):02d}:00  samples={len(vals)}")
elif isinstance(result, list):
    print("\nERROR: Redis is still storing a list — clear_redis.py needs to be run again")
else:
    print("\nERROR: Redis key is empty or missing — seed_redis.py needs to be run")
