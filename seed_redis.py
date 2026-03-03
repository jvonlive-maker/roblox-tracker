"""
seed_redis.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
One-time script: reads concurrent-players.csv and writes the
historical hourly stats into Upstash Redis in exactly the same
format the FF2 ticker expects.

Run ONCE before starting the ticker:
    UPSTASH_REDIS_REST_URL=<url> UPSTASH_REDIS_REST_TOKEN=<token> python3 seed_redis.py

Or set those env vars in Railway and run it as a one-off job.
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

import csv
import json
import os
import sys
from collections import defaultdict
from datetime import datetime, timedelta

import requests

# ── Config ───────────────────────────────────────────────────
CSV_PATH             = "concurrent-players.csv"   # put CSV in same folder
TIMEZONE_OFFSET      = -5                          # UTC → EST
REDIS_KEY            = "ff2_ccu_data"
ROBLOX_HISTORICAL_PEAK = 23798
MAX_SAMPLES_PER_SLOT = 30                          # match ticker's slot[-30:] cap

REDIS_URL   = os.environ.get("UPSTASH_REDIS_REST_URL",   "")
REDIS_TOKEN = os.environ.get("UPSTASH_REDIS_REST_TOKEN", "")

# ── Redis helpers ─────────────────────────────────────────────
http = requests.Session()

def _hdrs():
    return {"Authorization": f"Bearer {REDIS_TOKEN}"}

def redis_get(key: str) -> str | None:
    r = http.get(f"{REDIS_URL}/get/{key}", headers=_hdrs(), timeout=10)
    r.raise_for_status()
    return r.json().get("result")

def redis_set(key: str, value: str):
    payload = json.dumps([value])
    r = http.post(
        f"{REDIS_URL}/set/{key}",
        data=payload,
        headers={**_hdrs(), "Content-Type": "application/json"},
        timeout=15,
    )
    r.raise_for_status()

# ── Main ──────────────────────────────────────────────────────
def main():
    if not REDIS_URL or not REDIS_TOKEN:
        print("ERROR: set UPSTASH_REDIS_REST_URL and UPSTASH_REDIS_REST_TOKEN")
        sys.exit(1)

    # 1. Parse CSV
    print(f"Reading {CSV_PATH} ...")
    rows = []
    with open(CSV_PATH, encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            dt_est = (
                datetime.strptime(row["DateTime"].strip(), "%Y-%m-%d %H:%M:%S")
                + timedelta(hours=TIMEZONE_OFFSET)
            )
            rows.append((dt_est, int(row["Players"])))
    print(f"  {len(rows)} rows  |  {rows[0][0].date()} → {rows[-1][0].date()}")

    # 2. Bucket into weekday/weekend → hour
    buckets: dict[str, dict[str, list[int]]] = {
        "weekday": defaultdict(list),
        "weekend": defaultdict(list),
    }
    ticks = []
    for dt, ccu in rows:
        group = "weekend" if dt.weekday() >= 5 else "weekday"
        buckets[group][str(dt.hour)].append(ccu)
        ticks.append({"ts": dt.strftime("%Y-%m-%dT%H:%M"), "ccu": ccu})

    # Cap each slot to MAX_SAMPLES_PER_SLOT (keep most recent)
    stats: dict[str, dict[str, list[int]]] = {"weekday": {}, "weekend": {}}
    for group in ("weekday", "weekend"):
        for h, vals in buckets[group].items():
            stats[group][h] = vals[-MAX_SAMPLES_PER_SLOT:]

    # Keep last 120 ticks for the candle chart (~30 hrs)
    ticks = ticks[-120:]

    # ATH from CSV
    ath = max(ccu for _, ccu in rows)
    ath = max(ath, ROBLOX_HISTORICAL_PEAK)

    # 3. Load existing Redis data so we don't blow away live ticks
    print("Loading existing Redis data ...")
    existing = {}
    try:
        raw = redis_get(REDIS_KEY)
        if raw:
            existing = json.loads(raw)
            print(f"  Found existing data (ath={existing.get('ath', 'none')})")
        else:
            print("  No existing data — starting fresh.")
    except Exception as e:
        print(f"  Warning: could not read existing data: {e}")

    # 4. Merge: CSV stats fill in slots that have fewer samples than the CSV provides.
    #    Live ticker data (if any) takes priority — we never overwrite slots that
    #    already have MORE samples than what the CSV gives us.
    merged_stats: dict[str, dict[str, list[int]]] = {
        "weekday": {},
        "weekend": {},
    }
    for group in ("weekday", "weekend"):
        existing_group = existing.get("stats", {}).get(group, {})
        for h in set(list(stats[group].keys()) + list(existing_group.keys())):
            csv_slot  = stats[group].get(h, [])
            live_slot = existing_group.get(h, [])
            if len(live_slot) >= len(csv_slot):
                # Live data is richer — keep it
                merged_stats[group][h] = live_slot
            else:
                # CSV data is richer — use it, but append any live samples on top
                merged_stats[group][h] = (csv_slot + live_slot)[-MAX_SAMPLES_PER_SLOT:]

    # Merge ticks: keep existing live ticks, prepend CSV ticks to fill history
    existing_ticks = existing.get("ticks", [])
    # Find where CSV ticks end and live ticks begin (avoid duplicates by ts)
    existing_ts = {t["ts"] for t in existing_ticks}
    new_ticks   = [t for t in ticks if t["ts"] not in existing_ts]
    merged_ticks = (new_ticks + existing_ticks)[-120:]

    merged_ath = max(ath, existing.get("ath", 0))

    data = {
        "stats":   merged_stats,
        "ath":     merged_ath,
        "session": existing.get("session", {}),
        "ticks":   merged_ticks,
    }

    # 5. Write to Redis
    print("Writing to Redis ...")
    redis_set(REDIS_KEY, json.dumps(data, separators=(",", ":")))
    print("Done ✓")

    # 6. Summary
    print("\nSeeded stats summary:")
    for group in ("weekday", "weekend"):
        print(f"\n  {group.upper()}")
        for h in sorted(merged_stats[group], key=int):
            vals = merged_stats[group][h]
            avg  = sum(vals) / len(vals)
            print(f"    {int(h):02d}:00  samples={len(vals):3d}  avg={avg:6.0f}")

if __name__ == "__main__":
    main()
