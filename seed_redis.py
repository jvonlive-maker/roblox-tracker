"""
seed_redis.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
One-time script: reads one or both historical CSVs and writes
the combined hourly stats into Upstash Redis in exactly the
format the FF2 ticker + signal engine expect.

Supports two CSVs:
  concurrent-players.csv      — 8 days, 10-min intervals  (denser, more recent)
  concurrent-players__1_.csv  — 32 days, 60-min intervals (broader history)

When both are present, samples from BOTH are merged per hour
slot, then sorted by time so the most recent samples sit at
the end of the list (the ticker's slot[-30:] cap keeps them).

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
CSV_FILES = [
    "concurrent-players__1_.csv",   # 32-day hourly — primary history
    "concurrent-players.csv",       # 8-day 10-min  — denser recent data (optional)
]
TIMEZONE_OFFSET        = -5    # UTC → EST
REDIS_KEY              = "ff2_ccu_data"
ROBLOX_HISTORICAL_PEAK = 23798
MAX_SAMPLES_PER_SLOT   = 30    # matches ticker's slot[-30:] cap

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
    from urllib.parse import quote
    r = http.get(
        f"{REDIS_URL}/set/{quote(key, safe='')}/{quote(value, safe='')}",
        headers=_hdrs(),
        timeout=15,
    )
    r.raise_for_status()

# ── CSV parsing ───────────────────────────────────────────────
def parse_csv(path: str) -> list[tuple[datetime, int]]:
    rows = []
    with open(path, encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            dt_est = (
                datetime.strptime(row["DateTime"].strip(), "%Y-%m-%d %H:%M:%S")
                + timedelta(hours=TIMEZONE_OFFSET)
            )
            rows.append((dt_est, int(row["Players"])))
    return rows

# ── Main ──────────────────────────────────────────────────────
def main():
    if not REDIS_URL or not REDIS_TOKEN:
        print("ERROR: set UPSTASH_REDIS_REST_URL and UPSTASH_REDIS_REST_TOKEN")
        sys.exit(1)

    # 1. Load all available CSVs
    #    Deduplicate by timestamp — denser (10-min) CSV is listed second so it
    #    overwrites the hourly CSV for any overlapping timestamps.
    all_rows: dict[datetime, int] = {}
    for path in CSV_FILES:
        if not os.path.exists(path):
            print(f"  Skipping {path} (not found)")
            continue
        parsed = parse_csv(path)
        print(f"  {path}: {len(parsed)} rows  ({parsed[0][0].date()} → {parsed[-1][0].date()})")
        for dt, ccu in parsed:
            all_rows[dt] = ccu

    if not all_rows:
        print("ERROR: no CSV files found. Place at least one CSV in the same folder.")
        sys.exit(1)

    rows = sorted(all_rows.items())   # (datetime, ccu), oldest → newest
    print(f"\nCombined unique timestamps: {len(rows)}")
    print(f"Date range: {rows[0][0].date()} → {rows[-1][0].date()}")

    # 2. Bucket into weekday/weekend → hour
    raw_buckets: dict[str, dict[int, list[tuple[datetime, int]]]] = {
        "weekday": defaultdict(list),
        "weekend": defaultdict(list),
    }
    for dt, ccu in rows:
        group = "weekend" if dt.weekday() >= 5 else "weekday"
        raw_buckets[group][dt.hour].append((dt, ccu))

    # 3. Downsample to one representative value per clock-hour per day.
    #    This ensures the 10-min CSV (6 readings/hr) doesn't outweigh the
    #    hourly CSV — each calendar hour contributes exactly one sample.
    #    Then sort chronologically and keep the MAX_SAMPLES_PER_SLOT most recent.
    stats: dict[str, dict[str, list[int]]] = {"weekday": {}, "weekend": {}}

    for group in ("weekday", "weekend"):
        for hour, entries in raw_buckets[group].items():
            # Average all readings within each (date, hour) window
            by_clock_hour: dict[tuple, list[int]] = defaultdict(list)
            for dt, ccu in entries:
                by_clock_hour[(dt.date(), dt.hour)].append(ccu)

            # Sort chronologically — newest last so ticker's [-30:] keeps them
            hourly = sorted(
                ((date, hr), round(sum(vals) / len(vals)))
                for (date, hr), vals in by_clock_hour.items()
            )

            stats[group][str(hour)] = [ccu for _, ccu in hourly[-MAX_SAMPLES_PER_SLOT:]]

    # 4. Build tick list for the candle chart (last 120, ~30 hrs of 15-min ticks)
    ticks = [
        {"ts": dt.strftime("%Y-%m-%dT%H:%M"), "ccu": ccu}
        for dt, ccu in rows
    ][-120:]

    # 5. ATH from all data
    ath = max(max(ccu for _, ccu in rows), ROBLOX_HISTORICAL_PEAK)

    # 6. Load existing Redis data — live ticker data wins if richer
    print("\nLoading existing Redis data ...")
    existing: dict = {}
    try:
        raw = redis_get(REDIS_KEY)
        if raw:
            existing = json.loads(raw)
            print(f"  Found existing data (ath={existing.get('ath', 'none')})")
        else:
            print("  No existing data — writing fresh.")
    except Exception as e:
        print(f"  Warning: could not read existing data: {e}")

    # 7. Merge: live data wins if it has >= samples; CSV fills thinner slots
    merged_stats: dict[str, dict[str, list[int]]] = {"weekday": {}, "weekend": {}}
    for group in ("weekday", "weekend"):
        existing_group = existing.get("stats", {}).get(group, {})
        all_hours = set(list(stats[group].keys()) + list(existing_group.keys()))
        for h in all_hours:
            csv_slot  = stats[group].get(h, [])
            live_slot = existing_group.get(h, [])
            if len(live_slot) >= len(csv_slot):
                merged_stats[group][h] = live_slot
            else:
                # Use CSV as base, append any unique live samples on top
                merged_stats[group][h] = (csv_slot + live_slot)[-MAX_SAMPLES_PER_SLOT:]

    # Merge ticks: prepend CSV history, live ticks stay at the tail
    existing_ticks = existing.get("ticks", [])
    existing_ts    = {t["ts"] for t in existing_ticks}
    new_ticks      = [t for t in ticks if t["ts"] not in existing_ts]
    merged_ticks   = (new_ticks + existing_ticks)[-120:]

    merged_ath = max(ath, existing.get("ath", 0))

    data = {
        "stats":   merged_stats,
        "ath":     merged_ath,
        "session": existing.get("session", {}),
        "ticks":   merged_ticks,
    }

    # 8. Write to Redis
    print("Writing merged data to Redis ...")
    redis_set(REDIS_KEY, json.dumps(data, separators=(",", ":")))
    print("Done ✓")

    # 9. Human-readable summary
    print("\nSeeded stats summary:")
    for group in ("weekday", "weekend"):
        print(f"\n  {group.upper()}")
        for h in sorted(merged_stats[group], key=int):
            vals = merged_stats[group][h]
            avg  = sum(vals) / len(vals)
            print(f"    {int(h):02d}:00  samples={len(vals):3d}  avg={avg:7.0f}  "
                  f"min={min(vals):5d}  max={max(vals):5d}")
    print(f"\n  ATH: {merged_ath:,}")
    print(f"  Ticks in chart buffer: {len(merged_ticks)}")

if __name__ == "__main__":
    main()
