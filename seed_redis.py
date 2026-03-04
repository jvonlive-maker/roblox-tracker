"""
seed_redis.py
Seeds ALL games in config.GAMES using all available CSV tiers:
  - <game>_daily.csv   — long-term trend (daily since release)
  - <game>_hourly.csv  — dow x hour baseline (1 month hourly)
  - <game>_10min.csv   — recent momentum / intra-hour shape (1 week 10-min)

Run once before starting the ticker:
  UPSTASH_REDIS_REST_URL=x UPSTASH_REDIS_REST_TOKEN=y python3 seed_redis.py
"""

import csv, json, math, os, sys, statistics
from collections import defaultdict
from datetime import datetime, timedelta
from urllib.parse import quote

import requests
import config

REDIS_URL   = os.environ.get("UPSTASH_REDIS_REST_URL",   "")
REDIS_TOKEN = os.environ.get("UPSTASH_REDIS_REST_TOKEN", "")
http = requests.Session()

def _hdrs(): return {"Authorization": f"Bearer {REDIS_TOKEN}"}
def redis_get(k):
    r = http.get(f"{REDIS_URL}/get/{quote(k,safe='')}", headers=_hdrs(), timeout=10)
    r.raise_for_status()
    v = r.json().get("result")
    return v if isinstance(v, str) else None
def redis_set(k, v):
    http.get(f"{REDIS_URL}/set/{quote(k,safe='')}/{quote(v,safe='')}",
             headers=_hdrs(), timeout=15).raise_for_status()

def load_csv(path):
    if not os.path.exists(path):
        return []
    rows = []
    with open(path, encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            dt = (datetime.strptime(row["DateTime"].strip(), "%Y-%m-%d %H:%M:%S")
                  + timedelta(hours=config.TIMEZONE_OFFSET))
            rows.append((dt, int(row["Players"])))
    return sorted(rows)

def compute_slot_stats(values):
    """Given a list of CCU values, return avg, stddev, cv, n."""
    n = len(values)
    if n == 0:
        return None
    avg = sum(values) / n
    std = statistics.stdev(values) if n > 1 else 0.0
    cv  = (std / avg * 100) if avg > 0 else 0.0
    return {"avg": round(avg, 2), "std": round(std, 2), "cv": round(cv, 2), "n": n}

def compute_trend(daily_rows, days=30):
    """Linear regression slope (CCU/day) over last `days` days."""
    recent = daily_rows[-days:]
    if len(recent) < 3:
        return 0.0
    xs = list(range(len(recent)))
    ys = [c for _, c in recent]
    xm = sum(xs) / len(xs)
    ym = sum(ys) / len(ys)
    denom = sum((x - xm) ** 2 for x in xs)
    if denom == 0:
        return 0.0
    return sum((x - xm) * (y - ym) for x, y in zip(xs, ys)) / denom

def seed_game(game):
    name = game["name"]
    base = game.get("csv_base", name.lower())  # e.g. "ff2"
    print(f"\n{'='*55}\nSeeding: {name}")

    daily_rows  = load_csv(f"{base}_daily.csv")
    hourly_rows = load_csv(f"{base}_hourly.csv")
    tenmin_rows = load_csv(f"{base}_10min.csv")

    if not hourly_rows and not tenmin_rows:
        print(f"  WARNING: no CSV data found for {name} — skipping.")
        return

    for label, rows in [("daily", daily_rows), ("hourly", hourly_rows), ("10min", tenmin_rows)]:
        if rows:
            print(f"  {label:8s}: {len(rows):5d} rows  "
                  f"({rows[0][0].date()} → {rows[-1][0].date()})")
        else:
            print(f"  {label:8s}: not found")

    # ── Build dow x hour slots from hourly data ────────────────
    # dow = 0 (Mon) … 6 (Sun)
    dow_hour: dict[str, list[int]] = defaultdict(list)  # key = "dow_hour"
    for dt, ccu in hourly_rows:
        key = f"{dt.weekday()}_{dt.hour}"
        dow_hour[key].append(ccu)

    # Also fold in 10-min data: average each (date, hour) window → one value
    tenmin_by_dh: dict[tuple, list[int]] = defaultdict(list)
    for dt, ccu in tenmin_rows:
        tenmin_by_dh[(dt.date(), dt.weekday(), dt.hour)].append(ccu)
    for (date, dow, hour), vals in tenmin_by_dh.items():
        key = f"{dow}_{hour}"
        dow_hour[key].append(round(sum(vals) / len(vals)))

    # Compute stats per slot
    slots: dict[str, dict] = {}
    for key, vals in dow_hour.items():
        s = compute_slot_stats(vals)
        if s:
            slots[key] = s

    # ── Trend correction ───────────────────────────────────────
    trend_7d  = compute_trend(daily_rows, 7)   if daily_rows else 0.0
    trend_30d = compute_trend(daily_rows, 30)  if daily_rows else 0.0
    # Blend: 70% short-term, 30% medium-term
    trend_blend = trend_7d * 0.7 + trend_30d * 0.3
    last_daily  = daily_rows[-1][1] if daily_rows else None
    print(f"  Trend: 7d={trend_7d:+.1f}/day  30d={trend_30d:+.1f}/day  "
          f"blended={trend_blend:+.1f}/day")

    # ── Build tick buffer from 10-min data ────────────────────
    # Keep last 2016 ticks (~2 weeks at 10-min = enough for same-slot momentum)
    ticks = [{"ts": dt.strftime("%Y-%m-%dT%H:%M"), "ccu": ccu}
             for dt, ccu in tenmin_rows][-2016:]
    if not ticks and hourly_rows:
        ticks = [{"ts": dt.strftime("%Y-%m-%dT%H:%M"), "ccu": ccu}
                 for dt, ccu in hourly_rows][-2016:]

    # ── ATH ───────────────────────────────────────────────────
    all_ccus = [c for _, c in (daily_rows + hourly_rows + tenmin_rows)]
    ath = max(max(all_ccus), game["ath_floor"]) if all_ccus else game["ath_floor"]

    # ── Load existing Redis data ──────────────────────────────
    existing = {}
    try:
        raw = redis_get(game["redis_key"])
        if raw:
            existing = json.loads(raw)
            print(f"  Existing Redis data found (ATH={existing.get('ath'):,})")
        else:
            print("  No existing data — writing fresh.")
    except Exception as e:
        print(f"  Warning loading existing: {e}")

    # Merge ticks: CSV first, live ticks at tail
    existing_ticks = existing.get("ticks", [])
    existing_ts    = {t["ts"] for t in existing_ticks}
    merged_ticks   = ([t for t in ticks if t["ts"] not in existing_ts]
                      + existing_ticks)[-2016:]

    merged_ath = max(ath, existing.get("ath", 0))

    data = {
        "slots":      slots,          # new: dow_hour keyed slot stats
        "trend":      round(trend_blend, 3),
        "last_daily": last_daily,
        "ath":        merged_ath,
        "session":    existing.get("session", {}),
        "ticks":      merged_ticks,
    }

    redis_set(game["redis_key"], json.dumps(data, separators=(",", ":")))
    print(f"  Done ✓  slots={len(slots)}  ticks={len(merged_ticks)}  ATH={merged_ath:,}")

    # Summary of key slots
    days_label = ["Mon","Tue","Wed","Thu","Fri","Sat","Sun"]
    print(f"\n  Slot summary (peak hours):")
    for dow in range(7):
        for hour in [15, 16]:
            key = f"{dow}_{hour}"
            s = slots.get(key)
            if s:
                print(f"    {days_label[dow]} {hour:02d}:00  "
                      f"n={s['n']}  avg={s['avg']:7,.0f}  "
                      f"cv={s['cv']:.1f}%  stddev={s['std']:6,.0f}")

def main():
    if not REDIS_URL or not REDIS_TOKEN:
        print("ERROR: set UPSTASH_REDIS_REST_URL and UPSTASH_REDIS_REST_TOKEN")
        sys.exit(1)
    print(f"Seeding {len(config.GAMES)} game(s)...")
    for game in config.GAMES:
        seed_game(game)
    print(f"\nAll done. Start with: python3 ticker.py")

if __name__ == "__main__":
    main()
