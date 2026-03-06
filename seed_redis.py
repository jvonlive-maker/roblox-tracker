"""
seed_redis.py
Seeds ALL games in config.GAMES using all available CSV tiers:
  - <game>_daily.csv   — long-term trend (daily since release)
  - <game>_hourly.csv  — dow x hour baseline (1 month hourly)
  - <game>_10min.csv   — recent momentum / intra-hour shape (1 week 10-min)

Then automatically runs backtest to learn weights, bias, and slot errors
on top of the freshly seeded data — no need to run backtest.py separately.

Run once before starting the ticker:
  UPSTASH_REDIS_REST_URL=x UPSTASH_REDIS_REST_TOKEN=y python3 seed_redis.py

Optional flags:
  --game FF2   only seed + backtest one game
  --seed-only  skip backtest (seed only)
"""

import csv, json, os, sys, statistics
from collections import defaultdict
from datetime import datetime, date, timedelta
from urllib.parse import quote

import requests
import config

# ── Args ──────────────────────────────────────────────────────────────────────
seed_only   = "--seed-only" in sys.argv
filter_game = None
if "--game" in sys.argv:
    idx = sys.argv.index("--game")
    if idx + 1 < len(sys.argv):
        filter_game = sys.argv[idx + 1].upper()

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
    base = game.get("csv_base", name.lower())
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

    # ── Build dow x hour slots ─────────────────────────────────
    # Collect all CCU values per (dow, hour) key
    dow_hour: dict[str, list[int]] = defaultdict(list)

    for dt, ccu in hourly_rows:
        dow_hour[f"{dt.weekday()}_{dt.hour}"].append(ccu)

    # Fold 10-min data: average each (date, hour) window → one representative value
    tenmin_by_dh: dict[tuple, list[int]] = defaultdict(list)
    for dt, ccu in tenmin_rows:
        tenmin_by_dh[(dt.date(), dt.weekday(), dt.hour)].append(ccu)
    for (_, dow, hour), vals in tenmin_by_dh.items():
        dow_hour[f"{dow}_{hour}"].append(round(sum(vals) / len(vals)))

    # Build slot dicts with _vals list (required by ticker's rolling window update)
    slots: dict[str, dict] = {}
    for key, vals in dow_hour.items():
        if not vals:
            continue
        n   = len(vals)
        avg = sum(vals) / n
        std = statistics.stdev(vals) if n > 1 else 0.0
        cv  = (std / avg * 100) if avg > 0 else 0.0
        # FIX: include _vals so ticker's record_snapshot can extend the rolling window
        # without treating the slot as brand-new (seed_n logic uses n to backfill)
        slots[key] = {
            "avg":   round(avg, 2),
            "std":   round(std, 2),
            "cv":    round(cv,  2),
            "n":     n,
            "_vals": vals[-30:],   # keep last 30 as the warm-start rolling window
        }

    # ── Trend ─────────────────────────────────────────────────
    trend_7d    = compute_trend(daily_rows,  7) if daily_rows else 0.0
    trend_30d   = compute_trend(daily_rows, 30) if daily_rows else 0.0
    trend_blend = trend_7d * 0.7 + trend_30d * 0.3
    last_daily  = daily_rows[-1][1] if daily_rows else None
    print(f"  Trend: 7d={trend_7d:+.1f}/day  30d={trend_30d:+.1f}/day  "
          f"blended={trend_blend:+.1f}/day")

    # ── Tick buffer ────────────────────────────────────────────
    ticks = [{"ts": dt.strftime("%Y-%m-%dT%H:%M"), "ccu": ccu}
             for dt, ccu in tenmin_rows][-2016:]
    if not ticks and hourly_rows:
        ticks = [{"ts": dt.strftime("%Y-%m-%dT%H:%M"), "ccu": ccu}
                 for dt, ccu in hourly_rows][-2016:]

    # ── ATH ───────────────────────────────────────────────────
    all_ccus = [c for _, c in (daily_rows + hourly_rows + tenmin_rows)]
    ath      = max(max(all_ccus), game["ath_floor"]) if all_ccus else game["ath_floor"]

    # ── Load existing Redis data ──────────────────────────────
    existing = {}
    try:
        raw = redis_get(game["redis_key"])
        if raw:
            existing = json.loads(raw)
            print(f"  Existing Redis data found (ATH={existing.get('ath', 0):,})")
        else:
            print("  No existing data — writing fresh.")
    except Exception as e:
        print(f"  Warning loading existing: {e}")

    # Merge ticks: CSV first, live ticks at tail (deduplicated by ts)
    existing_ticks = existing.get("ticks", [])
    existing_ts    = {t["ts"] for t in existing_ticks}
    merged_ticks   = ([t for t in ticks if t["ts"] not in existing_ts]
                      + existing_ticks)[-2016:]

    merged_ath    = max(ath, existing.get("ath", 0))
    merged_ath_ts = existing.get("ath_ts") if existing.get("ath", 0) >= ath else None

    # Preserve learned state across reseeds — only overwrite structural slot data.
    # signal_weights, bias, slot_errors, pred_log, week_stats, signal_streak
    # are all the result of live learning and should survive a reseed.
    # drift_warned is intentionally RESET on reseed — drift alerts right after
    # a reseed are always noise since we just refreshed the slot averages.
    data = {
        # Structural — always overwritten from fresh CSV
        "slots":      slots,
        "trend":      round(trend_blend, 3),
        "last_daily": last_daily,
        "ath":        merged_ath,
        "ath_ts":     merged_ath_ts,
        "ticks":      merged_ticks,
        "seed_ts":    date.today().isoformat(),

        # Session — preserve live intraday state
        "session":    existing.get("session", {}),

        # Learned — preserve across reseeds
        "signal_weights": existing.get("signal_weights",
                                       {"baseline": 1.5, "slot": 1.2, "momentum": 0.6}),
        "bias":           existing.get("bias",         0.0),
        "slot_errors":    existing.get("slot_errors",  {}),
        "pred_log":       existing.get("pred_log",     []),
        "week_stats":     existing.get("week_stats",   {}),
        "signal_streak":  existing.get("signal_streak",
                                       {"signal": None, "count": 0}),

        # Reset on every reseed — stale drift warnings are irrelevant after fresh data
        "drift_warned":   {},
    }

    redis_set(game["redis_key"], json.dumps(data, separators=(",", ":")))
    print(f"  Done ✓  slots={len(slots)}  ticks={len(merged_ticks)}  ATH={merged_ath:,}")

    # ── Slot summary ──────────────────────────────────────────
    days_label = ["Mon","Tue","Wed","Thu","Fri","Sat","Sun"]

    # Peak hours per day
    print(f"\n  Peak hours (top slot per day):")
    for dow in range(7):
        day_slots = {h: slots[f"{dow}_{h}"] for h in range(24) if f"{dow}_{h}" in slots}
        if not day_slots:
            continue
        best_h = max(day_slots, key=lambda h: day_slots[h]["avg"])
        s = day_slots[best_h]
        print(f"    {days_label[dow]} {best_h:02d}:00  "
              f"n={s['n']}  avg={s['avg']:7,.0f}  "
              f"cv={s['cv']:.1f}%  std={s['std']:6,.0f}")

    # Noisiest slots — useful for knowing which signals will be suppressed
    noisy = [(k, s) for k, s in slots.items() if s["cv"] > 30]
    if noisy:
        noisy.sort(key=lambda x: x[1]["cv"], reverse=True)
        print(f"\n  ⚠️  Noisy slots (cv > 30% — signals will be suppressed):")
        for k, s in noisy[:8]:
            d, h = map(int, k.split("_"))
            print(f"    {days_label[d]} {h:02d}:00  cv={s['cv']:.0f}%  "
                  f"avg={s['avg']:,.0f}  n={s['n']}")

def main():
    if not REDIS_URL or not REDIS_TOKEN:
        print("ERROR: set UPSTASH_REDIS_REST_URL and UPSTASH_REDIS_REST_TOKEN")
        sys.exit(1)

    games = [g for g in config.GAMES
             if filter_game is None or g["name"].upper() == filter_game]

    if not games:
        print(f"ERROR: no matching game for --game {filter_game}")
        sys.exit(1)

    print(f"Seeding {len(games)} game(s)...")
    for game in games:
        seed_game(game)

    if seed_only:
        print(f"\nSeed complete (backtest skipped). Start with: python3 ticker.py")
        return

    # Auto-run backtest on top of freshly seeded data
    print(f"\n{'='*55}")
    print(f"Seed complete — running backtest to learn weights & bias...")
    print(f"{'='*55}")

    from backtest import backtest_game
    for game in games:
        backtest_game(game)

    print(f"\nAll done. Start with: python3 ticker.py")

if __name__ == "__main__":
    main()
