"""
backtest.py
Simulates predictions against historical CSV data to pre-train the model.
Merges hourly + 10-min CSVs, runs tick-by-tick prediction & scoring,
then writes learned weights, bias, and slot errors into Redis.

Run ONCE before starting the ticker (or after a reseed):
  UPSTASH_REDIS_REST_URL=x UPSTASH_REDIS_REST_TOKEN=y python3 backtest.py

Optional flags:
  --dry-run    simulate only, don't write to Redis
  --game FF2   only backtest one game
"""

import csv, json, os, sys, statistics
from collections import defaultdict
from datetime import datetime, timedelta
from urllib.parse import quote

import requests
import config

# ── Args ──────────────────────────────────────────────────────────────────────
dry_run     = "--dry-run" in sys.argv
filter_game = None
if "--game" in sys.argv:
    idx = sys.argv.index("--game")
    if idx + 1 < len(sys.argv):
        filter_game = sys.argv[idx + 1].upper()

# ── Redis ─────────────────────────────────────────────────────────────────────
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

# ── CSV loader ────────────────────────────────────────────────────────────────
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

def merge_csvs(hourly, tenmin):
    """
    Merge hourly and 10-min rows into one timeline.
    In the overlap period, prefer 10-min data (denser).
    Outside overlap, use whatever is available.
    Returns sorted list of (dt, ccu) with no duplicate timestamps.
    """
    seen = {}
    for dt, ccu in hourly:
        seen[dt] = ccu
    # 10-min overwrites hourly where they overlap
    for dt, ccu in tenmin:
        seen[dt] = ccu
    return sorted(seen.items())

# ── Slot stats (mirrors ticker logic) ─────────────────────────────────────────
def slot_avg(slots, dow, hour):
    return slots.get(f"{dow}_{hour % 24}", {}).get("avg")

def update_slot(slots, dow, hour, ccu):
    key  = f"{dow}_{hour}"
    slot = slots.setdefault(key, {"avg": ccu, "std": 0.0, "cv": 0.0, "n": 0, "_vals": []})
    vals = slot.get("_vals", [])
    vals.append(ccu)
    vals = vals[-30:]
    slot["_vals"] = vals
    slot["n"]     = len(vals)
    slot["avg"]   = sum(vals) / len(vals)
    slot["std"]   = statistics.stdev(vals) if len(vals) > 1 else 0.0
    slot["cv"]    = slot["std"] / slot["avg"] * 100 if slot["avg"] > 0 else 0.0
    slots[key]    = slot
    return slot

def trend_adjusted_avg(slot, last_daily, current_ccu):
    if slot is None or last_daily is None or last_daily == 0:
        return slot["avg"] if slot else None
    ratio = current_ccu / last_daily
    return slot["avg"] * 0.5 + slot["avg"] * ratio * 0.5

# ── Predictor (mirrors ticker logic) ──────────────────────────────────────────
def predict(slots, ticks, ccu, dow, hour, minute,
            weights, bias, last_daily, slot_errors):
    curr_slot = slots.get(f"{dow}_{hour}")
    next_slot = slots.get(f"{dow}_{(hour+1)%24}")

    baseline_delta = None
    baseline_cv    = None
    baseline_std   = None
    if curr_slot and next_slot and curr_slot["n"] >= 3:
        curr_adj       = trend_adjusted_avg(curr_slot, last_daily, ccu)
        next_adj       = trend_adjusted_avg(next_slot, last_daily, ccu)
        baseline_delta = (next_adj - curr_adj) * (15 / 60)
        baseline_cv    = curr_slot["cv"]
        baseline_std   = curr_slot["std"]

    momentum_delta = None
    if len(ticks) >= 3:
        recent = ticks[-3:]
        deltas = [recent[i+1][1] - recent[i][1] for i in range(2)]
        momentum_delta = deltas[0] * 0.4 + deltas[1] * 0.6

    slot_deltas_list = []
    for i in range(len(ticks) - 1):
        t0_dt, t0_ccu = ticks[i]
        t1_dt, t1_ccu = ticks[i+1]
        diff_mins = int((t1_dt - t0_dt).total_seconds() // 60)
        if (t0_dt.weekday() == dow and t0_dt.hour == hour
                and t0_dt.minute == minute and diff_mins in (10, 15, 60)):
            slot_deltas_list.append(t1_ccu - t0_ccu)
    slot_delta = sum(slot_deltas_list) / len(slot_deltas_list) if slot_deltas_list else None

    sigs = []; wts = []; used = []
    if baseline_delta is not None:
        cv_factor = max(0.1, 1.0 - (baseline_cv or 50) / 100)
        sigs.append(baseline_delta); wts.append(weights["baseline"] * cv_factor)
        used.append("baseline")
    if slot_delta is not None:
        n_factor = min(1.0, len(slot_deltas_list) / 8)
        sigs.append(slot_delta); wts.append(weights["slot"] * n_factor)
        used.append("slot")
    if momentum_delta is not None:
        sigs.append(momentum_delta); wts.append(weights["momentum"])
        used.append("momentum")

    if not sigs:
        return None, None, []

    total_w   = sum(wts)
    delta     = sum(s * w for s, w in zip(sigs, wts)) / total_w
    delta_corr = delta - bias
    predicted  = round(ccu + delta_corr)
    return predicted, baseline_std, used

# ── Scoring & learning (mirrors ticker logic) ──────────────────────────────────
def score_and_learn(predicted, actual, ccu, dh_key, signals_used,
                    weights, bias, slot_errors):
    error     = predicted - actual
    abs_error = abs(error)

    # Per-slot error memory
    se   = slot_errors.setdefault(dh_key, {"mae": abs_error, "n": 1, "_errs": [abs_error]})
    errs = se.get("_errs", [se["mae"]])
    errs.append(abs_error)
    errs        = errs[-20:]
    se["_errs"] = errs
    se["n"]     = len(errs)
    se["mae"]   = sum(errs) / len(errs)
    slot_errors[dh_key] = se

    # Bias correction
    alpha = 0.15
    bias  = bias * (1 - alpha) + error * alpha

    # Adaptive weights
    pct_error = abs_error / max(ccu, 1) * 100
    if pct_error < 5:
        for s in signals_used:
            if s in weights:
                weights[s] = min(2.5, weights[s] * 1.03)
    elif pct_error > 20:
        for s in signals_used:
            if s in weights:
                weights[s] = max(0.2, weights[s] * 0.95)

    total = sum(weights.values())
    if total > 0:
        factor = 3.3 / total
        for k in weights:
            weights[k] = round(weights[k] * factor, 4)

    return bias, weights, slot_errors

# ── Main backtest loop ────────────────────────────────────────────────────────
def backtest_game(game):
    name     = game["name"]
    base     = game.get("csv_base", name.lower())
    print(f"\n{'='*55}\nBacktesting: {name}")

    hourly = load_csv(f"{base}_hourly.csv")
    tenmin = load_csv(f"{base}_10min.csv")

    if not hourly and not tenmin:
        # Try uploads directory
        hourly = load_csv(f"/mnt/user-data/uploads/{base}_hourly.csv")
        tenmin = load_csv(f"/mnt/user-data/uploads/{base}_10min.csv")

    if not hourly and not tenmin:
        print(f"  No CSVs found for {name} — skipping")
        return

    timeline = merge_csvs(hourly, tenmin)
    print(f"  Timeline: {timeline[0][0].date()} → {timeline[-1][0].date()}  "
          f"({len(timeline)} ticks)")

    # Load existing Redis data to get slot averages + last_daily
    existing = {}
    try:
        raw = redis_get(game["redis_key"])
        if raw:
            existing = json.loads(raw)
            print(f"  Loaded existing Redis data")
    except Exception as e:
        print(f"  Warning: could not load Redis: {e}")

    # Start from existing slots (seeded) — backtest refines on top
    slots       = existing.get("slots", {})
    last_daily  = existing.get("last_daily")
    weights     = {"baseline": 1.5, "slot": 1.2, "momentum": 0.6}
    bias        = 0.0
    slot_errors = {}

    # Tick buffer for momentum
    tick_buf = []
    # Prediction buffer: (dt, predicted, dh_key, signals_used)
    pred_buf = None

    scored = skipped = errors = 0
    abs_errors = []

    for i, (dt, ccu) in enumerate(timeline):
        dow    = dt.weekday()
        hour   = dt.hour
        minute = dt.minute
        dh_key = f"{dow}_{hour}"

        # Score previous prediction
        if pred_buf is not None:
            p_dt, p_pred, p_dh, p_used = pred_buf
            bias, weights, slot_errors = score_and_learn(
                p_pred, ccu, ccu, p_dh, p_used, weights, bias, slot_errors
            )
            abs_errors.append(abs(p_pred - ccu))
            scored += 1
            pred_buf = None

        # Update slot stats
        update_slot(slots, dow, hour, ccu)

        # Update last_daily (use daily CCU as rough proxy)
        if last_daily is None:
            last_daily = ccu

        # Make prediction for next tick
        predicted, std, used = predict(
            slots, tick_buf, ccu, dow, hour, minute,
            weights, bias, last_daily, slot_errors
        )

        if predicted is not None:
            pred_buf = (dt, predicted, dh_key, used)
        else:
            skipped += 1

        tick_buf.append((dt, ccu))
        tick_buf = tick_buf[-2016:]

    # Summary
    mae = sum(abs_errors) / len(abs_errors) if abs_errors else 0
    print(f"  Scored: {scored}  Skipped: {skipped}")
    print(f"  MAE: {mae:.1f} CCU")
    print(f"  Final bias: {bias:+.2f}")
    print(f"  Final weights: {weights}")

    # Per-slot error summary (top 5 worst)
    if slot_errors:
        worst = sorted(slot_errors.items(), key=lambda x: x[1]["mae"], reverse=True)[:5]
        print(f"  Worst slots:")
        days = ["Mon","Tue","Wed","Thu","Fri","Sat","Sun"]
        for k, se in worst:
            d, h = map(int, k.split("_"))
            print(f"    {days[d]} {h:02d}:00  MAE={se['mae']:.0f}  n={se['n']}")

    if dry_run:
        print(f"  DRY RUN — not writing to Redis")
        return

    # Merge learned state into existing Redis data
    existing["signal_weights"] = weights
    existing["bias"]           = round(bias, 4)
    existing["slot_errors"]    = slot_errors
    existing["slots"]          = slots
    # Preserve everything else (ticks, pred_log, session, ath, etc.)

    redis_set(game["redis_key"],
              json.dumps(existing, separators=(",", ":")))
    print(f"  Written to Redis ✓")

def main():
    if not dry_run and (not REDIS_URL or not REDIS_TOKEN):
        print("ERROR: set UPSTASH_REDIS_REST_URL and UPSTASH_REDIS_REST_TOKEN")
        print("(or use --dry-run to simulate without Redis)")
        sys.exit(1)

    games = [g for g in config.GAMES
             if filter_game is None or g["name"].upper() == filter_game]

    for game in games:
        backtest_game(game)

    print(f"\nDone. Restart the ticker to use the new weights.")

if __name__ == "__main__":
    main()
