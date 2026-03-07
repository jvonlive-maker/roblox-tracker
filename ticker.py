"""
ticker.py
Multi-game Roblox CCU ticker.
- Polls every game in config.GAMES every 15 min
- Sends alerts to Discord webhook
- Persists state in Upstash Redis
- Self-improving: adaptive signal weights, bias correction, per-slot error memory

Fixes:
  1. LONG/SHORT suppressed to HOLD when cv > 30%
  2. Peak fallback removed — no more cross-day misleading peak
  3. last_tick restored unconditionally across restarts
  4. time_to_peak requires meaningful uplift (PEAK_MIN_UPLIFT_PCT)

Additions:
  5. Per-slot drift detection — alerts when slot averages lag actuals by >20%
  6. Confidence calibration report in weekly summary
  7. Discord spacer — zero-width space message between each game's embed
  8. Trend engine — structural pattern detection (DOW cycle, decay, macro, anomaly, etc.)
"""

import json, logging, os, signal, time, statistics
from datetime import datetime, timezone, timedelta
from urllib.parse import quote

import requests
import config

logging.basicConfig(level=logging.INFO,
                    format="[%(asctime)s] %(levelname)s: %(message)s",
                    datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

_shutdown = False
def _handle_signal(sig, frame):
    global _shutdown
    log.info("Shutdown — finishing current tick.")
    _shutdown = True
signal.signal(signal.SIGINT,  _handle_signal)
signal.signal(signal.SIGTERM, _handle_signal)

http = requests.Session()
http.headers.update({"User-Agent": "Mozilla/5.0"})

REDIS_URL   = os.environ.get("UPSTASH_REDIS_REST_URL",   "")
REDIS_TOKEN = os.environ.get("UPSTASH_REDIS_REST_TOKEN", "")

def _rh(): return {"Authorization": f"Bearer {REDIS_TOKEN}"}

def redis_get(key):
    r = http.get(f"{REDIS_URL}/get/{quote(key,safe='')}", headers=_rh(), timeout=10)
    r.raise_for_status()
    v = r.json().get("result")
    return v if isinstance(v, str) else None

def redis_set(key, value, retries=3):
    url = f"{REDIS_URL}/set/{quote(key,safe='')}/{quote(value,safe='')}"
    for attempt in range(1, retries+1):
        try:
            http.get(url, headers=_rh(), timeout=10).raise_for_status()
            return
        except requests.RequestException as e:
            wait = 2**attempt
            log.warning(f"Redis SET failed ({attempt}/{retries}): {e}. Retry in {wait}s")
            time.sleep(wait)
    log.error("All Redis SET attempts failed.")

# ── Constants ─────────────────────────────────────────────────────────────────
CV_NOISE_CEILING    = 30.0   # suppress LONG/SHORT above this CV%
PEAK_MIN_UPLIFT_PCT = 10.0   # minimum % uplift required to show time-to-peak
DRIFT_THRESHOLD_PCT = 20.0   # slot avg vs recent actuals % gap to trigger drift alert
DRIFT_MIN_TICKS     = 8      # need at least this many recent same-slot ticks to check
AUTOSEED_MIN_TICKS  = 50     # minimum stored ticks required to attempt auto-seed
AUTOSEED_MIN_SLOT_N = 8      # if avg slot samples < this, trigger auto-seed

DAYS = ["Mon","Tue","Wed","Thu","Fri","Sat","Sun"]

# ── Data ──────────────────────────────────────────────────────────────────────

def load_data(game):
    try:
        raw = redis_get(game["redis_key"])
        if raw:
            data = json.loads(raw)
            data.setdefault("ath",            game["ath_floor"])
            data["ath"] = max(data["ath"],    game["ath_floor"])
            data.setdefault("ath_ts",         None)
            data.setdefault("slots",          {})
            data.setdefault("trend",          0.0)
            data.setdefault("last_daily",     None)
            data.setdefault("session",        {})
            data.setdefault("ticks",          [])
            data.setdefault("pred_log",       [])
            data.setdefault("week_stats",     {})
            data.setdefault("signal_weights", {"baseline": 1.5, "slot": 1.2, "momentum": 0.6})
            data.setdefault("bias",           0.0)
            data.setdefault("slot_errors",    {})
            data.setdefault("seed_ts",        None)
            data.setdefault("signal_streak",  {"signal": None, "count": 0})
            data.setdefault("drift_warned",   {})
            data.setdefault("trend_state",    {})
            return data
    except Exception as e:
        log.warning(f"[{game['name']}] Redis load failed: {e} — starting fresh.")
    return {
        "slots": {}, "trend": 0.0, "last_daily": None,
        "ath": game["ath_floor"], "ath_ts": None,
        "session": {}, "ticks": [], "pred_log": [], "week_stats": {},
        "signal_weights": {"baseline": 1.5, "slot": 1.2, "momentum": 0.6},
        "bias": 0.0, "slot_errors": {},
        "seed_ts": None, "signal_streak": {"signal": None, "count": 0},
        "drift_warned": {}, "trend_state": {},
    }

def save_data(game, data):
    redis_set(game["redis_key"], json.dumps(data, separators=(",", ":")))

def autoseed_from_ticks(game, data):
    """
    If slot data is sparse, replay stored ticks to rebuild slot stats automatically.
    No manual seed_redis.py needed — the ticker seeds itself from its own Redis history.
    Skips if slots already have enough data so live stats are not overwritten.
    Called once per game on startup (restore_state) and at the start of each tick
    if slots are still thin.
    """
    ticks = data.get("ticks", [])
    if len(ticks) < AUTOSEED_MIN_TICKS:
        return False  # not enough history yet

    populated = [s for s in data["slots"].values() if s.get("n", 0) > 0]
    if populated:
        avg_n = sum(s["n"] for s in populated) / len(populated)
        if avg_n >= AUTOSEED_MIN_SLOT_N:
            return False  # already well-populated

    log.info(f"[{game['name']}] Auto-seeding slots from {len(ticks)} stored ticks...")
    for t in ticks:
        try:
            t_dt = datetime.strptime(t["ts"], "%Y-%m-%dT%H:%M")
        except ValueError:
            continue
        key  = f"{t_dt.weekday()}_{t_dt.hour}"
        ccu_t = t["ccu"]
        slot = data["slots"].setdefault(key, {
            "avg": ccu_t, "std": 0.0, "cv": 0.0, "n": 0, "_vals": []
        })
        vals = slot.get("_vals") or []
        vals.append(ccu_t)
        vals = vals[-30:]
        slot["_vals"] = vals
        slot["n"]     = len(vals)
        slot["avg"]   = sum(vals) / len(vals)
        slot["std"]   = statistics.stdev(vals) if len(vals) > 1 else 0.0
        slot["cv"]    = (slot["std"] / slot["avg"] * 100) if slot["avg"] > 0 else 0.0
        data["slots"][key] = slot

    data["seed_ts"] = ticks[0]["ts"]
    n_slots = len(data["slots"])
    log.info(f"[{game['name']}] Auto-seed complete — {len(ticks)} ticks → {n_slots} slots. seed_ts={data['seed_ts']}")
    return True

def record_snapshot(data, game, dow, hour, ccu, now_est):
    key  = f"{dow}_{hour}"
    slot = data["slots"].setdefault(key, {"avg": ccu, "std": 0.0, "cv": 0.0, "n": 0, "_vals": []})
    vals = slot.get("_vals")
    if not vals:
        seed_n = min(slot.get("n", 0), 30)
        vals   = [round(slot["avg"])] * seed_n if seed_n > 0 else []
    vals.append(ccu)
    vals = vals[-30:]
    slot["_vals"] = vals
    slot["n"]     = len(vals)
    slot["avg"]   = sum(vals) / len(vals)
    slot["std"]   = statistics.stdev(vals) if len(vals) > 1 else 0.0
    slot["cv"]    = (slot["std"] / slot["avg"] * 100) if slot["avg"] > 0 else 0.0
    data["slots"][key] = slot

    is_new_ath = ccu > data["ath"]
    if is_new_ath:
        data["ath"]    = ccu
        data["ath_ts"] = now_est.isoformat()

    tick_ts = now_est.strftime("%Y-%m-%dT%H:%M")
    if not data["ticks"] or data["ticks"][-1]["ts"] != tick_ts:
        data["ticks"].append({"ts": tick_ts, "ccu": ccu})
        data["ticks"] = data["ticks"][-2016:]

    week_key = now_est.strftime("%Y-W%W")
    ws = data["week_stats"].setdefault(week_key, {
        "high": ccu, "low": ccu, "sum": 0, "n": 0
    })
    ws["high"] = max(ws["high"], ccu)
    ws["low"]  = min(ws["low"],  ccu)
    ws["sum"] += ccu
    ws["n"]   += 1
    for old in sorted(data["week_stats"].keys())[:-2]:
        del data["week_stats"][old]

    return slot["avg"], data["ath"], is_new_ath

# ── Prediction log & scoring ──────────────────────────────────────────────────

def record_prediction(data, now_est, predicted_ccu, confidence, dow, hour, signals_used):
    if predicted_ccu is None:
        return
    data["pred_log"].append({
        "ts":           now_est.strftime("%Y-%m-%dT%H:%M"),
        "predicted":    predicted_ccu,
        "confidence":   confidence,
        "actual":       None,
        "dow_hour":     f"{dow}_{hour}",
        "signals_used": signals_used,
    })
    data["pred_log"] = data["pred_log"][-200:]

def score_last_prediction(data, ccu, now_dow, now_hour):
    scored_entry = None
    for entry in reversed(data["pred_log"]):
        if entry["actual"] is None:
            entry["actual"] = ccu
            scored_entry    = entry
            break

    if scored_entry is None:
        return None

    error     = scored_entry["predicted"] - ccu
    abs_error = abs(error)
    dh_key    = scored_entry.get("dow_hour", f"{now_dow}_{now_hour}")

    se = data["slot_errors"].setdefault(dh_key, {"mae": abs_error, "n": 1, "_errs": [abs_error]})
    errs = se.get("_errs", [se["mae"]])
    errs.append(abs_error)
    errs = errs[-20:]
    se["_errs"] = errs
    se["n"]     = len(errs)
    se["mae"]   = sum(errs) / len(errs)
    data["slot_errors"][dh_key] = se

    alpha        = 0.15
    data["bias"] = data["bias"] * (1 - alpha) + error * alpha

    signals_used = scored_entry.get("signals_used", [])
    weights      = data["signal_weights"]
    pct_error    = abs_error / max(ccu, 1) * 100

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

    data["signal_weights"] = weights

    log.info(f"Scored: pred={scored_entry['predicted']:,} actual={ccu:,} "
             f"err={error:+,} bias={data['bias']:+.1f} weights={weights}")
    return scored_entry

def prediction_accuracy(data):
    scored = [e for e in data["pred_log"] if e["actual"] is not None]
    if len(scored) < 3:
        return None, None, None, 0

    errors = [abs(e["predicted"] - e["actual"]) for e in scored]
    mae    = sum(errors) / len(errors)

    hits = total = 0
    for i in range(1, len(scored)):
        prev = scored[i-1]["actual"]
        curr = scored[i]["actual"]
        pred = scored[i]["predicted"]
        if prev is None or curr is None: continue
        if (curr - prev) * (pred - prev) > 0: hits += 1
        total += 1
    directional = (hits / total * 100) if total > 0 else None

    from collections import defaultdict
    by_conf = defaultdict(list)
    for e in scored:
        by_conf[e.get("confidence", "—")].append(abs(e["predicted"] - e["actual"]))
    calibration = {k: round(sum(v) / len(v), 1) for k, v in by_conf.items() if v}

    return mae, directional, calibration, len(scored)

def slot_confidence(data, dh_key, cv):
    se = data["slot_errors"].get(dh_key)
    if se and se["n"] >= 5:
        slot = data["slots"].get(dh_key)
        if slot and slot["avg"] > 0:
            obs_cv = se["mae"] / slot["avg"] * 100
            if   obs_cv < 8:  return "High"
            elif obs_cv < 18: return "Medium"
            else:             return "Low"
    if   cv < 12: return "High"
    elif cv < 22: return "Medium"
    else:         return "Low"

# ── Drift detection ───────────────────────────────────────────────────────────

def check_slot_drift(game, data, dow, hour, now_date):
    dh_key = f"{dow}_{hour}"
    slot   = data["slots"].get(dh_key)
    if not slot or slot["n"] < config.SIGNAL_MIN_SAMPLES:
        return

    ticks          = data.get("ticks", [])
    recent_actuals = []
    for t in ticks:
        try:
            t_dt = datetime.strptime(t["ts"], "%Y-%m-%dT%H:%M")
        except ValueError:
            continue
        if t_dt.weekday() == dow and t_dt.hour == hour:
            recent_actuals.append(t["ccu"])
    recent_actuals = recent_actuals[-DRIFT_MIN_TICKS:]

    if len(recent_actuals) < DRIFT_MIN_TICKS:
        return

    recent_avg = sum(recent_actuals) / len(recent_actuals)
    drift_pct  = (slot["avg"] - recent_avg) / max(recent_avg, 1) * 100

    if abs(drift_pct) < DRIFT_THRESHOLD_PCT:
        return

    warned = data.setdefault("drift_warned", {})
    last_w = warned.get(dh_key)
    if last_w:
        try:
            days_since = (now_date - datetime.fromisoformat(last_w).date()).days
            if days_since < 7:
                return
        except (ValueError, TypeError):
            pass

    direction  = "above" if drift_pct > 0 else "below"
    slot_label = f"{DAYS[dow]} {hour:02d}:00"

    msg = (
        f"Slot avg is {abs(drift_pct):.0f}% {direction} recent actuals.\n"
        f"\n"
        f"Slot:          {slot_label}  (n={slot['n']})\n"
        f"Stored avg:    {int(slot['avg']):,}\n"
        f"Recent actual: {int(recent_avg):,}  (last {len(recent_actuals)} ticks)\n"
        f"\n"
        + ("Predictions skewing high — false LONGs likely.\n" if drift_pct > 0
           else "Predictions skewing low — false SHORTs likely.\n")
        + f"Consider reseeding: `python3 seed_redis.py`"
    )
    notify(game, f"📊 {game['name']} — Slot Drift: {slot_label}", msg, {"signal": "HOLD"})
    warned[dh_key]       = now_date.isoformat()
    data["drift_warned"] = warned
    log.warning(f"[{game['name']}] Drift: {slot_label} avg={slot['avg']:.0f} "
                f"recent={recent_avg:.0f} drift={drift_pct:+.1f}%")

# ── Signal streak tracker ─────────────────────────────────────────────────────

def update_streak(data, signal):
    streak = data.setdefault("signal_streak", {"signal": None, "count": 0})
    if streak["signal"] == signal and signal not in ("HOLD", "INSUFFICIENT DATA"):
        streak["count"] += 1
    elif signal not in ("HOLD", "INSUFFICIENT DATA"):
        streak["signal"] = signal
        streak["count"]  = 1
    else:
        streak["signal"] = None
        streak["count"]  = 0
    data["signal_streak"] = streak
    return streak["signal"], streak["count"]

# ── Time-to-peak ──────────────────────────────────────────────────────────────

def time_to_peak(data, now_est):
    if not data["slots"]:
        return None
    dow      = now_est.weekday()
    now_mins = now_est.hour * 60 + now_est.minute

    curr_slot = data["slots"].get(f"{dow}_{now_est.hour}")
    curr_avg  = curr_slot["avg"] if curr_slot else 0

    best_avg  = -1
    best_hour = -1
    for h in range(now_est.hour + 1, 24):
        slot = data["slots"].get(f"{dow}_{h}")
        if slot and slot["avg"] > best_avg:
            best_avg  = slot["avg"]
            best_hour = h

    if best_hour == -1:
        return None

    if curr_avg > 0 and (best_avg - curr_avg) / curr_avg * 100 < PEAK_MIN_UPLIFT_PCT:
        return None

    delta_m   = best_hour * 60 - now_mins
    hrs, mins = divmod(delta_m, 60)
    peak_fmt  = now_est.replace(hour=best_hour, minute=0).strftime("%-I:%M %p")
    if hrs > 0 and mins:
        eta = f"~{hrs}h {mins}m"
    elif hrs:
        eta = f"~{hrs}h"
    else:
        eta = f"~{mins}m"

    return f"Peak in {eta}  ({DAYS[dow]} {peak_fmt}  avg {int(best_avg):,})"

# ── Reseed reminder ───────────────────────────────────────────────────────────

RESEED_DAYS = 45

def check_reseed_reminder(game, data):
    seed_ts = data.get("seed_ts")
    if not seed_ts:
        return
    try:
        seeded_on = datetime.fromisoformat(seed_ts).date()
    except (ValueError, TypeError):
        return

    now_date = datetime.now(timezone.utc).date()
    age_days = (now_date - seeded_on).days
    if age_days < RESEED_DAYS:
        return

    last_warned = data.get("reseed_warned")
    if last_warned:
        try:
            if (now_date - datetime.fromisoformat(last_warned).date()).days < 30:
                return
        except (ValueError, TypeError):
            pass

    msg = (
        f"Seed data is {age_days} days old — slot averages may be drifting.\n"
        f"\n"
        f"Download fresh CSVs from Rolimons and run:\n"
        f"`python3 seed_redis.py`\n"
        f"then redeploy."
    )
    notify(game, f"⚠️ {game['name']} — Time to Reseed", msg, {"signal": "HOLD"})
    data["reseed_warned"] = now_date.isoformat()
    log.info(f"[{game['name']}] Reseed reminder sent (age={age_days}d)")

# ── Session state ─────────────────────────────────────────────────────────────

class GameState:
    def __init__(self):
        self.last_tick      = None
        self.midnight_ccu   = None
        self.last_date      = None
        self.intraday_high  = 0
        self.intraday_low   = None
        self.intraday_sum   = 0
        self.intraday_ticks = 0

_game_states = {}
def get_state(game):
    k = game["redis_key"]
    if k not in _game_states:
        _game_states[k] = GameState()
    return _game_states[k]

def restore_state(game, data):
    state     = get_state(game)
    s         = data.get("session", {})
    now_est   = datetime.now(timezone.utc) + timedelta(hours=config.TIMEZONE_OFFSET)
    today_str = now_est.date().isoformat()

    state.midnight_ccu = s.get("midnight_ccu")
    state.last_tick    = s.get("last_tick")

    if s.get("last_date") == today_str:
        state.last_date      = now_est.date()
        state.intraday_high  = s.get("intraday_high", 0)
        raw_low              = s.get("intraday_low")
        state.intraday_low   = raw_low if raw_low else None
        state.intraday_sum   = s.get("intraday_sum",   0)
        state.intraday_ticks = s.get("intraday_ticks", 0)
        prev = f"{state.last_tick:,}" if state.last_tick else "none"
        log.info(f"[{game['name']}] Resumed — last CCU: {prev}")
    else:
        state.intraday_high  = 0
        state.intraday_low   = None
        state.intraday_sum   = 0
        state.intraday_ticks = 0
        log.info(f"[{game['name']}] New day — intraday reset (last_tick kept)")

    # Auto-seed slots from tick history if they're sparse (runs on startup)
    if autoseed_from_ticks(game, data):
        save_data(game, data)

def persist_state(game, data, now_est):
    state = get_state(game)
    data["session"] = {
        "last_tick":      state.last_tick,
        "midnight_ccu":   state.midnight_ccu,
        "last_date":      now_est.date().isoformat(),
        "intraday_high":  state.intraday_high,
        "intraday_low":   state.intraday_low,
        "intraday_sum":   state.intraday_sum,
        "intraday_ticks": state.intraday_ticks,
    }

# ── Helpers ───────────────────────────────────────────────────────────────────

def diff_label(current, historical):
    if historical is None or historical == 0:
        return 0.0, "—"
    diff = current - historical
    pct  = diff / historical * 100
    sign = "+" if diff >= 0 else ""
    return pct, f"{sign}{diff:,} ({sign}{pct:.1f}%)"

def get_slot(data, dow, hour):
    return data["slots"].get(f"{dow}_{hour % 24}")

# ── Signal engine ─────────────────────────────────────────────────────────────

SIGNAL_COLORS = {
    "LONG": 0x26A69A, "SHORT": 0xEF5350,
    "HOLD": 0xF4C430, "INSUFFICIENT DATA": 0x888888,
}
SIGNAL_EMOJI = {
    "LONG": "🟢", "SHORT": "🔴", "HOLD": "🟡", "INSUFFICIENT DATA": "⚪"
}

def trend_adjusted_avg(slot, trend_per_day, last_daily_ccu, current_ccu):
    if slot is None or last_daily_ccu is None or last_daily_ccu == 0:
        return slot["avg"] if slot else None
    ratio     = current_ccu / last_daily_ccu
    raw_avg   = slot["avg"]
    trend_adj = raw_avg * ratio
    return raw_avg * 0.5 + trend_adj * 0.5

def compute_signal(data, ccu, dow, hour):
    curr_slot  = get_slot(data, dow, hour)
    next_slot  = get_slot(data, dow, hour + 1)
    next2_slot = get_slot(data, dow, hour + 2)
    dh_key     = f"{dow}_{hour}"

    if curr_slot is None or curr_slot["n"] < config.SIGNAL_MIN_SAMPLES:
        return {
            "signal": "INSUFFICIENT DATA", "confidence": "—",
            "reasoning": f"Need {config.SIGNAL_MIN_SAMPLES}+ samples for this slot.",
            "curr_avg": None, "next1_avg": None, "next2_avg": None,
            "cv": None, "std": None,
        }

    curr_avg  = trend_adjusted_avg(curr_slot,  data["trend"], data["last_daily"], ccu)
    next1_avg = trend_adjusted_avg(next_slot,  data["trend"], data["last_daily"], ccu) if next_slot  else None
    next2_avg = trend_adjusted_avg(next2_slot, data["trend"], data["last_daily"], ccu) if next2_slot else None
    cv        = curr_slot["cv"]
    pct       = (ccu - curr_avg) / curr_avg * 100

    confidence = slot_confidence(data, dh_key, cv)
    if cv > CV_NOISE_CEILING:
        confidence = "Low"

    trend_up = trend_down = False
    trend_str = "trend unclear"
    if next1_avg is not None:
        d1 = (next1_avg - curr_avg) / curr_avg * 100
        if next2_avg is not None:
            d2 = (next2_avg - curr_avg) / curr_avg * 100
            if   d1 >  config.SIGNAL_TREND_PCT and d2 >  config.SIGNAL_TREND_PCT:
                trend_up   = True; trend_str = f"avg rises +{d1:.1f}% then +{d2:.1f}% over 2h"
            elif d1 < -config.SIGNAL_TREND_PCT and d2 < -config.SIGNAL_TREND_PCT:
                trend_down = True; trend_str = f"avg falls {d1:.1f}% then {d2:.1f}% over 2h"
            else:
                trend_str = f"mixed next 2h ({d1:+.1f}%, {d2:+.1f}%)"
        else:
            if   d1 >  config.SIGNAL_TREND_PCT: trend_up   = True; trend_str = f"avg rises +{d1:.1f}% next hr"
            elif d1 < -config.SIGNAL_TREND_PCT: trend_down = True; trend_str = f"avg falls {d1:.1f}% next hr"
            else: trend_str = f"flat next hr ({d1:+.1f}%)"

    extreme_decline = pct < -45

    if   pct < -config.SIGNAL_LONG_PCT and trend_up and not extreme_decline:
        signal = "LONG";  reasoning = f"{pct:.1f}% below adj. avg — {trend_str} — recovery expected"
    elif pct >  config.SIGNAL_SHORT_PCT and trend_down:
        signal = "SHORT"; reasoning = f"+{pct:.1f}% above adj. avg — {trend_str} — pullback expected"
    elif pct < -config.SIGNAL_LONG_PCT:
        signal = "HOLD";  reasoning = f"{pct:.1f}% below avg but {trend_str}"
    elif pct >  config.SIGNAL_SHORT_PCT:
        signal = "HOLD";  reasoning = f"+{pct:.1f}% above avg but {trend_str}"
    else:
        signal = "HOLD";  reasoning = f"{pct:+.1f}% vs adj. avg — {trend_str}"

    if cv > CV_NOISE_CEILING and signal in ("LONG", "SHORT"):
        log.info(f"[signal suppressed] {signal} → HOLD (cv={cv:.1f}% > {CV_NOISE_CEILING}%)")
        signal    = "HOLD"
        reasoning = f"Signal suppressed — cv {cv:.0f}% exceeds noise threshold ({CV_NOISE_CEILING:.0f}%)"

    return {
        "signal": signal, "confidence": confidence, "reasoning": reasoning,
        "curr_avg": curr_avg, "next1_avg": next1_avg, "next2_avg": next2_avg,
        "cv": cv, "std": curr_slot["std"],
    }

# ── Next-tick predictor ───────────────────────────────────────────────────────

def predict_next_tick(data, ccu, now_est, dow):
    hour   = now_est.hour
    minute = now_est.minute
    ticks  = data.get("ticks", [])
    dh_key = f"{dow}_{hour}"

    weights_cfg = data.get("signal_weights", {"baseline": 1.5, "slot": 1.2, "momentum": 0.6})

    curr_slot = get_slot(data, dow, hour)
    next_slot = get_slot(data, dow, hour + 1)
    baseline_delta = None
    baseline_cv    = None
    baseline_std   = None
    if curr_slot and next_slot and curr_slot["n"] >= config.SIGNAL_MIN_SAMPLES:
        curr_adj       = trend_adjusted_avg(curr_slot, data["trend"], data["last_daily"], ccu)
        next_adj       = trend_adjusted_avg(next_slot, data["trend"], data["last_daily"], ccu)
        baseline_delta = (next_adj - curr_adj) * (15 / 60)
        baseline_cv    = curr_slot["cv"]
        baseline_std   = curr_slot["std"]

    momentum_delta = None
    if len(ticks) >= 3:
        recent = ticks[-3:]
        deltas = [recent[i+1]["ccu"] - recent[i]["ccu"] for i in range(len(recent)-1)]
        momentum_delta = deltas[0] * 0.4 + deltas[1] * 0.6

    slot_deltas_list = []
    for i in range(len(ticks) - 1):
        try:
            t0 = datetime.strptime(ticks[i]["ts"],   "%Y-%m-%dT%H:%M")
            t1 = datetime.strptime(ticks[i+1]["ts"], "%Y-%m-%dT%H:%M")
        except ValueError:
            continue
        diff_mins = (t1 - t0).seconds // 60
        if (t0.weekday() == dow and t0.hour == hour
                and t0.minute == minute and diff_mins in (10, 15)):
            slot_deltas_list.append(ticks[i+1]["ccu"] - ticks[i]["ccu"])
    slot_delta = sum(slot_deltas_list) / len(slot_deltas_list) if slot_deltas_list else None

    signals      = []
    weights      = []
    signals_used = []

    if baseline_delta is not None:
        cv_factor = max(0.1, 1.0 - (baseline_cv or 50) / 100)
        signals.append(baseline_delta)
        weights.append(weights_cfg["baseline"] * cv_factor)
        signals_used.append("baseline")

    if slot_delta is not None:
        n_factor = min(1.0, len(slot_deltas_list) / 8)
        signals.append(slot_delta)
        weights.append(weights_cfg["slot"] * n_factor)
        signals_used.append("slot")

    if momentum_delta is not None:
        signals.append(momentum_delta)
        weights.append(weights_cfg["momentum"])
        signals_used.append("momentum")

    if not signals:
        return {"predicted_ccu": None, "delta": None, "delta_pct": None,
                "confidence": "—", "detail": "insufficient data",
                "std": None, "signals_used": []}

    total_w    = sum(weights)
    delta      = sum(s * w for s, w in zip(signals, weights)) / total_w
    bias       = data.get("bias", 0.0)
    delta_corr = delta - bias
    predicted  = round(ccu + delta_corr)
    delta_int  = predicted - ccu
    delta_pct  = delta_corr / ccu * 100 if ccu else 0

    se = data["slot_errors"].get(dh_key)
    if se and se["n"] >= 5:
        obs_cv = se["mae"] / max(ccu, 1) * 100
        if   obs_cv < 8:  confidence = "High"
        elif obs_cv < 18: confidence = "Medium"
        else:             confidence = "Low"
    else:
        n_signals = len(signals)
        if n_signals == 3 and baseline_cv is not None and baseline_cv < 15:
            confidence = "High"
        elif n_signals >= 2:
            confidence = "Medium"
        else:
            confidence = "Low"
    if (baseline_cv or 100) > CV_NOISE_CEILING and confidence != "Low":
        confidence = "Low"

    parts = []
    if baseline_delta is not None: parts.append(f"baseline {baseline_delta:+.0f}")
    if slot_delta     is not None: parts.append(f"slot {slot_delta:+.0f} (n={len(slot_deltas_list)})")
    if momentum_delta is not None: parts.append(f"momentum {momentum_delta:+.0f}")
    if abs(bias) > 10:             parts.append(f"bias {-bias:+.0f}")

    return {
        "predicted_ccu": predicted,
        "delta":         delta_int,
        "delta_pct":     delta_pct,
        "confidence":    confidence,
        "detail":        " | ".join(parts),
        "std":           baseline_std,
        "signals_used":  signals_used,
    }

# ── Event detection ───────────────────────────────────────────────────────────

def detect_event(data, ccu, dow, hour, pct_15m):
    slot = get_slot(data, dow, hour)
    if slot is None or slot["avg"] == 0:
        return None
    ratio = ccu / slot["avg"]
    if ratio >= 2.0 and pct_15m > 30:
        return f"🚨 POSSIBLE EVENT  ({ratio:.1f}x normal)"
    return None

# ── Discord ───────────────────────────────────────────────────────────────────

def send_discord(game, title, message, sig, retries=3):
    webhook = config.DISCORD_WEBHOOK
    if not webhook:
        return
    color = sig.get("color_override") if sig.get("color_override") else SIGNAL_COLORS.get(sig["signal"], 0x888888)
    payload = json.dumps({"embeds": [{"title": title, "description": message,
                                      "color": color,
                                      "footer": {"text": f"{game['name']}  •  EST"}}]})
    for attempt in range(1, retries+1):
        try:
            r = http.post(webhook, data=payload,
                          headers={"Content-Type": "application/json"}, timeout=10)
            if r.status_code == 429:
                time.sleep(r.json().get("retry_after", 2)); continue
            r.raise_for_status(); return
        except requests.RequestException as e:
            if attempt == retries:
                log.error(f"[{game['name']}] Discord failed: {e}"); return
            time.sleep(2**attempt)

def send_spacer():
    webhook = config.DISCORD_WEBHOOK
    if not webhook:
        return
    payload = json.dumps({"content": "\u200b"})
    try:
        r = http.post(webhook, data=payload,
                      headers={"Content-Type": "application/json"}, timeout=10)
        if r.status_code == 429:
            time.sleep(r.json().get("retry_after", 2))
            http.post(webhook, data=payload,
                      headers={"Content-Type": "application/json"}, timeout=10)
        r.raise_for_status()
    except requests.RequestException as e:
        log.warning(f"Spacer failed: {e}")

def notify(game, title, message, sig):
    send_discord(game, title, message, sig)

# ── Roblox API ────────────────────────────────────────────────────────────────

def fetch_ccu(game, retries=3):
    url = f"https://games.roblox.com/v1/games?universeIds={game['universe_id']}"
    for attempt in range(1, retries+1):
        try:
            r = http.get(url, timeout=10)
            r.raise_for_status()
            return r.json()["data"][0]["playing"]
        except (requests.RequestException, KeyError, IndexError) as e:
            if attempt == retries:
                log.error(f"[{game['name']}] Roblox API failed: {e}"); return None
            time.sleep(2**attempt)
    return None

# ── Trend engine ──────────────────────────────────────────────────────────────

def _trend_cooldown_ok(data, key, hours):
    ts_store = data.get("trend_state", {}).get(key)
    if not ts_store:
        return True
    try:
        last = datetime.fromisoformat(ts_store)
        return (datetime.now(timezone.utc) - last).total_seconds() > hours * 3600
    except (ValueError, TypeError):
        return True

def _trend_mark(data, key, now_est):
    data.setdefault("trend_state", {})[key] = now_est.isoformat()

def _dow_avgs(data):
    """Average CCU per day-of-week, using all slots for that DOW."""
    result = {}
    for d in range(7):
        vals = [s["avg"] for k, s in data["slots"].items()
                if k.startswith(f"{d}_") and s.get("n", 0) >= 3]
        result[d] = sum(vals) / len(vals) if vals else None
    return result

def _rolling_weekly_avgs(ticks, weeks):
    from collections import defaultdict
    by_week = defaultdict(list)
    for t in ticks:
        try:
            dt = datetime.strptime(t["ts"], "%Y-%m-%dT%H:%M")
        except ValueError:
            continue
        iso = dt.isocalendar()
        by_week[f"{iso[0]}-W{iso[1]:02d}"].append(t["ccu"])
    keys = sorted(by_week)[-weeks:]
    return [sum(by_week[k]) / len(by_week[k]) for k in keys if by_week[k]]

def run_trend_analysis(game, data, ccu, dow, hour, now_est):
    """
    Detect structural patterns in a game's stored tick data and fire Discord
    alerts for any that are new or changed. Works for any game — game-agnostic.

    Patterns detected:
      DOW_SPIKE        — one day consistently far above its neighbors
      REVERSAL         — at structural trough with spike day approaching
      PEAK_BREAKDOWN   — structural spike day running significantly below its own avg
      DECAY_ACCEL      — same DOW declining week-over-week beyond normal noise
      MACRO_TREND      — 4-week vs 8-week rolling avg crossover (growth/decline)
      ANOMALY_HIGH/LOW — current CCU is an extreme z-score outlier for this slot
      PEAK_HOUR        — historical peak hour for today's DOW is <3h away
      MOMENTUM         — 3 consecutive same-direction ticks with above-avg magnitude
    """
    # Need enough slot history before running anything
    if sum(1 for s in data["slots"].values() if s.get("n", 0) >= 4) < 6:
        return

    cooldown_h  = game.get("trend_alert_cooldown_h", 6)
    spike_ratio = game.get("trend_spike_ratio", 1.8)
    anomaly_sig = game.get("trend_anomaly_sigma", 2.5)
    dow_avgs    = _dow_avgs(data)
    valid_avgs  = {d: v for d, v in dow_avgs.items() if v}
    ticks       = data.get("ticks", [])

    def _fire(title, body, color):
        notify(game, title, body, {"signal": "HOLD", "color_override": color})

    # ── 1. DOW spike day ──────────────────────────────────────────────────────
    if len(valid_avgs) >= 5:
        for d, avg in valid_avgs.items():
            neighbors = [valid_avgs.get((d-1)%7), valid_avgs.get((d+1)%7)]
            neighbors = [n for n in neighbors if n]
            if not neighbors:
                continue
            ratio = avg / (sum(neighbors) / len(neighbors))
            if ratio >= spike_ratio and dow in (d, (d-1)%7):
                key = f"dow_spike_{d}"
                if _trend_cooldown_ok(data, key, cooldown_h * 24 * 3):
                    _trend_mark(data, key, now_est)
                    prev_avg = valid_avgs.get((d-1)%7, 0)
                    _fire(
                        f"📈 {game['name']} — Structural Spike: {DAYS[d]}",
                        f"{DAYS[d]} is a confirmed structural spike day.\n\n"
                        f"Avg CCU:    {int(avg):,}\n"
                        f"vs {DAYS[(d-1)%7]}:      {int(prev_avg):,}  ({ratio:.1f}× ratio)\n\n"
                        f"Signal: LONG — {DAYS[d]} CCU historically {ratio:.1f}× above neighbors.",
                        0xF9C74F,
                    )
                    log.info(f"[{game['name']}] Trend: DOW_SPIKE {DAYS[d]} ({ratio:.1f}×)")

    # ── 2. Reversal setup ─────────────────────────────────────────────────────
    if len(valid_avgs) >= 5:
        trough_d = min(valid_avgs, key=valid_avgs.get)
        peak_d   = max(valid_avgs, key=valid_avgs.get)
        days_to_peak = (peak_d - dow) % 7
        ratio = valid_avgs[peak_d] / valid_avgs[trough_d] if valid_avgs[trough_d] else 0
        if (dow == trough_d or valid_avgs.get(dow, 0) / valid_avgs[trough_d] <= 1.15) \
                and 0 < days_to_peak <= 3 and ratio >= 1.5:
            key = f"reversal_{trough_d}"
            if _trend_cooldown_ok(data, key, cooldown_h * 18):
                _trend_mark(data, key, now_est)
                _fire(
                    f"🔄 {game['name']} — Reversal Setup",
                    f"At structural weekly LOW ({DAYS[trough_d]} avg {int(valid_avgs[trough_d]):,}).\n"
                    f"Peak day ({DAYS[peak_d]}) is {days_to_peak} day(s) away — avg {int(valid_avgs[peak_d]):,}.\n\n"
                    f"Live CCU:    {ccu:,}\n"
                    f"Expected ×:  {ratio:.1f}×\n\n"
                    f"Signal: LONG — buy the trough, ride to {DAYS[peak_d]}.",
                    0x43E97B,
                )
                log.info(f"[{game['name']}] Trend: REVERSAL trough={DAYS[trough_d]} peak={DAYS[peak_d]}")

    # ── 3. Peak day breakdown ─────────────────────────────────────────────────
    if len(valid_avgs) >= 5:
        peak_d = max(valid_avgs, key=valid_avgs.get)
        if dow == peak_d:
            slot = data["slots"].get(f"{dow}_{hour}")
            if slot and slot["n"] >= 5:
                pct_vs_avg = (ccu - slot["avg"]) / slot["avg"] * 100
                if pct_vs_avg < -25:
                    key = f"peak_breakdown_{dow}"
                    if _trend_cooldown_ok(data, key, cooldown_h):
                        _trend_mark(data, key, now_est)
                        severity = "SEVERE" if pct_vs_avg < -40 else "MODERATE"
                        _fire(
                            f"⚠️ {game['name']} — {severity} Peak Breakdown ({DAYS[peak_d]})",
                            f"{DAYS[peak_d]} is the spike day but CCU is {abs(pct_vs_avg):.0f}% below its own avg.\n\n"
                            f"Live CCU:    {ccu:,}\n"
                            f"Slot avg:    {int(slot['avg']):,}\n"
                            f"Deviation:   {pct_vs_avg:+.0f}%\n\n"
                            f"Signal: {'SHORT — structural support failing.' if pct_vs_avg < -40 else 'HOLD — watch for recovery.'}",
                            0xEF233C if severity == "SEVERE" else 0xFF6B35,
                        )
                        log.info(f"[{game['name']}] Trend: PEAK_BREAKDOWN {pct_vs_avg:+.0f}%")

    # ── 4. Decay acceleration ─────────────────────────────────────────────────
    if len(ticks) >= 40:
        dow_vals = [t["ccu"] for t in ticks
                    if datetime.strptime(t["ts"], "%Y-%m-%dT%H:%M").weekday() == dow
                    if True][:0]  # placeholder — collect properly below
        dow_vals = []
        for t in ticks:
            try:
                if datetime.strptime(t["ts"], "%Y-%m-%dT%H:%M").weekday() == dow:
                    dow_vals.append(t["ccu"])
            except ValueError:
                pass
        if len(dow_vals) >= 4:
            recent_avg = sum(dow_vals[-2:]) / 2
            prior_avg  = sum(dow_vals[-4:-2]) / 2
            if prior_avg > 0:
                decay_pct = (recent_avg - prior_avg) / prior_avg * 100
                if decay_pct < -15:
                    key = f"decay_accel_{dow}"
                    if _trend_cooldown_ok(data, key, cooldown_h * 4):
                        _trend_mark(data, key, now_est)
                        _fire(
                            f"📉 {game['name']} — Decay Accelerating ({DAYS[dow]})",
                            f"{DAYS[dow]} CCU declining faster than normal week-over-week.\n\n"
                            f"Recent 2-week avg:  {int(recent_avg):,}\n"
                            f"Prior 2-week avg:   {int(prior_avg):,}\n"
                            f"Δ:                  {decay_pct:+.1f}%\n\n"
                            f"Signal: BEARISH — weekly floor eroding.",
                            0xEF233C,
                        )
                        log.info(f"[{game['name']}] Trend: DECAY_ACCEL {DAYS[dow]} {decay_pct:+.1f}%")

    # ── 5. Macro trend ────────────────────────────────────────────────────────
    avgs_8 = _rolling_weekly_avgs(ticks, 8)
    avgs_4 = _rolling_weekly_avgs(ticks, 4)
    if len(avgs_8) >= 4 and len(avgs_4) >= 2:
        slow = sum(avgs_8[-4:]) / 4
        fast = sum(avgs_4[-2:]) / 2
        if slow > 0:
            pct = (fast - slow) / slow * 100
            prev_dir = data.get("trend_state", {}).get("macro_direction", "FLAT")
            if abs(pct) >= 10:
                direction    = "UP" if pct > 0 else "DOWN"
                is_reversal  = (direction == "UP" and prev_dir != "UP") or \
                               (direction == "DOWN" and prev_dir != "DOWN")
                key = "macro_trend"
                if _trend_cooldown_ok(data, key, cooldown_h * 4 * 24):
                    _trend_mark(data, key, now_est)
                    data.setdefault("trend_state", {})["macro_direction"] = direction
                    if is_reversal and direction == "UP":
                        t = f"📈 {game['name']} — Macro Reversal: RECOVERY"
                        b = f"4-week avg crossed ABOVE 8-week — macro uptrend starting.\n\n" \
                            f"4-week avg:  {int(fast):,}\n8-week avg:  {int(slow):,}\nΔ: {pct:+.1f}%\n\nSignal: LONG — macro tailwind."
                        c = 0x43E97B
                    elif is_reversal:
                        t = f"📉 {game['name']} — Macro Reversal: DECLINE"
                        b = f"4-week avg crossed BELOW 8-week — macro downtrend starting.\n\n" \
                            f"4-week avg:  {int(fast):,}\n8-week avg:  {int(slow):,}\nΔ: {pct:+.1f}%\n\nSignal: SHORT — macro headwind."
                        c = 0xEF233C
                    elif direction == "UP":
                        t = f"📈 {game['name']} — Macro Trend: UPTREND"
                        b = f"4-week avg remains above 8-week — uptrend intact.\n\n" \
                            f"4-week avg:  {int(fast):,}\n8-week avg:  {int(slow):,}\nΔ: {pct:+.1f}%"
                        c = 0x43E97B
                    else:
                        t = f"📉 {game['name']} — Macro Trend: DOWNTREND"
                        b = f"4-week avg remains below 8-week — downtrend intact.\n\n" \
                            f"4-week avg:  {int(fast):,}\n8-week avg:  {int(slow):,}\nΔ: {pct:+.1f}%"
                        c = 0xEF233C
                    _fire(t, b, c)
                    log.info(f"[{game['name']}] Trend: MACRO_{direction} {pct:+.1f}%")

    # ── 6. Anomaly (z-score) ──────────────────────────────────────────────────
    slot = data["slots"].get(f"{dow}_{hour}")
    if slot and slot["n"] >= 6 and slot.get("std", 0) > 0:
        z = (ccu - slot["avg"]) / slot["std"]
        if abs(z) >= anomaly_sig:
            direction = "HIGH" if z > 0 else "LOW"
            key = f"anomaly_{direction}"
            if _trend_cooldown_ok(data, key, cooldown_h):
                _trend_mark(data, key, now_est)
                if direction == "HIGH":
                    _fire(
                        f"🚨 {game['name']} — Anomaly: SPIKE (+{z:.1f}σ)",
                        f"CCU is {z:.1f}σ ABOVE slot average — possible event or viral moment.\n\n"
                        f"Live CCU:  {ccu:,}\nSlot avg:  {int(slot['avg']):,}\n"
                        f"Slot σ:    {int(slot['std']):,}\n\n"
                        f"Signal: LONG momentum — monitor for sustainability.",
                        0xFF6B35,
                    )
                else:
                    _fire(
                        f"🚨 {game['name']} — Anomaly: CRASH ({z:.1f}σ)",
                        f"CCU is {abs(z):.1f}σ BELOW slot average — possible outage or content issue.\n\n"
                        f"Live CCU:  {ccu:,}\nSlot avg:  {int(slot['avg']):,}\n"
                        f"Slot σ:    {int(slot['std']):,}\n\n"
                        f"Signal: SHORT — investigate before acting.",
                        0xEF233C,
                    )
                log.info(f"[{game['name']}] Trend: ANOMALY_{direction} z={z:.1f}")

    # ── 7. Peak hour incoming ─────────────────────────────────────────────────
    best_h, best_avg = -1, -1
    for h in range(24):
        s = data["slots"].get(f"{dow}_{h}")
        if s and s["avg"] > best_avg:
            best_avg = s["avg"]
            best_h   = h
    if best_h >= 0:
        hours_away = (best_h - hour) % 24
        if 0 < hours_away <= 3:
            curr_avg = (data["slots"].get(f"{dow}_{hour}") or {}).get("avg", 1)
            uplift   = (best_avg - curr_avg) / max(curr_avg, 1) * 100
            if uplift >= 15:
                key = f"peak_hour_{dow}"
                if _trend_cooldown_ok(data, key, 20):
                    _trend_mark(data, key, now_est)
                    _fire(
                        f"⏰ {game['name']} — Peak Hour in {hours_away}h ({DAYS[dow]} {best_h:02d}:00)",
                        f"Historical peak for {DAYS[dow]} is {best_h:02d}:00 — {hours_away}h away.\n\n"
                        f"Current avg ({hour:02d}:00):  {int(curr_avg):,}\n"
                        f"Peak avg    ({best_h:02d}:00):  {int(best_avg):,}\n"
                        f"Expected uplift:     +{uplift:.0f}%\n\n"
                        f"Signal: LONG before {best_h:02d}:00.",
                        0x90E0EF,
                    )
                    log.info(f"[{game['name']}] Trend: PEAK_HOUR {best_h:02d}:00 (+{uplift:.0f}%)")

    # ── 8. Intraday momentum ──────────────────────────────────────────────────
    if len(ticks) >= 4:
        recent_vals = [t["ccu"] for t in ticks[-4:]]
        deltas = [recent_vals[i+1] - recent_vals[i] for i in range(3)]
        slot   = data["slots"].get(f"{dow}_{hour}")
        if slot and slot.get("std", 0) > 0:
            all_up   = all(d > 0 for d in deltas)
            all_down = all(d < 0 for d in deltas)
            avg_mag  = sum(abs(d) for d in deltas) / 3
            if (all_up or all_down) and avg_mag >= slot["std"] * 0.5:
                direction = "UP" if all_up else "DOWN"
                key = f"momentum_{direction}"
                if _trend_cooldown_ok(data, key, cooldown_h * 0.5):
                    _trend_mark(data, key, now_est)
                    total_move = recent_vals[-1] - recent_vals[0]
                    pct_move   = total_move / max(recent_vals[0], 1) * 100
                    delta_str  = "  ".join(f"{d:+}" for d in deltas)
                    if direction == "UP":
                        _fire(
                            f"🚀 {game['name']} — Momentum: ACCELERATING",
                            f"3 consecutive up ticks above avg magnitude.\n\n"
                            f"Δ ticks:  {delta_str}\n"
                            f"Total:    {total_move:+,} ({pct_move:+.1f}%)\n\n"
                            f"Signal: SHORT-TERM LONG.",
                            0x43E97B,
                        )
                    else:
                        _fire(
                            f"📉 {game['name']} — Momentum: DECLINING",
                            f"3 consecutive down ticks above avg magnitude.\n\n"
                            f"Δ ticks:  {delta_str}\n"
                            f"Total:    {total_move:+,} ({pct_move:+.1f}%)\n\n"
                            f"Signal: SHORT-TERM SHORT.",
                            0xEF233C,
                        )
                    log.info(f"[{game['name']}] Trend: MOMENTUM_{direction} {total_move:+}")

# ── Weekly summary ────────────────────────────────────────────────────────────

def send_weekly_summary(game, data):
    now_est   = datetime.now(timezone.utc) + timedelta(hours=config.TIMEZONE_OFFSET)
    last_week = (now_est - timedelta(days=7)).strftime("%Y-W%W")
    ws        = data["week_stats"].get(last_week)
    if not ws or ws["n"] == 0:
        return

    avg = ws["sum"] // ws["n"]
    mae, dir_pct, calibration, n_pred = prediction_accuracy(data)
    w = data.get("signal_weights", {})

    acc_str  = f"\nPred  MAE {mae:.0f}  dir {dir_pct:.0f}%  n={n_pred}" if mae else ""
    w_str    = (f"\nWeights  base {w.get('baseline',0):.2f}  "
                f"slot {w.get('slot',0):.2f}  "
                f"mom {w.get('momentum',0):.2f}") if w else ""
    bias_str = f"\nBias correction  {-data.get('bias',0):+.0f} CCU" if abs(data.get("bias",0)) > 5 else ""

    cal_str = ""
    if calibration:
        parts = []
        for tier in ("High", "Medium", "Low"):
            if tier in calibration:
                parts.append(f"{tier} {calibration[tier]:.0f}")
        if parts:
            cal_str = "\nConf MAE  " + "  |  ".join(parts)
        h_mae = calibration.get("High")
        l_mae = calibration.get("Low")
        if h_mae and l_mae and h_mae >= l_mae * 0.9:
            cal_str += "\n⚠️ Conf tiers miscalibrated — thresholds may need tuning"

    msg = (f"↑ {ws['high']:,}  ↓ {ws['low']:,}  avg {avg:,}"
           f"{acc_str}{cal_str}{w_str}{bias_str}")
    notify(game, f"📊 {game['name']} — Weekly Summary", msg, {"signal": "HOLD"})
    log.info(f"[{game['name']}] Weekly summary sent.")

# ── Daily summary ─────────────────────────────────────────────────────────────

def send_daily_summary(game, data):
    state = get_state(game)
    if state.intraday_ticks == 0: return
    avg = state.intraday_sum // state.intraday_ticks
    low = state.intraday_low or 0
    msg = f"↑ {state.intraday_high:,}  ↓ {low:,}  avg {avg:,}"
    notify(game, f"📅 {game['name']} — Daily Summary", msg, {"signal": "HOLD"})
    log.info(f"[{game['name']}] Daily — high={state.intraday_high:,} low={low:,} avg={avg:,}")

# ── Main tick ─────────────────────────────────────────────────────────────────

def run_tick(game):
    now_est = datetime.now(timezone.utc) + timedelta(hours=config.TIMEZONE_OFFSET)
    dow     = now_est.weekday()
    today   = now_est.date()
    state   = get_state(game)

    if state.last_date != today:
        data = load_data(game)
        if state.last_date is not None:
            send_daily_summary(game, data)
            if dow == 0:
                send_weekly_summary(game, data)
        state.intraday_high  = 0
        state.intraday_low   = None
        state.intraday_sum   = 0
        state.intraday_ticks = 0
        state.last_date      = today

    ccu = fetch_ccu(game)
    if ccu is None: return

    data   = load_data(game)
    autoseed_from_ticks(game, data)   # no-op if slots already populated
    scored = score_last_prediction(data, ccu, dow, now_est.hour)

    avg_hour, ath, is_new_ath = record_snapshot(data, game, dow, now_est.hour, ccu, now_est)

    if state.midnight_ccu is None:
        state.midnight_ccu = ccu

    state.intraday_high   = max(state.intraday_high, ccu)
    state.intraday_low    = ccu if state.intraday_low is None else min(state.intraday_low, ccu)
    state.intraday_sum   += ccu
    state.intraday_ticks += 1

    pct_15m, d_15m = diff_label(ccu, state.last_tick)
    pct_avg, d_avg = diff_label(ccu, round(avg_hour))
    _,       d_24h = diff_label(ccu, state.midnight_ccu)

    sig          = compute_signal(data, ccu, dow, now_est.hour)
    sig_emoji    = SIGNAL_EMOJI.get(sig["signal"], "⚪")
    cv_val       = sig.get("cv")
    streak_input = sig["signal"] if sig.get("confidence") in ("High", "Medium") else "HOLD"
    streak_sig, streak_count = update_streak(data, streak_input)
    log.info(f"[{game['name']}] Signal: {sig['signal']} ({sig['confidence']}) "
             + (f"cv={cv_val:.1f}% — " if cv_val else "— ")
             + sig["reasoning"]
             + (f" [streak x{streak_count}]" if streak_count >= 2 else ""))

    pred = predict_next_tick(data, ccu, now_est, dow)
    record_prediction(data, now_est, pred["predicted_ccu"], pred["confidence"],
                      dow, now_est.hour, pred.get("signals_used", []))

    if pred["predicted_ccu"] is not None:
        sign     = "+" if pred["delta"] >= 0 else ""
        pred_str = (f"{pred['predicted_ccu']:,} ({sign}{pred['delta']:,} / "
                    f"{sign}{pred['delta_pct']:.1f}%)  {pred['confidence']}")
        log.info(f"[{game['name']}] Next tick: {pred_str}  [{pred['detail']}]")
    else:
        pred_str = "—"

    # Stop loss / take profit — only on actionable LONG or SHORT signals
    sl_tp_lines = ""
    dh_key = f"{dow}_{now_est.hour}"
    se     = data["slot_errors"].get(dh_key)
    use_sl_tp = (
        sig["signal"] in ("LONG", "SHORT")
        and pred["confidence"] == "High"
        and pred["predicted_ccu"] is not None
        and pred["std"] is not None
        and cv_val is not None and cv_val < 20
        and (
            (se and se["n"] >= 5 and se["mae"] / max(ccu, 1) * 100 < 8)
            or cv_val < 12
        )
    )
    if use_sl_tp:
        std = pred["std"]
        if sig["signal"] == "LONG":
            # Entered long at ccu — expect price to rise
            # TP above predicted, SL below predicted
            tp = round(pred["predicted_ccu"] + 1.0 * std)
            sl = round(pred["predicted_ccu"] - 1.5 * std)
            sl = max(sl, 0)
            if sl == 0 or tp <= ccu:
                sl_tp_lines = ""
            else:
                sl_tp_lines = f"\n🎯 TP  {tp:,}   🛑 SL  {sl:,}"
        else:  # SHORT
            # Entered short at ccu — expect price to fall
            # TP below predicted, SL above predicted
            tp = round(pred["predicted_ccu"] - 1.0 * std)
            sl = round(pred["predicted_ccu"] + 1.5 * std)
            tp = max(tp, 0)
            if tp >= ccu:
                sl_tp_lines = ""
            else:
                sl_tp_lines = f"\n🎯 TP  {tp:,}   🛑 SL  {sl:,}"

    mae, dir_pct, calibration, n_pred = prediction_accuracy(data)
    acc_str = f"\nModel  MAE {mae:.0f}  dir {dir_pct:.0f}%  n={n_pred}" if mae else ""

    forecast_parts = []
    if sig["next1_avg"]: forecast_parts.append(f"{int(sig['next1_avg']):,}")
    if sig["next2_avg"]: forecast_parts.append(f"{int(sig['next2_avg']):,}")
    forecast_str = " → ".join(forecast_parts) if forecast_parts else "—"

    event_label = detect_event(data, ccu, dow, now_est.hour, pct_15m)
    ttp_str     = time_to_peak(data, now_est)
    peak_str    = ttp_str if ttp_str else ""

    check_slot_drift(game, data, dow, now_est.hour, today)
    check_reseed_reminder(game, data)

    # ── Trend analysis — runs after snapshot, before save ─────────────────────
    run_trend_analysis(game, data, ccu, dow, now_est.hour, now_est)

    if is_new_ath:
        prev_ath  = game["ath_floor"]
        pct_above = (ccu - prev_ath) / prev_ath * 100 if prev_ath else 0
        title = f"🏆 {game['name']} NEW ATH: {ccu:,}  (+{pct_above:.1f}% above prev)"
    elif state.last_tick is not None and pct_15m > config.VOLATILITY_PCT:
        title = f"📈 {game['name']} SPIKE: {ccu:,}"
    elif state.last_tick is not None and pct_15m < -config.VOLATILITY_PCT:
        title = f"📉 {game['name']} DROP: {ccu:,}"
    elif sig["signal"] == "LONG":
        title = f"🟢 {game['name']} LONG: {ccu:,}"
    elif sig["signal"] == "SHORT":
        title = f"🔴 {game['name']} SHORT: {ccu:,}"
    else:
        title = f"{game['name']} • {DAYS[dow]} {now_est.strftime('%-I:%M %p')}  {ccu:,}"

    if event_label:                      trend = event_label
    elif pct_avg >  config.BREAKOUT_PCT: trend = "🔥 BREAKOUT"
    elif pct_avg < -config.DROP_AVG_PCT: trend = "📉 BELOW AVG"
    else:                                trend = "⚖️ Stable"

    time_label  = now_est.strftime("%-I:%M %p")
    low_display = f"{state.intraday_low:,}" if state.intraday_low is not None else "—"
    pct_str     = f"{(ccu-sig['curr_avg'])/sig['curr_avg']*100:+.1f}%" if sig["curr_avg"] else ""
    cv_str      = f"  cv {cv_val:.0f}%" if cv_val is not None else ""

    bias_val  = data.get("bias", 0.0)
    bias_pct  = abs(bias_val) / max(ccu, 1) * 100
    if bias_pct > 20:
        bias_note = f"  ⚠️ bias {-bias_val:+.0f} (model converging)"
    elif abs(bias_val) > 15:
        bias_note = f"  bias {-bias_val:+.0f}"
    else:
        bias_note = ""

    streak_str = f"  x{streak_count}" if streak_count >= 3 else ""
    sig_line1  = f"{sig_emoji} {sig['signal']}{streak_str}  •  {sig['confidence']}{cv_str}"
    sig_line2  = f"CCU {pct_str} vs adj. avg  •  1h: {forecast_str}"

    message = (
        f"{trend}  •  {DAYS[dow]} {time_label}\n"
        f"\n"
        f"CCU      {ccu:,}\n"
        f"15m      {d_15m}\n"
        f"vs Avg   {d_avg}\n"
        f"Day open {d_24h}\n"
        f"\n"
        f"↑ {state.intraday_high:,}  ↓ {low_display}  •  ATH 🏆 {ath:,}\n"
        f"\n"
        f"{sig_line1}\n"
        f"{sig_line2}\n"
        f"Next tick  {pred_str}{bias_note}"
        f"{sl_tp_lines}"
        + (f"\n{peak_str}" if peak_str else "")
        + acc_str
    )

    persist_state(game, data, now_est)
    save_data(game, data)

    notify(game, title, message, sig)
    state.last_tick = ccu
    log.info(f"[{game['name']}] Tick @ {time_label} — "
             f"CCU: {ccu:,} | Avg: {round(avg_hour):,} | ATH: {ath:,} | "
             f"bias={bias_val:+.1f} weights={data['signal_weights']}")

# ── Loop ──────────────────────────────────────────────────────────────────────

def seconds_until_next_quarter():
    now     = datetime.now()
    elapsed = (now.minute % 15) * 60 + now.second
    return 900 - elapsed

if __name__ == "__main__":
    if not REDIS_URL or not REDIS_TOKEN:
        log.error("Missing Redis env vars"); raise SystemExit(1)
    if not config.GAMES:
        log.error("No games in config.py"); raise SystemExit(1)

    for game in config.GAMES:
        restore_state(game, load_data(game))
        log.info(f"Tracking: {game['name']}  (universe {game['universe_id']})")

    log.info(f"Redis: {REDIS_URL[:40]}...")
    log.info(f"Discord: {'enabled' if config.DISCORD_WEBHOOK else 'disabled'}")
    log.info(f"Games: {len(config.GAMES)}  |  Spacer between embeds: enabled")

    while not _shutdown:
        wait = seconds_until_next_quarter()
        log.info(f"Next tick in {wait}s")
        for _ in range(wait):
            if _shutdown: break
            time.sleep(1)

        if not _shutdown:
            for i, game in enumerate(config.GAMES):
                if _shutdown: break
                try:
                    run_tick(game)
                    if i < len(config.GAMES) - 1:
                        send_spacer()
                except Exception as e:
                    log.error(f"[{game['name']}] Unhandled error: {e}", exc_info=True)

    log.info("Ticker stopped cleanly.")
