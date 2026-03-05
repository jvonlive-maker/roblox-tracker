"""
ticker.py
Multi-game Roblox CCU ticker.
- Polls every game in config.GAMES every 15 min
- Sends alerts to Discord webhook
- Persists state in Upstash Redis
- Self-improving: adaptive signal weights, bias correction, per-slot error memory
Requires: pip install requests
"""

import json, logging, os, signal, time, statistics, math
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

# ── Data ──────────────────────────────────────────────────────────────────────

def load_data(game):
    try:
        raw = redis_get(game["redis_key"])
        if raw:
            data = json.loads(raw)
            data.setdefault("ath",          game["ath_floor"])
            data["ath"] = max(data["ath"],  game["ath_floor"])
            data.setdefault("ath_ts",       None)
            data.setdefault("slots",        {})
            data.setdefault("trend",        0.0)
            data.setdefault("last_daily",   None)
            data.setdefault("session",      {})
            data.setdefault("ticks",        [])
            data.setdefault("pred_log",     [])
            data.setdefault("week_stats",   {})
            # Learning state
            data.setdefault("signal_weights", {"baseline": 1.5, "slot": 1.2, "momentum": 0.6})
            data.setdefault("bias",           0.0)   # rolling residual correction
            data.setdefault("slot_errors",    {})    # per dow_hour MAE
            data.setdefault("seed_ts",        None)  # date of last reseed
            data.setdefault("signal_streak",  {"signal": None, "count": 0})  # consecutive signal
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
    }

def save_data(game, data):
    redis_set(game["redis_key"], json.dumps(data, separators=(",", ":")))

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
        "high": ccu, "low": ccu, "sum": 0, "n": 0, "pred_hits": 0, "pred_total": 0
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
        "signals_used": signals_used,   # list of signal names that fired
    })
    data["pred_log"] = data["pred_log"][-200:]

def score_last_prediction(data, ccu, now_dow, now_hour):
    """
    Score the most recent unscored prediction.
    Also:
      1. Update per-slot error memory (slot_errors)
      2. Update rolling bias (systematic over/under prediction)
      3. Update adaptive signal weights based on which signals were most accurate
    Returns the scored entry or None.
    """
    scored_entry = None
    for entry in reversed(data["pred_log"]):
        if entry["actual"] is None:
            entry["actual"] = ccu
            scored_entry    = entry
            break

    if scored_entry is None:
        return None

    error     = scored_entry["predicted"] - ccu   # signed: + = predicted too high
    abs_error = abs(error)
    dh_key    = scored_entry.get("dow_hour", f"{now_dow}_{now_hour}")

    # ── 1. Per-slot error memory ──────────────────────────────
    se = data["slot_errors"].setdefault(dh_key, {"mae": abs_error, "n": 1, "_errs": [abs_error]})
    errs = se.get("_errs", [se["mae"]])
    errs.append(abs_error)
    errs = errs[-20:]
    se["_errs"] = errs
    se["n"]     = len(errs)
    se["mae"]   = sum(errs) / len(errs)
    data["slot_errors"][dh_key] = se

    # ── 2. Rolling bias correction ────────────────────────────
    # Exponential moving average of signed error (alpha=0.15)
    # Positive bias = model is consistently predicting too high → subtract next time
    alpha        = 0.15
    data["bias"] = data["bias"] * (1 - alpha) + error * alpha

    # ── 3. Adaptive signal weights ────────────────────────────
    # We don't know each signal's individual contribution to the error, but we
    # track which signal *combination* was used and update weights proportionally.
    # Simpler heuristic: if error is large and momentum was used, decay momentum
    # weight slightly. If error is small, boost whichever signals fired.
    signals_used = scored_entry.get("signals_used", [])
    weights      = data["signal_weights"]
    pct_error    = abs_error / max(ccu, 1) * 100

    if pct_error < 5:
        # Accurate — modestly boost all signals that contributed
        for s in signals_used:
            if s in weights:
                weights[s] = min(2.5, weights[s] * 1.03)
    elif pct_error > 20:
        # Inaccurate — decay all signals that contributed
        for s in signals_used:
            if s in weights:
                weights[s] = max(0.2, weights[s] * 0.95)

    # Renormalise so they don't drift to infinity — keep sum ≈ 3.3 (original sum)
    total = sum(weights.values())
    if total > 0:
        target = 3.3
        factor = target / total
        for k in weights:
            weights[k] = round(weights[k] * factor, 4)

    data["signal_weights"] = weights

    log.info(f"Scored: pred={scored_entry['predicted']:,} actual={ccu:,} "
             f"err={error:+,} bias={data['bias']:+.1f} "
             f"weights={weights}")
    return scored_entry

def prediction_accuracy(data):
    scored = [e for e in data["pred_log"] if e["actual"] is not None]
    if len(scored) < 3:
        return None, None, 0
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
    return mae, directional, len(scored)

def slot_confidence(data, dh_key, cv):
    """
    Confidence based on OBSERVED per-slot error history (not just CV).
    After enough predictions, actual MAE beats CV as a predictor of reliability.
    Falls back to CV-based if no slot error history yet.
    """
    se = data["slot_errors"].get(dh_key)
    if se and se["n"] >= 5:
        # Use actual observed MAE relative to last known avg for this slot
        slot = data["slots"].get(dh_key)
        if slot and slot["avg"] > 0:
            obs_cv = se["mae"] / slot["avg"] * 100
            if   obs_cv < 8:  return "High"
            elif obs_cv < 18: return "Medium"
            else:             return "Low"
    # Fall back to CV%
    if   cv < 12: return "High"
    elif cv < 22: return "Medium"
    else:         return "Low"

# ── Signal streak tracker ────────────────────────────────────────────────────

def update_streak(data, signal):
    """
    Track consecutive ticks with the same signal.
    Returns (signal, count) — count>=3 means sustained signal.
    """
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
    """
    Returns string like 'Peak in ~2h 15m (Fri 4:00 PM  avg 7,140)'.
    Looks at all remaining slots today and finds the highest average one.
    Returns None if already at/past today's peak or no slot data.
    """
    if not data["slots"]:
        return None
    dow      = now_est.weekday()
    now_mins = now_est.hour * 60 + now_est.minute

    best_avg  = -1
    best_hour = -1
    for h in range(now_est.hour + 1, 24):   # only future hours
        slot = data["slots"].get(f"{dow}_{h}")
        if slot and slot["avg"] > best_avg:
            best_avg  = slot["avg"]
            best_hour = h

    if best_hour == -1:
        return None   # past peak for today

    # Check it's actually higher than current slot
    curr_slot = data["slots"].get(f"{dow}_{now_est.hour}")
    if curr_slot and best_avg <= curr_slot["avg"] * 1.05:
        return None   # already near peak, not worth showing

    delta_m  = best_hour * 60 - now_mins
    hrs, mins = divmod(delta_m, 60)
    peak_fmt = now_est.replace(hour=best_hour, minute=0).strftime("%-I:%M %p")
    eta      = f"~{hrs}h {mins}m" if hrs > 0 and mins else (f"~{hrs}h" if hrs else f"~{mins}m")

    return f"Peak in {eta}  ({DAYS[dow]} {peak_fmt}  avg {int(best_avg):,})"

# ── Reseed reminder ───────────────────────────────────────────────────────────

RESEED_DAYS = 45

def check_reseed_reminder(game, data):
    """
    Sends a Discord alert if seed data is older than RESEED_DAYS.
    Only fires once per 30 days so it doesn't spam.
    """
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
        f"`python3 clear_redis.py && python3 seed_redis.py`\n"
        f"then redeploy."
    )
    notify(game, f"\u26a0\ufe0f {game['name']} — Time to Reseed", msg, {"signal": "HOLD"})
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
    state = get_state(game)
    s     = data.get("session", {})
    state.last_tick    = s.get("last_tick")
    state.midnight_ccu = s.get("midnight_ccu")
    now_est   = datetime.now(timezone.utc) + timedelta(hours=config.TIMEZONE_OFFSET)
    today_str = now_est.date().isoformat()
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
        log.info(f"[{game['name']}] New day — intraday reset")

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

def peak_slot(data):
    best_key = max(data["slots"], key=lambda k: data["slots"][k]["avg"], default=None)
    if not best_key:
        return None
    dow, hour = map(int, best_key.split("_"))
    return DAYS[dow], hour, data["slots"][best_key]["avg"]

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
    # Hard ceiling — above 30% CV the slot is too noisy to trust regardless of history
    if cv > 30 and confidence != "Low":
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

    # If CCU is >45% below adj avg it's structural decline, not a dip — never LONG
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

    # Adaptive weights learned from prediction history
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
        # Weight most recent move heavier
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
        # Scale baseline weight by inverse CV (low volatility = trust more)
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

    total_w = sum(weights)
    delta   = sum(s * w for s, w in zip(signals, weights)) / total_w

    # ── Bias correction ───────────────────────────────────────
    # Subtract the rolling signed error so systematic drift is removed
    bias        = data.get("bias", 0.0)
    delta_corr  = delta - bias
    bias_str    = f" bias_corr={-bias:+.0f}" if abs(bias) > 10 else ""

    predicted = round(ccu + delta_corr)
    delta_int = predicted - ccu
    delta_pct = delta_corr / ccu * 100 if ccu else 0

    # Confidence: use observed slot MAE if available, else signal count + CV
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
    # Hard CV ceiling — noisy slots can never be High/Medium regardless of other factors
    if (baseline_cv or 100) > 30 and confidence != "Low":
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
    color   = SIGNAL_COLORS.get(sig["signal"], 0x888888)
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

# ── Weekly summary ────────────────────────────────────────────────────────────

def send_weekly_summary(game, data):
    now_est   = datetime.now(timezone.utc) + timedelta(hours=config.TIMEZONE_OFFSET)
    last_week = (now_est - timedelta(days=7)).strftime("%Y-W%W")
    ws        = data["week_stats"].get(last_week)
    if not ws or ws["n"] == 0:
        return

    avg = ws["sum"] // ws["n"]
    mae, dir_pct, n_pred = prediction_accuracy(data)
    pk = peak_slot(data)
    w  = data.get("signal_weights", {})

    acc_str  = f"\nPred  MAE {mae:.0f}  dir {dir_pct:.0f}%  n={n_pred}" if mae else ""
    peak_str = f"\nPeak  {pk[0]} {int(pk[1]):02d}:00  avg {int(pk[2]):,}" if pk else ""
    w_str    = (f"\nWeights  base {w.get('baseline',0):.2f}  "
                f"slot {w.get('slot',0):.2f}  "
                f"mom {w.get('momentum',0):.2f}") if w else ""
    bias_str = f"\nBias correction  {-data.get('bias',0):+.0f} CCU" if abs(data.get("bias",0)) > 5 else ""

    msg = (f"↑ {ws['high']:,}  ↓ {ws['low']:,}  avg {avg:,}"
           f"{acc_str}{peak_str}{w_str}{bias_str}")
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

DAYS = ["Mon","Tue","Wed","Thu","Fri","Sat","Sun"]

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
    # Only track streaks for High/Medium confidence — Low is too noisy
    streak_input  = sig["signal"] if sig.get("confidence") in ("High", "Medium") else "HOLD"
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

    # Stop loss / take profit — High confidence + low observed error
    sl_tp_lines = ""
    dh_key = f"{dow}_{now_est.hour}"
    se     = data["slot_errors"].get(dh_key)
    use_sl_tp = (
        pred["confidence"] == "High"
        and pred["predicted_ccu"] is not None
        and pred["std"] is not None
        and cv_val is not None and cv_val < 20   # hard CV ceiling for SL/TP
        and (
            (se and se["n"] >= 5 and se["mae"] / max(ccu, 1) * 100 < 8)
            or cv_val < 12
        )
    )
    if use_sl_tp:
        std = pred["std"]
        tp  = round(pred["predicted_ccu"] + 1.0 * std)
        sl  = round(pred["predicted_ccu"] - 1.5 * std)
        sl  = max(sl, 0)   # SL can never be negative
        if sl == 0 or tp <= ccu:
            sl_tp_lines = ""   # nonsensical levels, suppress
        else:
            sl_tp_lines = f"\n🎯 TP  {tp:,}   🛑 SL  {sl:,}"
        log.info(f"[{game['name']}] SL/TP: sl={sl:,} tp={tp:,}")

    mae, dir_pct, n_pred = prediction_accuracy(data)
    acc_str = f"\nModel  MAE {mae:.0f}  dir {dir_pct:.0f}%  n={n_pred}" if mae else ""

    forecast_parts = []
    if sig["next1_avg"]: forecast_parts.append(f"{int(sig['next1_avg']):,}")
    if sig["next2_avg"]: forecast_parts.append(f"{int(sig['next2_avg']):,}")
    forecast_str = " → ".join(forecast_parts) if forecast_parts else "—"

    event_label  = detect_event(data, ccu, dow, now_est.hour, pct_15m)
    ttp_str      = time_to_peak(data, now_est)
    check_reseed_reminder(game, data)

    pk = peak_slot(data)
    # Use time-to-peak if available (more useful), fall back to static peak slot
    if ttp_str:
        peak_str = ttp_str   # already has day/time/avg
    elif pk:
        peak_str = f"Peak  {pk[0]} {int(pk[1]):02d}:00  avg {int(pk[2]):,}"
    else:
        peak_str = ""

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

    if event_label:           trend = event_label
    elif pct_avg >  config.BREAKOUT_PCT: trend = "🔥 BREAKOUT"
    elif pct_avg < -config.DROP_AVG_PCT: trend = "📉 BELOW AVG"
    else:                                trend = "⚖️ Stable"

    time_label  = now_est.strftime("%-I:%M %p")
    low_display = f"{state.intraday_low:,}" if state.intraday_low is not None else "—"
    pct_str     = f"{(ccu-sig['curr_avg'])/sig['curr_avg']*100:+.1f}%" if sig["curr_avg"] else ""
    cv_str      = f"  cv {cv_val:.0f}%" if cv_val is not None else ""

    # Show bias correction in notification if meaningful
    bias_val     = data.get("bias", 0.0)
    bias_pct     = abs(bias_val) / max(ccu, 1) * 100
    if bias_pct > 20:
        bias_note = f"  ⚠️ bias {-bias_val:+.0f} (model converging)"
    elif abs(bias_val) > 15:
        bias_note = f"  bias {-bias_val:+.0f}"
    else:
        bias_note = ""

    streak_str = f"  x{streak_count}" if streak_count >= 3 else ""
    sig_line1  = f"{sig_emoji} {sig['signal']}{streak_str}  •  {sig['confidence']}{cv_str}"
    sig_line2 = f"CCU {pct_str} vs adj. avg  •  1h: {forecast_str}"

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

    while not _shutdown:
        wait = seconds_until_next_quarter()
        log.info(f"Syncing — next tick in {wait}s")
        for _ in range(wait):
            if _shutdown: break
            time.sleep(1)
        if not _shutdown:
            for game in config.GAMES:
                try:
                    run_tick(game)
                except Exception as e:
                    log.error(f"[{game['name']}] Unhandled error: {e}", exc_info=True)
            time.sleep(5)

    log.info("Ticker stopped cleanly.")
