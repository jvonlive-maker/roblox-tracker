"""
ticker.py
Multi-game Roblox CCU ticker.
- Polls every game in config.GAMES every 15 min
- Sends alerts to Discord webhook
- Persists state in Upstash Redis
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

# ── Data ─────────────────────────────────────────────────────────────────────

def load_data(game):
    try:
        raw = redis_get(game["redis_key"])
        if raw:
            data = json.loads(raw)
            data.setdefault("ath",        game["ath_floor"])
            data["ath"] = max(data["ath"], game["ath_floor"])
            data.setdefault("slots",      {})
            data.setdefault("trend",      0.0)
            data.setdefault("last_daily", None)
            data.setdefault("session",    {})
            data.setdefault("ticks",      [])
            return data
    except Exception as e:
        log.warning(f"[{game['name']}] Redis load failed: {e} — starting fresh.")
    return {"slots": {}, "trend": 0.0, "last_daily": None,
            "ath": game["ath_floor"], "session": {}, "ticks": []}

def save_data(game, data):
    redis_set(game["redis_key"], json.dumps(data, separators=(",", ":")))

def record_snapshot(data, game, dow, hour, ccu, now_est):
    # Update dow x hour slot stats incrementally
    key = f"{dow}_{hour}"
    slot = data["slots"].setdefault(key, {"avg": ccu, "std": 0.0, "cv": 0.0, "n": 0, "_vals": []})
    # Keep raw values list (capped at 30) for live recalculation
    vals = slot.get("_vals", [])
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
        data["ath"] = ccu

    tick_ts = now_est.strftime("%Y-%m-%dT%H:%M")
    if not data["ticks"] or data["ticks"][-1]["ts"] != tick_ts:
        data["ticks"].append({"ts": tick_ts, "ccu": ccu})
        data["ticks"] = data["ticks"][-2016:]

    return data["slots"][key]["avg"], data["ath"], is_new_ath

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

# ── Signal engine ─────────────────────────────────────────────────────────────

SIGNAL_COLORS = {
    "LONG": 0x26A69A, "SHORT": 0xEF5350,
    "HOLD": 0xF4C430, "INSUFFICIENT DATA": 0x888888,
}
SIGNAL_EMOJI = {
    "LONG": "🟢", "SHORT": "🔴", "HOLD": "🟡", "INSUFFICIENT DATA": "⚪"
}

def trend_adjusted_avg(slot, trend_per_day, last_daily_ccu, current_ccu):
    """
    Adjust the historical slot average for the observed decline trend.
    We anchor to last known daily CCU and apply the trend to estimate
    what the 'true' current baseline is.
    """
    if slot is None or last_daily_ccu is None or last_daily_ccu == 0:
        return slot["avg"] if slot else None
    # Ratio of current reality vs last daily snapshot
    ratio = current_ccu / last_daily_ccu
    # Blend: 50% raw historical avg, 50% trend-adjusted
    raw_avg     = slot["avg"]
    trend_adj   = raw_avg * ratio
    return raw_avg * 0.5 + trend_adj * 0.5

def compute_signal(data, ccu, dow, hour):
    curr_slot = get_slot(data, dow, hour)
    next_slot = get_slot(data, dow, hour + 1)
    next2_slot= get_slot(data, dow, hour + 2)

    if curr_slot is None or curr_slot["n"] < config.SIGNAL_MIN_SAMPLES:
        return {
            "signal": "INSUFFICIENT DATA", "confidence": "—",
            "reasoning": f"Need {config.SIGNAL_MIN_SAMPLES}+ samples for this slot.",
            "curr_avg": None, "next1_avg": None, "next2_avg": None, "cv": None,
        }

    curr_avg  = trend_adjusted_avg(curr_slot, data["trend"], data["last_daily"], ccu)
    next1_avg = trend_adjusted_avg(next_slot,  data["trend"], data["last_daily"], ccu) if next_slot else None
    next2_avg = trend_adjusted_avg(next2_slot, data["trend"], data["last_daily"], ccu) if next2_slot else None
    cv        = curr_slot["cv"]
    pct       = (ccu - curr_avg) / curr_avg * 100

    # Confidence from CV: lower volatility = more confident
    if   cv < 12:  confidence = "High"
    elif cv < 22:  confidence = "Medium"
    else:          confidence = "Low"

    # Trend direction
    trend_up = trend_down = False
    trend_str = "trend unclear"
    if next1_avg is not None:
        d1 = (next1_avg - curr_avg) / curr_avg * 100
        if next2_avg is not None:
            d2 = (next2_avg - curr_avg) / curr_avg * 100
            if d1 > config.SIGNAL_TREND_PCT and d2 > config.SIGNAL_TREND_PCT:
                trend_up  = True; trend_str = f"avg rises +{d1:.1f}% → +{d2:.1f}% over 2h"
            elif d1 < -config.SIGNAL_TREND_PCT and d2 < -config.SIGNAL_TREND_PCT:
                trend_down= True; trend_str = f"avg falls {d1:.1f}% → {d2:.1f}% over 2h"
            else:
                trend_str = f"mixed next 2h ({d1:+.1f}%, {d2:+.1f}%)"
        else:
            if   d1 >  config.SIGNAL_TREND_PCT: trend_up   = True; trend_str = f"avg rises +{d1:.1f}% next hr"
            elif d1 < -config.SIGNAL_TREND_PCT: trend_down  = True; trend_str = f"avg falls {d1:.1f}% next hr"
            else: trend_str = f"flat next hr ({d1:+.1f}%)"

    if pct < -config.SIGNAL_LONG_PCT and trend_up:
        signal    = "LONG"
        reasoning = f"{pct:.1f}% below adj. avg — {trend_str} — recovery expected"
    elif pct > config.SIGNAL_SHORT_PCT and trend_down:
        signal    = "SHORT"
        reasoning = f"+{pct:.1f}% above adj. avg — {trend_str} — pullback expected"
    elif pct < -config.SIGNAL_LONG_PCT:
        signal    = "HOLD"; reasoning = f"{pct:.1f}% below avg but {trend_str}"
    elif pct > config.SIGNAL_SHORT_PCT:
        signal    = "HOLD"; reasoning = f"+{pct:.1f}% above avg but {trend_str}"
    else:
        signal    = "HOLD"; reasoning = f"{pct:+.1f}% vs adj. avg — {trend_str}"

    return {
        "signal": signal, "confidence": confidence, "reasoning": reasoning,
        "curr_avg": curr_avg, "next1_avg": next1_avg, "next2_avg": next2_avg, "cv": cv,
    }

# ── Next-tick predictor ───────────────────────────────────────────────────────

def predict_next_tick(data, ccu, now_est, dow):
    """
    Three-signal predictor:
    1. Trend-adjusted dow x hour baseline delta (curr → next hour slot, prorated to 15 min)
    2. Recent momentum from last 3 ticks
    3. Intra-hour position bias from 10-min shape data
    """
    hour   = now_est.hour
    minute = now_est.minute
    ticks  = data.get("ticks", [])

    # ── Signal 1: baseline drift ──────────────────────────────
    curr_slot = get_slot(data, dow, hour)
    next_slot = get_slot(data, dow, hour + 1)
    baseline_delta = None
    baseline_cv    = None
    if curr_slot and next_slot and curr_slot["n"] >= config.SIGNAL_MIN_SAMPLES:
        curr_adj = trend_adjusted_avg(curr_slot, data["trend"], data["last_daily"], ccu)
        next_adj = trend_adjusted_avg(next_slot, data["trend"], data["last_daily"], ccu)
        # Prorate: we're `minute` mins into the hour, so fraction remaining = (60-minute)/60
        # The next tick fires in 15 min. Drift for 15 min = hourly_drift × (15/60)
        hourly_drift   = next_adj - curr_adj
        baseline_delta = hourly_drift * (15 / 60)
        baseline_cv    = curr_slot["cv"]

    # ── Signal 2: recent momentum (last 3 ticks) ──────────────
    momentum_delta = None
    if len(ticks) >= 3:
        recent = ticks[-3:]
        # Average of tick-to-tick deltas
        deltas = [recent[i+1]["ccu"] - recent[i]["ccu"] for i in range(len(recent)-1)]
        # Weight more recent delta heavier
        weights = [0.4, 0.6]
        momentum_delta = sum(d*w for d,w in zip(deltas, weights))

    # ── Signal 3: same dow+hour+minute historical momentum ─────
    slot_delta = None
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
    if slot_deltas_list:
        slot_delta = sum(slot_deltas_list) / len(slot_deltas_list)

    # ── Combine ───────────────────────────────────────────────
    signals = []
    weights = []

    if baseline_delta is not None:
        signals.append(baseline_delta)
        # Weight inversely to CV — low volatility slot gets more weight
        w = max(0.1, 1.0 - baseline_cv / 100)
        weights.append(w * 1.5)   # baseline gets 1.5× base weight

    if slot_delta is not None:
        signals.append(slot_delta)
        # More historical same-slot samples = more weight
        w = min(1.0, len(slot_deltas_list) / 8)
        weights.append(w * 1.2)

    if momentum_delta is not None:
        signals.append(momentum_delta)
        weights.append(0.6)       # momentum gets moderate weight

    if not signals:
        return {"predicted_ccu": None, "delta": None, "delta_pct": None,
                "confidence": "—", "detail": "insufficient data"}

    total_w   = sum(weights)
    delta     = sum(s * w for s, w in zip(signals, weights)) / total_w
    predicted = round(ccu + delta)
    delta_int = predicted - ccu
    delta_pct = delta / ccu * 100 if ccu else 0

    # Confidence: based on how many signals fired + slot CV
    n_signals = len(signals)
    if n_signals == 3 and baseline_cv is not None and baseline_cv < 15:
        confidence = "High"
    elif n_signals >= 2:
        confidence = "Medium"
    else:
        confidence = "Low"

    parts = []
    if baseline_delta is not None: parts.append(f"baseline {baseline_delta:+.0f}")
    if slot_delta     is not None: parts.append(f"slot {slot_delta:+.0f} (n={len(slot_deltas_list)})")
    if momentum_delta is not None: parts.append(f"momentum {momentum_delta:+.0f}")

    return {
        "predicted_ccu": predicted,
        "delta":         delta_int,
        "delta_pct":     delta_pct,
        "confidence":    confidence,
        "detail":        " | ".join(parts),
    }

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

# ── Daily summary ─────────────────────────────────────────────────────────────

def send_daily_summary(game):
    state = get_state(game)
    if state.intraday_ticks == 0: return
    avg  = state.intraday_sum // state.intraday_ticks
    low  = state.intraday_low or 0
    msg  = f"↑ {state.intraday_high:,}  ↓ {low:,}  avg {avg:,}"
    notify(game, f"📅 {game['name']} — Daily Summary", msg,
           {"signal": "HOLD"})
    log.info(f"[{game['name']}] Daily summary — "
             f"high={state.intraday_high:,} low={low:,} avg={avg:,}")

# ── Main tick ─────────────────────────────────────────────────────────────────

DAYS = ["Mon","Tue","Wed","Thu","Fri","Sat","Sun"]

def run_tick(game):
    now_est = datetime.now(timezone.utc) + timedelta(hours=config.TIMEZONE_OFFSET)
    dow     = now_est.weekday()
    today   = now_est.date()
    state   = get_state(game)

    if state.last_date != today:
        if state.last_date is not None:
            send_daily_summary(game)
        state.intraday_high  = 0
        state.intraday_low   = None
        state.intraday_sum   = 0
        state.intraday_ticks = 0
        state.last_date      = today

    ccu = fetch_ccu(game)
    if ccu is None: return

    data = load_data(game)
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

    persist_state(game, data, now_est)
    save_data(game, data)

    sig       = compute_signal(data, ccu, dow, now_est.hour)
    sig_emoji = SIGNAL_EMOJI.get(sig["signal"], "⚪")
    log.info(f"[{game['name']}] Signal: {sig['signal']} ({sig['confidence']}) "
             f"cv={sig.get('cv','?'):.1f}% — {sig['reasoning']}" if sig.get('cv') else
             f"[{game['name']}] Signal: {sig['signal']} — {sig['reasoning']}")

    pred = predict_next_tick(data, ccu, now_est, dow)
    if pred["predicted_ccu"] is not None:
        sign     = "+" if pred["delta"] >= 0 else ""
        pred_str = (f"{pred['predicted_ccu']:,} ({sign}{pred['delta']:,} / "
                    f"{sign}{pred['delta_pct']:.1f}%)  {pred['confidence']}")
        log.info(f"[{game['name']}] Next tick: {pred_str}  [{pred['detail']}]")
    else:
        pred_str = "—"

    # Hourly forecast
    forecast_parts = []
    if sig["next1_avg"]: forecast_parts.append(f"{int(sig['next1_avg']):,}")
    if sig["next2_avg"]: forecast_parts.append(f"{int(sig['next2_avg']):,}")
    forecast_str = " → ".join(forecast_parts) if forecast_parts else "—"

    # Title
    if is_new_ath:
        title = f"🏆 {game['name']} NEW RECORD: {ccu:,}"; priority="5"; tags="tada,fire"
    elif state.last_tick is not None and pct_15m > config.VOLATILITY_PCT:
        title = f"📈 {game['name']} SPIKE: {ccu:,}"; priority="4"; tags="warning"
    elif state.last_tick is not None and pct_15m < -config.VOLATILITY_PCT:
        title = f"📉 {game['name']} DROP: {ccu:,}"; priority="4"; tags="warning"
    elif sig["signal"] == "LONG":
        title = f"🟢 {game['name']} LONG: {ccu:,}"; priority="4"; tags="moneybag"
    elif sig["signal"] == "SHORT":
        title = f"🔴 {game['name']} SHORT: {ccu:,}"; priority="4"; tags="chart_with_downwards_trend"
    else:
        title = f"{game['name']} • {DAYS[dow]} {now_est.strftime('%-I:%M %p')}  {ccu:,}"
        priority="3"; tags="football"

    # Trend label
    if   pct_avg >  config.BREAKOUT_PCT: trend = "🔥 BREAKOUT"
    elif pct_avg < -config.DROP_AVG_PCT: trend = "📉 BELOW AVG"
    else:                                trend = "⚖️ Stable"

    time_label  = now_est.strftime("%-I:%M %p")
    low_display = f"{state.intraday_low:,}" if state.intraday_low is not None else "—"
    pct_str     = f"{(ccu-sig['curr_avg'])/sig['curr_avg']*100:+.1f}%" if sig["curr_avg"] else ""
    cv_str      = f"  cv {sig['cv']:.0f}%" if sig.get("cv") is not None else ""

    sig_line1 = f"{sig_emoji} {sig['signal']}  •  {sig['confidence']}{cv_str}"
    sig_line2 = f"CCU {pct_str} vs adj. avg  •  1h: {forecast_str}"

    message = (
        f"{trend}  •  {DAYS[dow]} {time_label}\n"
        f"\n"
        f"CCU      {ccu:,}\n"
        f"15m      {d_15m}\n"
        f"vs Avg   {d_avg}\n"
        f"Since ↑  {d_24h}\n"
        f"\n"
        f"↑ {state.intraday_high:,}  ↓ {low_display}  •  ATH 🏆 {ath:,}\n"
        f"\n"
        f"{sig_line1}\n"
        f"{sig_line2}\n"
        f"Next tick  {pred_str}"
    )

    notify(game, title, message, sig)
    state.last_tick = ccu
    log.info(f"[{game['name']}] Tick @ {time_label} — "
             f"CCU: {ccu:,} | Avg: {round(avg_hour):,} | ATH: {ath:,}")

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
