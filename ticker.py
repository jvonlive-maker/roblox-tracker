"""
ticker.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Multi-game Roblox CCU ticker.
- Polls every game in config.GAMES every 15 min
- Sends alerts to Discord webhook
- Persists all state in Upstash Redis
Requires: pip install requests
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

import json
import logging
import os
import signal
import time
from datetime import datetime, timezone, timedelta
from urllib.parse import quote

import requests

import config

# ───────────────────────────────────────────────────────────
# LOGGING
# ───────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ───────────────────────────────────────────────────────────
# GRACEFUL SHUTDOWN
# ───────────────────────────────────────────────────────────
_shutdown = False

def _handle_signal(sig, frame):
    global _shutdown
    log.info("Shutdown signal received — finishing current tick then exiting.")
    _shutdown = True

signal.signal(signal.SIGINT,  _handle_signal)
signal.signal(signal.SIGTERM, _handle_signal)

# ───────────────────────────────────────────────────────────
# HTTP SESSION
# ───────────────────────────────────────────────────────────
HEADERS = {"User-Agent": "Mozilla/5.0"}
http = requests.Session()
http.headers.update(HEADERS)

# ───────────────────────────────────────────────────────────
# UPSTASH REDIS
# ───────────────────────────────────────────────────────────
REDIS_URL   = os.environ.get("UPSTASH_REDIS_REST_URL",   "")
REDIS_TOKEN = os.environ.get("UPSTASH_REDIS_REST_TOKEN", "")


def _redis_headers():
    return {"Authorization": f"Bearer {REDIS_TOKEN}"}


def redis_get(key: str) -> str | None:
    url    = f"{REDIS_URL}/get/{quote(key, safe='')}"
    r      = http.get(url, headers=_redis_headers(), timeout=10)
    r.raise_for_status()
    result = r.json().get("result")
    return result if isinstance(result, str) else None


def redis_set(key: str, value: str, retries: int = 3):
    url = f"{REDIS_URL}/set/{quote(key, safe='')}/{quote(value, safe='')}"
    for attempt in range(1, retries + 1):
        try:
            r = http.get(url, headers=_redis_headers(), timeout=10)
            r.raise_for_status()
            return
        except requests.RequestException as e:
            wait = 2 ** attempt
            log.warning(f"Redis SET failed (attempt {attempt}/{retries}): {e}. Retry in {wait}s")
            time.sleep(wait)
    log.error("All Redis SET attempts failed.")


# ───────────────────────────────────────────────────────────
# DATA  —  load / save / snapshot
# ───────────────────────────────────────────────────────────
def load_data(game: dict) -> dict:
    try:
        raw = redis_get(game["redis_key"])
        if raw:
            data = json.loads(raw)
            data.setdefault("ath",     game["ath_floor"])
            data["ath"] = max(data["ath"], game["ath_floor"])
            data.setdefault("stats",   {"weekday": {}, "weekend": {}})
            data.setdefault("session", {})
            data.setdefault("ticks",   [])
            return data
    except Exception as e:
        log.warning(f"[{game['name']}] Redis load failed: {e} — starting fresh.")
    return {
        "stats":   {"weekday": {}, "weekend": {}},
        "ath":     game["ath_floor"],
        "session": {},
        "ticks":   [],
    }


def save_data(game: dict, data: dict):
    redis_set(game["redis_key"], json.dumps(data, separators=(",", ":")))


def record_snapshot(data: dict, game: dict, is_weekend: bool, hour: int,
                    ccu: int, now_est: datetime) -> tuple[float, int, bool]:
    group    = "weekend" if is_weekend else "weekday"
    hour_key = str(hour)
    slot     = data["stats"][group].setdefault(hour_key, [])
    slot.append(ccu)
    data["stats"][group][hour_key] = slot[-30:]

    is_new_ath = ccu > data["ath"]
    if is_new_ath:
        data["ath"] = ccu

    tick_ts = now_est.strftime("%Y-%m-%dT%H:%M")
    if not data["ticks"] or data["ticks"][-1]["ts"] != tick_ts:
        data["ticks"].append({"ts": tick_ts, "ccu": ccu})
        data["ticks"] = data["ticks"][-120:]

    avg = sum(data["stats"][group][hour_key]) / len(data["stats"][group][hour_key])
    return avg, data["ath"], is_new_ath


# ───────────────────────────────────────────────────────────
# PER-GAME SESSION STATE
# ───────────────────────────────────────────────────────────
class GameState:
    def __init__(self):
        self.last_tick:      int | None = None
        self.midnight_ccu:   int | None = None
        self.last_date:      object     = None
        self.intraday_high:  int        = 0
        self.intraday_low:   int | None = None
        self.intraday_sum:   int        = 0
        self.intraday_ticks: int        = 0


# One GameState per game, keyed by redis_key
_game_states: dict[str, GameState] = {}

def get_state(game: dict) -> GameState:
    key = game["redis_key"]
    if key not in _game_states:
        _game_states[key] = GameState()
    return _game_states[key]


def restore_state(game: dict, data: dict):
    state     = get_state(game)
    s         = data.get("session", {})
    state.last_tick    = s.get("last_tick")
    state.midnight_ccu = s.get("midnight_ccu")

    now_est   = datetime.now(timezone.utc) + timedelta(hours=config.TIMEZONE_OFFSET)
    today_str = now_est.date().isoformat()
    saved     = s.get("last_date")

    if saved == today_str:
        state.last_date      = now_est.date()
        state.intraday_high  = s.get("intraday_high", 0)
        raw_low              = s.get("intraday_low")
        state.intraday_low   = raw_low if raw_low else None
        state.intraday_sum   = s.get("intraday_sum",   0)
        state.intraday_ticks = s.get("intraday_ticks", 0)
        prev = f"{state.last_tick:,}" if state.last_tick else "none"
        log.info(f"[{game['name']}] Resumed from {saved} — last CCU: {prev}")
    else:
        log.info(f"[{game['name']}] New day — intraday state reset")


def persist_state(game: dict, data: dict, now_est: datetime):
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


# ───────────────────────────────────────────────────────────
# CANDLESTICK CHART
# ───────────────────────────────────────────────────────────
BG      = "#131722"
PANEL   = "#1E222D"
GRID    = "#2A2E39"
TEXT    = "#D1D4DC"
GREEN   = "#26A69A"
RED     = "#EF5350"
AVGLINE = "#F4C430"


def build_candles(ticks: list[dict]) -> list[dict]:
    needed  = ticks[-(config.CANDLES * config.TICKS_PER_CANDLE):]
    candles = []
    for i in range(0, len(needed), config.TICKS_PER_CANDLE):
        chunk = needed[i:i + config.TICKS_PER_CANDLE]
        if not chunk:
            continue
        values = [t["ccu"] for t in chunk]
        candles.append({
            "label": chunk[0]["ts"][11:16],
            "open":  values[0],
            "close": values[-1],
            "high":  max(values),
            "low":   min(values),
        })
    return candles[-config.CANDLES:]




# ───────────────────────────────────────────────────────────
# FORMATTING
# ───────────────────────────────────────────────────────────
def diff_label(current: int, historical: int | None) -> tuple[float, str]:
    if historical is None or historical == 0:
        return 0.0, "—"
    diff = current - historical
    pct  = diff / historical * 100
    sign = "+" if diff >= 0 else ""
    return pct, f"{sign}{diff:,} ({sign}{pct:.1f}%)"


# ───────────────────────────────────────────────────────────
# SIGNAL ENGINE
# ───────────────────────────────────────────────────────────
def _hour_avg(stats: dict, group: str, hour: int) -> tuple[float, int] | None:
    slot = stats[group].get(str(hour % 24), [])
    if len(slot) < config.SIGNAL_MIN_SAMPLES:
        return None
    return sum(slot) / len(slot), len(slot)


def compute_signal(data: dict, ccu: int, hour: int, is_weekend: bool) -> dict:
    group = "weekend" if is_weekend else "weekday"
    stats = data["stats"]

    curr  = _hour_avg(stats, group, hour)
    next1 = _hour_avg(stats, group, hour + 1)
    next2 = _hour_avg(stats, group, hour + 2)

    if curr is None:
        return {
            "signal": "INSUFFICIENT DATA", "confidence": "—",
            "reasoning": f"Need {config.SIGNAL_MIN_SAMPLES}+ samples for hour {hour}.",
            "curr_avg": None, "next1_avg": None, "next2_avg": None,
        }

    curr_avg, curr_n = curr
    next1_avg = next1[0] if next1 else None
    next2_avg = next2[0] if next2 else None
    pct       = (ccu - curr_avg) / curr_avg * 100

    trend_up = trend_down = False
    trend_str = "unknown next-hour trend"

    if next1_avg is not None:
        d1 = (next1_avg - curr_avg) / curr_avg * 100
        if next2_avg is not None:
            d2 = (next2_avg - curr_avg) / curr_avg * 100
            if d1 > config.SIGNAL_TREND_PCT and d2 > config.SIGNAL_TREND_PCT:
                trend_up  = True
                trend_str = f"avg rises +{d1:.1f}% then +{d2:.1f}% over 2h"
            elif d1 < -config.SIGNAL_TREND_PCT and d2 < -config.SIGNAL_TREND_PCT:
                trend_down = True
                trend_str  = f"avg falls {d1:.1f}% then {d2:.1f}% over 2h"
            else:
                trend_str = f"mixed next 2h ({d1:+.1f}%, {d2:+.1f}%)"
        else:
            if d1 > config.SIGNAL_TREND_PCT:
                trend_up  = True
                trend_str = f"avg rises +{d1:.1f}% next hour"
            elif d1 < -config.SIGNAL_TREND_PCT:
                trend_down = True
                trend_str  = f"avg falls {d1:.1f}% next hour"
            else:
                trend_str = f"flat next hour ({d1:+.1f}%)"

    score = min(curr_n, 20) + (3 if next1 else 0) + (3 if next2 else 0)
    confidence = "High" if score >= 18 else "Medium" if score >= 10 else "Low"

    if pct < -config.SIGNAL_LONG_PCT and trend_up:
        signal    = "LONG"
        reasoning = f"{pct:.1f}% below avg — {trend_str} — expect recovery"
    elif pct > config.SIGNAL_SHORT_PCT and trend_down:
        signal    = "SHORT"
        reasoning = f"+{pct:.1f}% above avg — {trend_str} — expect pullback"
    elif pct < -config.SIGNAL_LONG_PCT:
        signal    = "HOLD"
        reasoning = f"{pct:.1f}% below avg but {trend_str}"
    elif pct > config.SIGNAL_SHORT_PCT:
        signal    = "HOLD"
        reasoning = f"+{pct:.1f}% above avg but {trend_str}"
    else:
        signal    = "HOLD"
        reasoning = f"{pct:+.1f}% vs avg — {trend_str}"

    return {
        "signal": signal, "confidence": confidence, "reasoning": reasoning,
        "curr_avg": curr_avg, "next1_avg": next1_avg, "next2_avg": next2_avg,
    }


# ───────────────────────────────────────────────────────────
# NTFY
# ───────────────────────────────────────────────────────────

def send_discord(game: dict, title: str, message: str,
                 sig: dict, retries: int = 3):
    webhook = config.DISCORD_WEBHOOK
    if not webhook:
        return

    color = SIGNAL_COLORS.get(sig["signal"], 0x888888)

    # Build a clean Discord embed
    embed = {
        "title":       title,
        "description": message,
        "color":       color,
        "footer":      {"text": f"{game['name']}  •  EST"},
    }

    payload = json.dumps({"embeds": [embed]})

    for attempt in range(1, retries + 1):
        try:
            r = http.post(webhook, data=payload,
                          headers={"Content-Type": "application/json"}, timeout=10)
            # 204 = success, 429 = rate limited
            if r.status_code == 429:
                retry_after = r.json().get("retry_after", 2)
                log.warning(f"[{game['name']}] Discord rate limited — waiting {retry_after}s")
                time.sleep(retry_after)
                continue
            r.raise_for_status()
            break
        except requests.RequestException as e:
            if attempt == retries:
                log.error(f"[{game['name']}] Discord send failed: {e}")
                return
            time.sleep(2 ** attempt)



# ───────────────────────────────────────────────────────────
# COMBINED NOTIFY  —  sends to both ntfy + Discord
# ───────────────────────────────────────────────────────────
def notify(game: dict, title: str, message: str, sig: dict):
    send_discord(game, title, message, sig)


# ───────────────────────────────────────────────────────────
# ROBLOX API
# ───────────────────────────────────────────────────────────
def fetch_ccu(game: dict, retries: int = 3) -> int | None:
    url = f"https://games.roblox.com/v1/games?universeIds={game['universe_id']}"
    for attempt in range(1, retries + 1):
        try:
            r = http.get(url, timeout=10)
            r.raise_for_status()
            return r.json()["data"][0]["playing"]
        except (requests.RequestException, KeyError, IndexError) as e:
            if attempt == retries:
                log.error(f"[{game['name']}] Roblox API failed: {e}")
                return None
            time.sleep(2 ** attempt)
    return None


# ───────────────────────────────────────────────────────────
# DAILY SUMMARY
# ───────────────────────────────────────────────────────────
def send_daily_summary(game: dict):
    state = get_state(game)
    if state.intraday_ticks == 0:
        return
    avg  = state.intraday_sum // state.intraday_ticks
    high = state.intraday_high
    low  = state.intraday_low or 0
    title   = f"📅 {game['name']} — Daily Summary"
    message = f"↑ {high:,}  ↓ {low:,}  avg {avg:,}"
    notify(game, title, message, {"signal": "HOLD", "confidence": "—", "reasoning": ""})
    log.info(f"[{game['name']}] Daily summary — high={high:,} low={low:,} avg={avg:,}")


# ───────────────────────────────────────────────────────────
# TICK  —  runs once per game per 15-min mark
# ───────────────────────────────────────────────────────────
def run_tick(game: dict):
    now_est    = datetime.now(timezone.utc) + timedelta(hours=config.TIMEZONE_OFFSET)
    is_weekend = now_est.weekday() >= 5
    today      = now_est.date()
    state      = get_state(game)

    # Day rollover
    if state.last_date != today:
        if state.last_date is not None:
            send_daily_summary(game)
        state.intraday_high  = 0
        state.intraday_low   = None
        state.intraday_sum   = 0
        state.intraday_ticks = 0
        state.last_date      = today

    ccu = fetch_ccu(game)
    if ccu is None:
        return

    data = load_data(game)
    avg_hour, ath, is_new_ath = record_snapshot(data, game, is_weekend, now_est.hour, ccu, now_est)

    if state.midnight_ccu is None:
        state.midnight_ccu = ccu

    state.intraday_high   = max(state.intraday_high, ccu)
    state.intraday_low    = ccu if state.intraday_low is None else min(state.intraday_low, ccu)
    state.intraday_sum   += ccu
    state.intraday_ticks += 1

    pct_15m, d_15m = diff_label(ccu, state.last_tick)
    pct_avg, d_avg = diff_label(ccu, int(avg_hour))
    _,       d_24h = diff_label(ccu, state.midnight_ccu)

    persist_state(game, data, now_est)
    save_data(game, data)



    sig       = compute_signal(data, ccu, now_est.hour, is_weekend)
    sig_emoji = SIGNAL_EMOJI.get(sig["signal"], "⚪")
    log.info(f"[{game['name']}] Signal: {sig['signal']} ({sig['confidence']}) — {sig['reasoning']}")

    # Forecast
    forecast_parts = []
    if sig["next1_avg"] is not None:
        forecast_parts.append(f"{int(sig['next1_avg']):,}")
    if sig["next2_avg"] is not None:
        forecast_parts.append(f"{int(sig['next2_avg']):,}")
    forecast_str = " → ".join(forecast_parts) if forecast_parts else "—"

    # Title + priority
    if is_new_ath:
        title    = f"🏆 {game['name']} NEW RECORD: {ccu:,}"
        priority = "5"; tags = "tada,fire"
    elif state.last_tick is not None and pct_15m > config.VOLATILITY_PCT:
        title    = f"📈 {game['name']} SPIKE: {ccu:,}"
        priority = "4"; tags = "chart_with_upwards_trend,warning"
    elif state.last_tick is not None and pct_15m < -config.VOLATILITY_PCT:
        title    = f"📉 {game['name']} DROP: {ccu:,}"
        priority = "4"; tags = "chart_with_downwards_trend,warning"
    elif sig["signal"] == "LONG":
        title    = f"🟢 {game['name']} LONG: {ccu:,}"
        priority = "4"; tags = "green_circle,moneybag"
    elif sig["signal"] == "SHORT":
        title    = f"🔴 {game['name']} SHORT: {ccu:,}"
        priority = "4"; tags = "red_circle,chart_with_downwards_trend"
    else:
        title    = f"{game['name']} Ticker: {ccu:,}"
        priority = "3"; tags = "football"

    # Trend
    if   pct_avg >  config.BREAKOUT_PCT:  trend = "🔥 BREAKOUT"
    elif pct_avg < -config.DROP_AVG_PCT:  trend = "📉 BELOW AVG"
    else:                                 trend = "⚖️ Stable"

    # Signal summary lines (short, mobile-friendly)
    time_label  = now_est.strftime("%-I:%M %p")
    low_display = f"{state.intraday_low:,}" if state.intraday_low is not None else "—"
    pct_str     = f"{(ccu - sig['curr_avg']) / sig['curr_avg'] * 100:+.1f}%" if sig["curr_avg"] else ""
    sig_line1   = f"{sig_emoji} {sig['signal']}  •  {sig['confidence']} confidence"
    sig_line2   = f"CCU {pct_str} vs avg  •  Next: {forecast_str}"

    message = (
        f"{trend}  •  {time_label}\n"
        f"\n"
        f"CCU      {ccu:,}\n"
        f"15m      {d_15m}\n"
        f"vs Avg   {d_avg}\n"
        f"Since ↑  {d_24h}\n"
        f"\n"
        f"↑ {state.intraday_high:,}  ↓ {low_display}  •  ATH 🏆 {ath:,}\n"
        f"\n"
        f"{sig_line1}\n"
        f"{sig_line2}"
    )

    notify(game, title, message, sig)

    state.last_tick = ccu
    log.info(f"[{game['name']}] Tick @ {time_label} — CCU: {ccu:,} | Avg: {int(avg_hour):,} | ATH: {ath:,}")


# ───────────────────────────────────────────────────────────
# CLOCK-SYNCED LOOP
# ───────────────────────────────────────────────────────────
def seconds_until_next_quarter() -> int:
    now     = datetime.now()
    elapsed = (now.minute % 15) * 60 + now.second
    return 900 - elapsed


if __name__ == "__main__":
    if not REDIS_URL or not REDIS_TOKEN:
        log.error("Missing UPSTASH_REDIS_REST_URL / UPSTASH_REDIS_REST_TOKEN env vars")
        raise SystemExit(1)

    if not config.GAMES:
        log.error("No games configured in config.py")
        raise SystemExit(1)

    # Restore state for all games
    for game in config.GAMES:
        restore_state(game, load_data(game))
        log.info(f"Tracking: {game['name']}  (universe {game['universe_id']})")

    log.info(f"Redis: {REDIS_URL[:40]}...")
    log.info(f"Discord: {'enabled' if config.DISCORD_WEBHOOK else 'disabled'}")

    while not _shutdown:
        wait = seconds_until_next_quarter()
        log.info(f"Syncing — next tick in {wait}s")
        for _ in range(wait):
            if _shutdown:
                break
            time.sleep(1)

        if not _shutdown:
            for game in config.GAMES:
                try:
                    run_tick(game)
                except Exception as e:
                    log.error(f"[{game['name']}] Unhandled tick error: {e}")
            time.sleep(5)

    log.info("Ticker stopped cleanly.")
