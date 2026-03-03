"""
FF2 CCU Ticker
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Polls Roblox every 15 min, sends ntfy notification + chart.
State persists across restarts via Upstash Redis (free tier).
Requires: pip install requests matplotlib
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

import io
import json
import logging
import os
import signal
import time
from datetime import datetime, timezone, timedelta
from urllib.parse import quote

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.lines import Line2D
import requests

# ───────────────────────────────────────────────────────────
# CONFIGURATION
# ───────────────────────────────────────────────────────────
UNIVERSE_ID            = "3150475059"
GAME_NAME              = "FF2"
NTFY_TOPIC             = "CCU_TICKER8312010"
TIMEZONE_OFFSET        = -5          # EST (UTC-5)
REDIS_KEY              = "ff2_ccu_data"

ROBLOX_HISTORICAL_PEAK = 23798       # ATH floor — prevents false alerts on fresh data

VOLATILITY_PCT         = 20          # ±% in 15 min triggers spike/drop alert
BREAKOUT_PCT           = 15          # % above hourly avg
DROP_AVG_PCT           = 15          # % below hourly avg (positive; comparison is explicit)

CANDLES                = 5           # how many 1-hr candles to show
TICKS_PER_CANDLE       = 4           # 4×15min ticks = 1 hr candle

# Signal engine
SIGNAL_MIN_SAMPLES     = 3           # minimum historical samples needed to emit a signal
SIGNAL_LONG_PCT        = 8           # CCU must be this % below hour avg to consider LONG
SIGNAL_SHORT_PCT       = 8           # CCU must be this % above hour avg to consider SHORT
SIGNAL_TREND_PCT       = 3           # next-hour avg must differ by this % to confirm trend

HEADERS = {"User-Agent": "Mozilla/5.0"}

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
    log.info("Shutdown signal — finishing current tick then exiting.")
    _shutdown = True

signal.signal(signal.SIGINT,  _handle_signal)
signal.signal(signal.SIGTERM, _handle_signal)

# ───────────────────────────────────────────────────────────
# HTTP SESSION
# ───────────────────────────────────────────────────────────
http = requests.Session()
http.headers.update(HEADERS)

# ───────────────────────────────────────────────────────────
# UPSTASH REDIS  —  free persistent storage
# Set these env vars in Railway → Variables:
#   UPSTASH_REDIS_REST_URL
#   UPSTASH_REDIS_REST_TOKEN
# ───────────────────────────────────────────────────────────
REDIS_URL   = os.environ.get("UPSTASH_REDIS_REST_URL",   "")
REDIS_TOKEN = os.environ.get("UPSTASH_REDIS_REST_TOKEN", "")


def _redis_headers() -> dict:
    return {"Authorization": f"Bearer {REDIS_TOKEN}"}


def redis_get(key: str) -> str | None:
    """
    GET key via Upstash REST API.
    Returns the string value, or None if key does not exist.
    """
    url = f"{REDIS_URL}/get/{quote(key, safe='')}"
    r   = http.get(url, headers=_redis_headers(), timeout=10)
    r.raise_for_status()
    return r.json().get("result")  # None when key is missing


def redis_set(key: str, value: str, retries: int = 3):
    """
    SET key value via Upstash REST API (POST with JSON body).
    Upstash REST: POST /set/<key>  body={"value": "<string>"}
    Retries up to `retries` times with exponential back-off.
    """
    url     = f"{REDIS_URL}/set/{quote(key, safe='')}"
    payload = json.dumps({"value": value})
    for attempt in range(1, retries + 1):
        try:
            r = http.post(
                url,
                data=payload,
                headers={**_redis_headers(), "Content-Type": "application/json"},
                timeout=10,
            )
            r.raise_for_status()
            return
        except requests.RequestException as e:
            wait = 2 ** attempt
            log.warning(f"Redis SET failed (attempt {attempt}/{retries}): {e}. Retry in {wait}s")
            time.sleep(wait)
    log.error("All Redis SET attempts failed — data NOT saved this tick.")


# ───────────────────────────────────────────────────────────
# DATA  —  load / save / snapshot
# ───────────────────────────────────────────────────────────
def load_data() -> dict:
    try:
        raw = redis_get(REDIS_KEY)
        if raw:
            data = json.loads(raw)
            data.setdefault("ath",     ROBLOX_HISTORICAL_PEAK)
            data["ath"] = max(data["ath"], ROBLOX_HISTORICAL_PEAK)
            data.setdefault("stats",   {"weekday": {}, "weekend": {}})
            data.setdefault("session", {})
            data.setdefault("ticks",   [])
            return data
    except Exception as e:
        log.warning(f"Redis load failed: {e} — starting fresh.")
    return {
        "stats":   {"weekday": {}, "weekend": {}},
        "ath":     ROBLOX_HISTORICAL_PEAK,
        "session": {},
        "ticks":   [],
    }


def save_data(data: dict):
    redis_set(REDIS_KEY, json.dumps(data, separators=(",", ":")))


def record_snapshot(data: dict, is_weekend: bool, hour: int,
                    ccu: int, now_est: datetime) -> tuple[float, int, bool]:
    """Appends to hourly stats + raw tick log. Returns (avg, ath, is_new_ath)."""
    group    = "weekend" if is_weekend else "weekday"
    hour_key = str(hour)
    slot     = data["stats"][group].setdefault(hour_key, [])
    slot.append(ccu)
    data["stats"][group][hour_key] = slot[-30:]

    is_new_ath = ccu > data["ath"]
    if is_new_ath:
        data["ath"] = ccu

    # Raw tick log — deduplicated by minute, keep last 120 ticks (~30 hrs)
    tick_ts = now_est.strftime("%Y-%m-%dT%H:%M")
    if not data["ticks"] or data["ticks"][-1]["ts"] != tick_ts:
        data["ticks"].append({"ts": tick_ts, "ccu": ccu})
        data["ticks"] = data["ticks"][-120:]

    avg = sum(data["stats"][group][hour_key]) / len(data["stats"][group][hour_key])
    return avg, data["ath"], is_new_ath


# ───────────────────────────────────────────────────────────
# SESSION STATE  —  survives restarts via Redis JSON
# ───────────────────────────────────────────────────────────
class State:
    last_tick:      int | None = None
    midnight_ccu:   int | None = None
    last_date:      object     = None
    intraday_high:  int        = 0
    intraday_low:   int | None = None   # None until first tick; avoids false 0 low
    intraday_sum:   int        = 0
    intraday_ticks: int        = 0

state = State()


def restore_state(data: dict):
    s = data.get("session", {})
    state.last_tick    = s.get("last_tick")
    state.midnight_ccu = s.get("midnight_ccu")

    now_est   = datetime.now(timezone.utc) + timedelta(hours=TIMEZONE_OFFSET)
    today_str = now_est.date().isoformat()
    saved     = s.get("last_date")

    if saved == today_str:
        state.last_date      = now_est.date()
        state.intraday_high  = s.get("intraday_high",  0)
        # Restore None if was never set (stored as null)
        raw_low = s.get("intraday_low")
        state.intraday_low   = raw_low if raw_low else None
        state.intraday_sum   = s.get("intraday_sum",   0)
        state.intraday_ticks = s.get("intraday_ticks", 0)
        prev = f"{state.last_tick:,}" if state.last_tick else "none"
        log.info(f"Resumed state from {saved} — last CCU: {prev}")
    else:
        log.info(f"New day ({today_str}) — intraday state reset")


def persist_state(data: dict, now_est: datetime):
    data["session"] = {
        "last_tick":      state.last_tick,
        "midnight_ccu":   state.midnight_ccu,
        "last_date":      now_est.date().isoformat(),
        "intraday_high":  state.intraday_high,
        "intraday_low":   state.intraday_low,   # None stored as JSON null — safe
        "intraday_sum":   state.intraday_sum,
        "intraday_ticks": state.intraday_ticks,
    }


# ───────────────────────────────────────────────────────────
# CANDLESTICK CHART  (TradingView dark style)
# ───────────────────────────────────────────────────────────
BG      = "#131722"
PANEL   = "#1E222D"
GRID    = "#2A2E39"
TEXT    = "#D1D4DC"
GREEN   = "#26A69A"
RED     = "#EF5350"
AVGLINE = "#F4C430"


def build_candles(ticks: list[dict]) -> list[dict]:
    """
    Group ticks into 1-hr candles (4 ticks each).
    Returns list of {label, open, high, low, close} dicts, newest last.
    """
    needed  = ticks[-(CANDLES * TICKS_PER_CANDLE):]
    candles = []
    for i in range(0, len(needed), TICKS_PER_CANDLE):
        chunk = needed[i:i + TICKS_PER_CANDLE]
        if not chunk:
            continue
        values = [t["ccu"] for t in chunk]
        candles.append({
            "label": chunk[0]["ts"][11:16],   # "HH:MM" of candle open
            "open":  values[0],
            "close": values[-1],
            "high":  max(values),
            "low":   min(values),
        })
    return candles[-CANDLES:]


def render_chart(ticks: list[dict], avg_hour: float, ath: int) -> bytes | None:
    """Render candlestick chart and return PNG bytes, or None if not enough data."""
    candles = build_candles(ticks)
    if len(candles) < 2:
        return None

    fig, ax = plt.subplots(figsize=(7, 3.2), facecolor=BG)
    ax.set_facecolor(PANEL)

    xs = range(len(candles))

    for i, c in enumerate(candles):
        color    = GREEN if c["close"] >= c["open"] else RED
        body_bot = min(c["open"], c["close"])
        body_top = max(c["open"], c["close"])
        body_h   = max(body_top - body_bot, 1)

        ax.plot([i, i], [c["low"], c["high"]], color=color, linewidth=1.2, zorder=2)
        ax.bar(i, body_h, bottom=body_bot, width=0.55,
               color=color, edgecolor=color, linewidth=0.5, zorder=3)

    ax.axhline(avg_hour, color=AVGLINE, linewidth=1, linestyle="--",
               label=f"Avg {int(avg_hour):,}", zorder=4)

    all_vals = [v for c in candles for v in (c["high"], c["low"])]
    y_min, y_max = min(all_vals), max(all_vals)
    padding = max((y_max - y_min) * 0.12, 50)

    if y_min - padding <= ath <= y_max + padding:
        ax.axhline(ath, color="#9B59B6", linewidth=1, linestyle=":",
                   label=f"ATH {ath:,}", zorder=4)

    ax.set_xlim(-0.6, len(candles) - 0.4)
    ax.set_ylim(y_min - padding, y_max + padding)
    ax.set_xticks(list(xs))
    ax.set_xticklabels([c["label"] for c in candles], color=TEXT, fontsize=8)
    ax.yaxis.set_tick_params(labelcolor=TEXT, labelsize=8)
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: f"{int(v):,}"))
    ax.spines[:].set_visible(False)
    ax.grid(axis="y", color=GRID, linewidth=0.6, zorder=1)

    legend_handles = [
        Line2D([0], [0], color=AVGLINE, linewidth=1.5, linestyle="--",
               label=f"Avg  {int(avg_hour):,}"),
        mpatches.Patch(color=GREEN, label="Bullish"),
        mpatches.Patch(color=RED,   label="Bearish"),
    ]
    ax.legend(handles=legend_handles, loc="upper left",
              fontsize=7.5, facecolor=PANEL, edgecolor=GRID,
              labelcolor=TEXT, framealpha=0.9)

    ax.set_title(f"{GAME_NAME}  •  1-hr candles  •  EST",
                 color=TEXT, fontsize=9, pad=6)

    fig.tight_layout(pad=0.8)

    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=130, facecolor=BG)
    plt.close(fig)
    buf.seek(0)
    return buf.read()


# ───────────────────────────────────────────────────────────
# FORMATTING HELPERS
# ───────────────────────────────────────────────────────────
def diff_label(current: int, historical: int | None) -> tuple[float, str]:
    if historical is None or historical == 0:
        return 0.0, "—"
    diff = current - historical
    pct  = diff / historical * 100
    sign = "+" if diff >= 0 else ""
    return pct, f"{sign}{diff:,} ({sign}{pct:.1f}%)"


# ───────────────────────────────────────────────────────────
# NTFY  —  text notification + optional image attachment
# ───────────────────────────────────────────────────────────
def _ntfy_headers(extra: dict) -> dict:
    """
    Build ntfy headers, encoding string values as UTF-8 bytes so that
    requests sends them with the correct charset — NOT percent-encoded.
    ntfy parses header values as UTF-8; using quote() was wrong and
    caused titles like 'FF2%20Ticker%3A%20551'.
    """
    return {k: v.encode("utf-8") if isinstance(v, str) else v
            for k, v in extra.items()}


def send_notification(title: str, message: str, priority: str,
                      tags: str, chart_png: bytes | None = None,
                      retries: int = 3):
    url = f"https://ntfy.sh/{NTFY_TOPIC}"

    # Send text notification
    for attempt in range(1, retries + 1):
        try:
            r = http.post(
                url,
                data=message.encode("utf-8"),
                headers=_ntfy_headers({
                    "Title":    title.strip(),
                    "Priority": priority,
                    "Tags":     tags,
                    "Markdown": "yes",
                }),
                timeout=10,
            )
            r.raise_for_status()
            break
        except requests.RequestException as e:
            wait = 2 ** attempt
            log.warning(f"ntfy text send failed (attempt {attempt}/{retries}): {e}. Retry in {wait}s")
            if attempt == retries:
                log.error("All ntfy text send attempts failed.")
                return
            time.sleep(wait)

    # Send chart as separate attachment (if available)
    if chart_png:
        for attempt in range(1, retries + 1):
            try:
                r = http.put(
                    url,
                    data=chart_png,
                    headers=_ntfy_headers({
                        "Title":        f"📊 {GAME_NAME} Chart",
                        "Priority":     "1",
                        "Tags":         "chart_with_upwards_trend",
                        "Filename":     "chart.png",
                        "Content-Type": "image/png",
                    }),
                    timeout=15,
                )
                r.raise_for_status()
                break
            except requests.RequestException as e:
                wait = 2 ** attempt
                log.warning(f"Chart send failed (attempt {attempt}/{retries}): {e}. Retry in {wait}s")
                if attempt == retries:
                    log.warning("Chart failed to send — text notification was still delivered.")
                time.sleep(wait)


# ───────────────────────────────────────────────────────────
# ROBLOX API FETCH
# ───────────────────────────────────────────────────────────
def fetch_ccu(retries: int = 3) -> int | None:
    url = f"https://games.roblox.com/v1/games?universeIds={UNIVERSE_ID}"
    for attempt in range(1, retries + 1):
        try:
            r = http.get(url, timeout=10)
            r.raise_for_status()
            return r.json()["data"][0]["playing"]
        except (requests.RequestException, KeyError, IndexError) as e:
            wait = 2 ** attempt
            log.warning(f"Roblox API error (attempt {attempt}/{retries}): {e}. Retry in {wait}s")
            time.sleep(wait)
    log.error("All Roblox API attempts failed.")
    return None


# ───────────────────────────────────────────────────────────
# DAILY SUMMARY
# ───────────────────────────────────────────────────────────
def send_daily_summary():
    if state.intraday_ticks == 0:
        return
    avg  = state.intraday_sum // state.intraday_ticks
    high = state.intraday_high
    low  = state.intraday_low if state.intraday_low is not None else 0
    msg = (
        f"🔺 Peak:    {high:,}\n"
        f"🔻 Trough:  {low:,}\n"
        f"📊 Avg:     {avg:,}"
    )
    send_notification(
        title    = f"📅 {GAME_NAME} — Daily Summary",
        message  = msg,
        priority = "3",
        tags     = "calendar,bar_chart",
    )
    log.info(f"Daily summary — high={high:,} low={low:,} avg={avg:,}")


# ───────────────────────────────────────────────────────────
# SIGNAL ENGINE  —  LONG / SHORT / HOLD based on history
# ───────────────────────────────────────────────────────────
def _hour_avg(stats: dict, group: str, hour: int) -> tuple[float, int] | None:
    """Returns (avg, sample_count) for a given hour slot, or None if not enough data."""
    slot = stats[group].get(str(hour % 24), [])
    if len(slot) < SIGNAL_MIN_SAMPLES:
        return None
    return sum(slot) / len(slot), len(slot)


def compute_signal(data: dict, ccu: int, hour: int, is_weekend: bool) -> dict:
    """
    Analyses current CCU against historical hourly averages for this hour
    and the next two hours.

    Returns a dict with keys:
        signal      : "LONG" | "SHORT" | "HOLD" | "INSUFFICIENT DATA"
        confidence  : "High" | "Medium" | "Low" | "—"
        reasoning   : human-readable explanation string
        curr_avg    : float | None
        next1_avg   : float | None
        next2_avg   : float | None
    """
    group = "weekend" if is_weekend else "weekday"
    stats = data["stats"]

    curr  = _hour_avg(stats, group, hour)
    next1 = _hour_avg(stats, group, hour + 1)
    next2 = _hour_avg(stats, group, hour + 2)

    if curr is None:
        return {
            "signal":     "INSUFFICIENT DATA",
            "confidence": "—",
            "reasoning":  f"Need {SIGNAL_MIN_SAMPLES}+ samples for hour {hour} — keep collecting.",
            "curr_avg":   None,
            "next1_avg":  None,
            "next2_avg":  None,
        }

    curr_avg, curr_n = curr
    next1_avg        = next1[0] if next1 else None
    next2_avg        = next2[0] if next2 else None
    pct_vs_now       = (ccu - curr_avg) / curr_avg * 100

    # Determine whether upcoming hours trend up or down vs current avg
    trend_up   = False
    trend_down = False
    trend_str  = "unknown next-hour trend"

    if next1_avg is not None:
        d1 = (next1_avg - curr_avg) / curr_avg * 100
        if next2_avg is not None:
            d2 = (next2_avg - curr_avg) / curr_avg * 100
            if d1 > SIGNAL_TREND_PCT and d2 > SIGNAL_TREND_PCT:
                trend_up  = True
                trend_str = f"hist. avg rises +{d1:.1f}% then +{d2:.1f}% over next 2h"
            elif d1 < -SIGNAL_TREND_PCT and d2 < -SIGNAL_TREND_PCT:
                trend_down = True
                trend_str  = f"hist. avg falls {d1:.1f}% then {d2:.1f}% over next 2h"
            else:
                trend_str = f"mixed next 2h ({d1:+.1f}%, {d2:+.1f}%)"
        else:
            if d1 > SIGNAL_TREND_PCT:
                trend_up  = True
                trend_str = f"hist. avg rises +{d1:.1f}% next hour"
            elif d1 < -SIGNAL_TREND_PCT:
                trend_down = True
                trend_str  = f"hist. avg falls {d1:.1f}% next hour"
            else:
                trend_str = f"flat next hour ({d1:+.1f}%)"

    # Confidence score — more samples + more future hours = higher confidence
    confidence_score = min(curr_n, 20) + (1 if next1 else 0) * 3 + (1 if next2 else 0) * 3
    if confidence_score >= 18:
        confidence = "High"
    elif confidence_score >= 10:
        confidence = "Medium"
    else:
        confidence = "Low"

    # Decision
    if pct_vs_now < -SIGNAL_LONG_PCT and trend_up:
        signal    = "LONG"
        reasoning = (
            f"CCU is {pct_vs_now:.1f}% below the {hour}:00 avg ({curr_avg:,.0f}). "
            f"{trend_str.capitalize()}. Expect recovery — consider longing."
        )
    elif pct_vs_now > SIGNAL_SHORT_PCT and trend_down:
        signal    = "SHORT"
        reasoning = (
            f"CCU is +{pct_vs_now:.1f}% above the {hour}:00 avg ({curr_avg:,.0f}). "
            f"{trend_str.capitalize()}. Expect pullback — consider shorting."
        )
    elif pct_vs_now < -SIGNAL_LONG_PCT:
        signal    = "HOLD"
        reasoning = (
            f"CCU {pct_vs_now:.1f}% below avg but {trend_str} — "
            f"dip present, no confirmed recovery trend yet."
        )
    elif pct_vs_now > SIGNAL_SHORT_PCT:
        signal    = "HOLD"
        reasoning = (
            f"CCU +{pct_vs_now:.1f}% above avg but {trend_str} — "
            f"elevated but no confirmed drop trend yet."
        )
    else:
        signal    = "HOLD"
        reasoning = (
            f"CCU {pct_vs_now:+.1f}% vs {hour}:00 avg ({curr_avg:,.0f}) — "
            f"within normal range. {trend_str.capitalize()}."
        )

    return {
        "signal":     signal,
        "confidence": confidence,
        "reasoning":  reasoning,
        "curr_avg":   curr_avg,
        "next1_avg":  next1_avg,
        "next2_avg":  next2_avg,
    }


# ───────────────────────────────────────────────────────────
# MAIN TICK
# ───────────────────────────────────────────────────────────
def run_tick():
    now_est    = datetime.now(timezone.utc) + timedelta(hours=TIMEZONE_OFFSET)
    is_weekend = now_est.weekday() >= 5
    today      = now_est.date()

    # ── Midnight rollover ────────────────────────────────────────
    if state.last_date != today:
        if state.last_date is not None:
            log.info("Day rollover detected — sending daily summary.")
            send_daily_summary()
        else:
            log.info(f"First tick of session ({today}) — intraday tracking started.")
        state.intraday_high  = 0
        state.intraday_low   = None
        state.intraday_sum   = 0
        state.intraday_ticks = 0
        state.last_date      = today

    # ── Fetch CCU ────────────────────────────────────────────────
    ccu = fetch_ccu()
    if ccu is None:
        return

    # ── Persist snapshot ─────────────────────────────────────────
    data = load_data()
    avg_hour, ath, is_new_ath = record_snapshot(data, is_weekend, now_est.hour, ccu, now_est)

    if state.midnight_ccu is None:
        state.midnight_ccu = ccu

    # ── Update intraday ──────────────────────────────────────────
    state.intraday_high   = max(state.intraday_high, ccu)
    state.intraday_low    = ccu if state.intraday_low is None else min(state.intraday_low, ccu)
    state.intraday_sum   += ccu
    state.intraday_ticks += 1

    # ── Deltas ───────────────────────────────────────────────────
    pct_15m, d_15m = diff_label(ccu, state.last_tick)
    pct_avg, d_avg = diff_label(ccu, int(avg_hour))
    _,       d_24h = diff_label(ccu, state.midnight_ccu)

    # ── Save everything ──────────────────────────────────────────
    persist_state(data, now_est)
    save_data(data)

    # ── Build chart ──────────────────────────────────────────────
    chart_png = None
    try:
        chart_png = render_chart(data["ticks"], avg_hour, ath)
    except Exception as e:
        log.warning(f"Chart render failed: {e}")

    # ── Signal engine ─────────────────────────────────────────────
    sig = compute_signal(data, ccu, now_est.hour, is_weekend)
    SIGNAL_EMOJI = {"LONG": "🟢", "SHORT": "🔴", "HOLD": "🟡", "INSUFFICIENT DATA": "⚪"}
    sig_emoji    = SIGNAL_EMOJI.get(sig["signal"], "⚪")
    log.info(f"Signal: {sig['signal']} ({sig['confidence']}) — {sig['reasoning']}")

    # ── Notification urgency ─────────────────────────────────────
    if is_new_ath:
        title    = f"🏆 NEW RECORD: {ccu:,}"
        priority = "5"
        tags     = "tada,fire"
    elif state.last_tick is not None and pct_15m > VOLATILITY_PCT:
        title    = f"📈 SPIKE: {ccu:,}"
        priority = "4"
        tags     = "chart_with_upwards_trend,warning"
    elif state.last_tick is not None and pct_15m < -VOLATILITY_PCT:
        title    = f"📉 DROP: {ccu:,}"
        priority = "4"
        tags     = "chart_with_downwards_trend,warning"
    elif sig["signal"] == "LONG":
        title    = f"🟢 LONG SIGNAL: {ccu:,}"
        priority = "4"
        tags     = "green_circle,moneybag"
    elif sig["signal"] == "SHORT":
        title    = f"🔴 SHORT SIGNAL: {ccu:,}"
        priority = "4"
        tags     = "red_circle,chart_with_downwards_trend"
    else:
        title    = f"{GAME_NAME} Ticker: {ccu:,}"
        priority = "3"
        tags     = "football"

    # ── Trend label ──────────────────────────────────────────────
    if   pct_avg >  BREAKOUT_PCT:  trend = "🔥 BREAKOUT"
    elif pct_avg < -DROP_AVG_PCT:  trend = "📉 BELOW AVG"
    else:                          trend = "⚖️ Stable"

    # ── Forecast line (next 1-2 hr avgs) ─────────────────────────
    forecast_parts = []
    if sig["next1_avg"] is not None:
        forecast_parts.append(f"{int(sig['next1_avg']):,}")
    if sig["next2_avg"] is not None:
        forecast_parts.append(f"{int(sig['next2_avg']):,}")
    forecast_str = " → ".join(forecast_parts) if forecast_parts else "not enough data"

    # ── Notification body ────────────────────────────────────────
    time_label  = now_est.strftime("%-I:%M %p")
    low_display = f"{state.intraday_low:,}" if state.intraday_low is not None else "—"
    message = (
        f"{trend}\n\n"
        f"15m       {d_15m}\n"
        f"vs Avg    {d_avg}  (avg {int(avg_hour):,})\n"
        f"Since ↑   {d_24h}\n\n"
        f"Today   ↑ {state.intraday_high:,}  ↓ {low_display}\n"
        f"ATH     🏆 {ath:,}\n\n"
        f"── Signal ──────────────────\n"
        f"{sig_emoji} {sig['signal']}  ({sig['confidence']} confidence)\n"
        f"{sig['reasoning']}\n"
        f"Forecast:  {forecast_str}"
    )

    send_notification(title, message, priority, tags, chart_png)

    state.last_tick = ccu
    log.info(f"Tick @ {time_label} — CCU: {ccu:,} | Avg: {int(avg_hour):,} | ATH: {ath:,}")


# ───────────────────────────────────────────────────────────
# CLOCK-SYNCED LOOP
# ───────────────────────────────────────────────────────────
def seconds_until_next_quarter() -> int:
    """Returns seconds until the next :00/:15/:30/:45 mark. Always 1–900."""
    now     = datetime.now()
    elapsed = (now.minute % 15) * 60 + now.second
    return 900 - elapsed


if __name__ == "__main__":
    if not REDIS_URL or not REDIS_TOKEN:
        log.error("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
        log.error("MISSING ENV VARS — Redis not configured. Add these to")
        log.error("Railway → your service → Variables:")
        log.error("  UPSTASH_REDIS_REST_URL    = <your REST URL>")
        log.error("  UPSTASH_REDIS_REST_TOKEN  = <your REST token>")
        log.error("Get both from console.upstash.com after creating a DB.")
        log.error("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
        raise SystemExit(1)

    restore_state(load_data())
    log.info(f"Starting {GAME_NAME} ticker → ntfy topic: {NTFY_TOPIC}")
    log.info(f"Redis connected: {REDIS_URL[:40]}...")

    while not _shutdown:
        wait = seconds_until_next_quarter()
        log.info(f"Syncing — next tick in {wait}s")
        for _ in range(wait):
            if _shutdown:
                break
            time.sleep(1)

        if not _shutdown:
            run_tick()
            # Sleep 5s so next seconds_until_next_quarter() doesn't fire twice
            time.sleep(5)

    log.info("Ticker stopped cleanly.")
