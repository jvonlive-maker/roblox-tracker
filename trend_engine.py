"""
trend_engine.py
---------------
Trend detection engine for the multi-game CCU ticker.

Analyses each game's stored slot/tick data every tick and fires Discord alerts
when a meaningful structural pattern is confirmed, changed, or broken.

Detected patterns (all derived purely from the data already in Redis):
  1. DOW cycle   — which day of week is the structural peak/trough for this game
  2. DOW spike   — a single day that consistently spikes far above neighbors
  3. Peak-hour   — what hour of the current DOW historically has the highest CCU
  4. Decay slope — rate of post-peak daily decay (e.g. Fri→Thu chain)
  5. Reversal    — today is at or below the structural trough (entry signal)
  6. Breakdown   — structural peak day is significantly weaker than its own history
  7. Macro trend — rolling 4-week vs 8-week average divergence (growth / decline)
  8. Anomaly     — current value is an outlier vs same-slot history (event / crash)
  9. Intraday momentum — 10-min momentum relative to slot average (acceleration)

Usage: call run_trend_analysis(game, data, ccu, now_est) once per tick, after
       record_snapshot() has run. It returns a list of TrendSignal objects and
       optionally fires Discord embeds for any that are NEW or CHANGED.

Drop-in integration into ticker.py run_tick():
    from trend_engine import run_trend_analysis
    trend_signals = run_trend_analysis(game, data, ccu, now_est)

config keys used (all optional, sensible defaults provided):
    game.get("trend_spike_ratio", 2.0)       — min ratio for spike-day detection
    game.get("trend_macro_weeks", 4)         — rolling window for macro comparison
    game.get("trend_anomaly_sigma", 2.5)     — std-dev threshold for anomaly alerts
    game.get("trend_alert_cooldown_h", 6)    — hours between re-firing same pattern
"""

import logging
import math
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass, field
from typing import Optional

log = logging.getLogger(__name__)

DAYS = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]

# ── Signal dataclass ──────────────────────────────────────────────────────────

@dataclass
class TrendSignal:
    kind: str            # e.g. "DOW_SPIKE", "REVERSAL", "ANOMALY_HIGH", …
    title: str
    body: str
    color: int           # Discord embed colour
    confidence: str      # "High" | "Medium" | "Low"
    is_new: bool = True  # False = same signal as last tick (suppressed)

# ── Colour palette ────────────────────────────────────────────────────────────

COL = {
    "green":  0x43E97B,
    "red":    0xEF233C,
    "yellow": 0xF9C74F,
    "blue":   0x90E0EF,
    "purple": 0x7B5EA7,
    "orange": 0xFF6B35,
    "grey":   0x7A7F94,
}

# ── Helpers ───────────────────────────────────────────────────────────────────

def _slot(data, dow, hour):
    return data["slots"].get(f"{dow}_{hour % 24}")

def _slot_avg(data, dow, hour):
    s = _slot(data, dow, hour)
    return s["avg"] if s and s["n"] >= 3 else None

def _min_samples(data, n=6):
    """True if at least n slots have enough data to trust DOW analysis."""
    return sum(1 for s in data["slots"].values() if s.get("n", 0) >= 4) >= n

def _recent_ticks(data, n=96):
    """Return the last n ticks as (datetime, ccu) pairs."""
    out = []
    for t in data.get("ticks", [])[-n:]:
        try:
            out.append((datetime.strptime(t["ts"], "%Y-%m-%dT%H:%M"), t["ccu"]))
        except ValueError:
            pass
    return out

def _weekly_avg_from_slots(data, dow):
    """Average of all slot averages for a given DOW (ignores hours with no data)."""
    vals = [s["avg"] for k, s in data["slots"].items()
            if k.startswith(f"{dow}_") and s.get("n", 0) >= 3]
    return sum(vals) / len(vals) if vals else None

def _all_dow_avgs(data):
    """Dict {dow: avg_ccu} for all 7 days."""
    return {d: _weekly_avg_from_slots(data, d) for d in range(7)}

def _rolling_weekly_avgs(ticks, weeks=4):
    """
    From tick history, compute week-level average CCU for the last `weeks` full weeks.
    Returns a list of floats (oldest first), or [] if insufficient data.
    """
    if not ticks:
        return []
    from collections import defaultdict
    by_week = defaultdict(list)
    for dt, ccu in ticks:
        iso = dt.isocalendar()
        key = f"{iso[0]}-W{iso[1]:02d}"
        by_week[key].append(ccu)
    sorted_keys = sorted(by_week)[-weeks:]
    return [sum(by_week[k]) / len(by_week[k]) for k in sorted_keys if by_week[k]]

def _cooldown_ok(data, key, hours):
    """True if at least `hours` have passed since key was last stored in trend_state."""
    ts_store = data.get("trend_state", {}).get(key)
    if not ts_store:
        return True
    try:
        last = datetime.fromisoformat(ts_store)
        return (datetime.now(timezone.utc) - last).total_seconds() > hours * 3600
    except (ValueError, TypeError):
        return True

def _mark_fired(data, key, now_est):
    ts = data.setdefault("trend_state", {})
    ts[key] = now_est.isoformat()

# ── Individual detectors ──────────────────────────────────────────────────────

def _detect_dow_spike(game, data, now_est, cooldown_h):
    """
    Detects if one DOW has a structurally anomalous average vs its neighbors.
    Fires once per week per game (when that day arrives).
    E.g. FLSV Friday is 3.1× Thursday — a very obvious structural spike.
    """
    signals = []
    dow_avgs = _all_dow_avgs(data)
    if sum(1 for v in dow_avgs.values() if v) < 5:
        return signals

    spike_ratio = game.get("trend_spike_ratio", 1.8)

    for d in range(7):
        if dow_avgs.get(d) is None:
            continue
        neighbors = [dow_avgs.get((d - 1) % 7), dow_avgs.get((d + 1) % 7)]
        neighbors = [n for n in neighbors if n]
        if not neighbors:
            continue
        neighbor_avg = sum(neighbors) / len(neighbors)
        ratio = dow_avgs[d] / neighbor_avg if neighbor_avg else 0

        if ratio >= spike_ratio:
            key = f"dow_spike_{d}"
            if not _cooldown_ok(data, key, cooldown_h * 24 * 3):  # re-fire weekly at most
                continue
            # Only fire on the day itself (or the day before as a preview)
            if now_est.weekday() in (d, (d - 1) % 7):
                _mark_fired(data, key, now_est)
                prev_day = DAYS[(d - 1) % 7]
                next_day = DAYS[(d + 1) % 7]
                signals.append(TrendSignal(
                    kind="DOW_SPIKE",
                    title=f"📈 Structural Spike Day: {DAYS[d]}",
                    body=(
                        f"Historical data confirms {DAYS[d]} is a structural CCU spike day.\n\n"
                        f"Avg CCU:      {int(dow_avgs[d]):,}\n"
                        f"vs {prev_day}:       {int(dow_avgs[(d-1)%7]):,}  ({ratio:.1f}× ratio)\n"
                        f"vs {next_day}:       {int(dow_avgs[(d+1)%7]):,}\n\n"
                        f"Signal: LONG setup — {DAYS[d]} CCU expected to be significantly above recent days."
                    ),
                    color=COL["yellow"],
                    confidence="High" if ratio >= 2.5 else "Medium",
                ))

    return signals


def _detect_reversal(game, data, ccu, dow, hour, now_est, cooldown_h):
    """
    Fires when current CCU is near the structural weekly trough AND the next
    day is historically a much higher day — classic buy-the-dip signal.
    """
    signals = []
    dow_avgs = _all_dow_avgs(data)
    if sum(1 for v in dow_avgs.values() if v) < 5:
        return signals

    curr_dow_avg = dow_avgs.get(dow)
    if not curr_dow_avg:
        return signals

    # Find structural trough day
    valid = {d: v for d, v in dow_avgs.items() if v}
    trough_dow = min(valid, key=valid.get)
    trough_avg = valid[trough_dow]

    # Find structural peak day
    peak_dow = max(valid, key=valid.get)
    peak_avg = valid[peak_dow]

    # Current day must BE the trough day (or within 10% of trough avg)
    if dow != trough_dow and (curr_dow_avg / trough_avg) > 1.15:
        return signals

    # The next structural high must be soon (within 3 days)
    days_to_peak = (peak_dow - dow) % 7
    if days_to_peak == 0 or days_to_peak > 3:
        return signals

    ratio = peak_avg / trough_avg
    if ratio < 1.5:
        return signals  # not a meaningful reversal

    key = f"reversal_{trough_dow}"
    if not _cooldown_ok(data, key, cooldown_h * 18):
        return signals

    _mark_fired(data, key, now_est)
    signals.append(TrendSignal(
        kind="REVERSAL",
        title=f"🔄 Reversal Setup — {DAYS[trough_dow]} Trough",
        body=(
            f"Currently at structural weekly LOW ({DAYS[trough_dow]} avg {int(trough_avg):,}).\n"
            f"Historical peak day ({DAYS[peak_dow]}) is {days_to_peak} day(s) away "
            f"— avg {int(peak_avg):,} (+{(ratio-1)*100:.0f}% uplift expected).\n\n"
            f"Live CCU:    {ccu:,}\n"
            f"Trough avg:  {int(trough_avg):,}\n"
            f"Peak avg:    {int(peak_avg):,}\n"
            f"Expected ×:  {ratio:.1f}×\n\n"
            f"Signal: LONG — buy the trough, ride to {DAYS[peak_dow]} spike."
        ),
        color=COL["green"],
        confidence="High" if ratio >= 2.5 else "Medium",
    ))
    return signals


def _detect_peak_breakdown(game, data, ccu, dow, hour, now_est, cooldown_h):
    """
    Fires when the structural spike day is underperforming its own history —
    the spike is weaker than expected (possible loss of momentum / game decline).
    """
    signals = []
    dow_avgs = _all_dow_avgs(data)
    valid = {d: v for d, v in dow_avgs.items() if v}
    if len(valid) < 5:
        return signals

    peak_dow = max(valid, key=valid.get)
    if dow != peak_dow:
        return signals

    # Compare current CCU to the slot average for this hour on peak day
    slot = _slot(data, dow, hour)
    if not slot or slot["n"] < 5:
        return signals

    pct_vs_avg = (ccu - slot["avg"]) / slot["avg"] * 100

    # Only flag if meaningfully below (not just normal noise)
    if pct_vs_avg > -25:
        return signals

    key = f"peak_breakdown_{dow}"
    if not _cooldown_ok(data, key, cooldown_h):
        return signals

    _mark_fired(data, key, now_est)
    severity = "SEVERE" if pct_vs_avg < -40 else "MODERATE"
    signals.append(TrendSignal(
        kind="PEAK_BREAKDOWN",
        title=f"⚠️ {severity} Peak Breakdown — {DAYS[peak_dow]}",
        body=(
            f"{DAYS[peak_dow]} is the structural spike day but CCU is running "
            f"{abs(pct_vs_avg):.0f}% below its own historical average.\n\n"
            f"Live CCU:      {ccu:,}\n"
            f"Slot avg:      {int(slot['avg']):,}\n"
            f"Deviation:     {pct_vs_avg:+.0f}%\n"
            f"Slot samples:  {slot['n']}\n\n"
            f"Signal: {'SHORT — structural support failing.' if pct_vs_avg < -40 else 'HOLD — watch for recovery.'}"
        ),
        color=COL["orange"] if severity == "MODERATE" else COL["red"],
        confidence="High" if slot["n"] >= 10 else "Medium",
    ))
    return signals


def _detect_decay_acceleration(game, data, ccu, dow, now_est, cooldown_h):
    """
    Fires when the daily decay rate is accelerating beyond normal — 
    the trough is getting deeper week-over-week (structural game decline).
    """
    signals = []
    ticks = _recent_ticks(data, n=200)
    if len(ticks) < 40:
        return signals

    # Collect same-DOW CCU over recent weeks
    dow_vals = [(dt, v) for dt, v in ticks if dt.weekday() == dow]
    if len(dow_vals) < 4:
        return signals

    # Compare last 2 to prior 2
    recent = [v for _, v in dow_vals[-2:]]
    prior  = [v for _, v in dow_vals[-4:-2]]
    if not prior:
        return signals

    recent_avg = sum(recent) / len(recent)
    prior_avg  = sum(prior)  / len(prior)
    if prior_avg == 0:
        return signals

    decay_pct = (recent_avg - prior_avg) / prior_avg * 100

    # Only report if accelerating decline beyond normal seasonal noise
    if decay_pct > -15:
        return signals

    key = f"decay_accel_{dow}"
    if not _cooldown_ok(data, key, cooldown_h * 4):
        return signals

    _mark_fired(data, key, now_est)
    signals.append(TrendSignal(
        kind="DECAY_ACCELERATION",
        title=f"📉 Decay Accelerating — {DAYS[dow]}",
        body=(
            f"{DAYS[dow]} CCU is declining week-over-week beyond normal seasonal patterns.\n\n"
            f"Recent 2-week avg:  {int(recent_avg):,}\n"
            f"Prior 2-week avg:   {int(prior_avg):,}\n"
            f"Δ:                  {decay_pct:+.1f}%\n\n"
            f"Signal: BEARISH — structural weekly floor is eroding. "
            f"Reduce expectations for {DAYS[dow]} peaks."
        ),
        color=COL["red"],
        confidence="Medium",
    ))
    return signals


def _detect_macro_trend(game, data, now_est, cooldown_h):
    """
    Compares 4-week rolling average vs 8-week rolling average.
    Detects: UPTREND, DOWNTREND, TREND_REVERSAL (cross).
    Fires at most once per 4 days.
    """
    signals = []
    ticks = _recent_ticks(data, n=2016)  # up to ~3 months of 15-min ticks
    if not ticks:
        return signals

    avgs_8 = _rolling_weekly_avgs(ticks, weeks=8)
    avgs_4 = _rolling_weekly_avgs(ticks, weeks=4)

    if len(avgs_8) < 4 or len(avgs_4) < 2:
        return signals

    slow = sum(avgs_8[-4:]) / 4   # 4-week slice of the 8-week window
    fast = sum(avgs_4[-2:]) / 2   # most recent 2 weeks

    if slow == 0:
        return signals

    pct = (fast - slow) / slow * 100

    prev_macro = data.get("trend_state", {}).get("macro_direction", "FLAT")

    if pct > 10:
        direction = "UP"
        is_reversal = prev_macro in ("DOWN", "FLAT")
    elif pct < -10:
        direction = "DOWN"
        is_reversal = prev_macro in ("UP", "FLAT")
    else:
        return signals  # flat, skip

    key = "macro_trend"
    if not _cooldown_ok(data, key, cooldown_h * 4 * 24):  # fire at most every ~4 days
        # Still update stored direction without firing
        data.setdefault("trend_state", {})["macro_direction"] = direction
        return signals

    _mark_fired(data, key, now_est)
    data.setdefault("trend_state", {})["macro_direction"] = direction

    if is_reversal and direction == "UP":
        title = "📈 Macro Reversal — RECOVERY"
        body_intro = "4-week average has crossed ABOVE the 8-week average — macro uptrend beginning."
        color = COL["green"]
    elif is_reversal and direction == "DOWN":
        title = "📉 Macro Reversal — DECLINE"
        body_intro = "4-week average has crossed BELOW the 8-week average — macro downtrend beginning."
        color = COL["red"]
    elif direction == "UP":
        title = "📈 Macro Trend: UPTREND"
        body_intro = "4-week average remains above 8-week — uptrend intact."
        color = COL["green"]
    else:
        title = "📉 Macro Trend: DOWNTREND"
        body_intro = "4-week average remains below 8-week — downtrend intact."
        color = COL["red"]

    signals.append(TrendSignal(
        kind="MACRO_TREND",
        title=title,
        body=(
            f"{body_intro}\n\n"
            f"4-week avg:  {int(fast):,}\n"
            f"8-week avg:  {int(slow):,}\n"
            f"Δ:           {pct:+.1f}%\n\n"
            f"Signal: {'LONG — macro tailwind.' if direction == 'UP' else 'SHORT — macro headwind.'}"
        ),
        color=color,
        confidence="High" if abs(pct) > 20 else "Medium",
    ))
    return signals


def _detect_anomaly(game, data, ccu, dow, hour, now_est, cooldown_h):
    """
    Fires when the current CCU is an extreme outlier vs its slot history —
    2.5σ above (event/spike) or 2.5σ below (crash/outage).
    """
    signals = []
    slot = _slot(data, dow, hour)
    if not slot or slot["n"] < 6 or slot.get("std", 0) == 0:
        return signals

    sigma = game.get("trend_anomaly_sigma", 2.5)
    z = (ccu - slot["avg"]) / slot["std"]

    if abs(z) < sigma:
        return signals

    direction = "HIGH" if z > 0 else "LOW"
    key = f"anomaly_{direction}"
    if not _cooldown_ok(data, key, cooldown_h):
        return signals

    _mark_fired(data, key, now_est)

    if direction == "HIGH":
        title = f"🚨 Anomaly: SPIKE (+{z:.1f}σ)"
        body = (
            f"CCU is {z:.1f} standard deviations ABOVE slot average — possible game event, "
            f"viral moment, or update.\n\n"
            f"Live CCU:    {ccu:,}\n"
            f"Slot avg:    {int(slot['avg']):,}\n"
            f"Slot std:    {int(slot['std']):,}\n"
            f"z-score:     +{z:.1f}σ\n\n"
            f"Signal: LONG momentum — event-driven spike. Monitor for sustainability."
        )
        color = COL["orange"]
    else:
        title = f"🚨 Anomaly: CRASH ({z:.1f}σ)"
        body = (
            f"CCU is {abs(z):.1f} standard deviations BELOW slot average — possible outage, "
            f"update breaking content, or mass player exodus.\n\n"
            f"Live CCU:    {ccu:,}\n"
            f"Slot avg:    {int(slot['avg']):,}\n"
            f"Slot std:    {int(slot['std']):,}\n"
            f"z-score:     {z:.1f}σ\n\n"
            f"Signal: SHORT — investigate root cause before trading."
        )
        color = COL["red"]

    conf = "High" if abs(z) >= 3.5 else "Medium"
    signals.append(TrendSignal(kind=f"ANOMALY_{direction}", title=title, body=body,
                               color=color, confidence=conf))
    return signals


def _detect_peak_hour(game, data, dow, hour, now_est, cooldown_h):
    """
    Once per day, identifies the expected peak hour for today's DOW
    and announces it if it's within the next 3 hours.
    """
    signals = []
    best_h, best_avg = -1, -1
    for h in range(24):
        avg = _slot_avg(data, dow, h)
        if avg and avg > best_avg:
            best_avg = avg
            best_h = h

    if best_h < 0:
        return signals

    hours_away = (best_h - hour) % 24
    if hours_away == 0 or hours_away > 3:
        return signals

    key = f"peak_hour_{dow}"
    if not _cooldown_ok(data, key, 20):  # once per day
        return signals

    _mark_fired(data, key, now_est)
    curr_avg = _slot_avg(data, dow, hour) or 1
    uplift_pct = (best_avg - curr_avg) / curr_avg * 100

    if uplift_pct < 15:
        return signals  # not worth alerting

    signals.append(TrendSignal(
        kind="PEAK_HOUR",
        title=f"⏰ Peak Hour Incoming — {DAYS[dow]} {best_h:02d}:00",
        body=(
            f"Historical peak for {DAYS[dow]} is {best_h:02d}:00 — {hours_away} hour(s) away.\n\n"
            f"Current avg ({hour:02d}:00):  {int(curr_avg):,}\n"
            f"Peak avg    ({best_h:02d}:00):  {int(best_avg):,}\n"
            f"Expected uplift:     +{uplift_pct:.0f}%\n\n"
            f"Signal: Position LONG before {best_h:02d}:00."
        ),
        color=COL["blue"],
        confidence="High" if data["slots"].get(f"{dow}_{best_h}", {}).get("n", 0) >= 8 else "Medium",
    ))
    return signals


def _detect_intraday_momentum(game, data, ccu, dow, hour, now_est, cooldown_h):
    """
    Detects sustained intraday acceleration: if the last 3 ticks are all moving
    in the same direction AND the rate exceeds what's historically normal for
    this slot, flag it as momentum signal.
    """
    signals = []
    ticks = data.get("ticks", [])
    if len(ticks) < 4:
        return signals

    recent = ticks[-4:]
    try:
        vals = [t["ccu"] for t in recent]
    except (KeyError, TypeError):
        return signals

    deltas = [vals[i+1] - vals[i] for i in range(3)]

    # All same direction?
    if not (all(d > 0 for d in deltas) or all(d < 0 for d in deltas)):
        return signals

    # Is the magnitude notable vs slot std?
    slot = _slot(data, dow, hour)
    if not slot or slot.get("std", 0) == 0:
        return signals

    avg_delta = sum(abs(d) for d in deltas) / 3
    if avg_delta < slot["std"] * 0.5:
        return signals  # normal noise

    direction = "UP" if deltas[0] > 0 else "DOWN"
    key = f"momentum_{direction}"
    if not _cooldown_ok(data, key, cooldown_h * 0.5):
        return signals

    _mark_fired(data, key, now_est)

    total_move = vals[-1] - vals[0]
    pct_move   = total_move / max(vals[0], 1) * 100

    if direction == "UP":
        signals.append(TrendSignal(
            kind="MOMENTUM_UP",
            title=f"🚀 Intraday Momentum: ACCELERATING",
            body=(
                f"3 consecutive up ticks with above-average magnitude.\n\n"
                f"Δ ticks:   {deltas[0]:+}  {deltas[1]:+}  {deltas[2]:+}\n"
                f"Total:     {total_move:+,} ({pct_move:+.1f}%)\n"
                f"Slot σ:    {int(slot['std']):,}\n\n"
                f"Signal: SHORT-TERM LONG — sustained buying pressure."
            ),
            color=COL["green"],
            confidence="Medium",
        ))
    else:
        signals.append(TrendSignal(
            kind="MOMENTUM_DOWN",
            title=f"📉 Intraday Momentum: DECLINING",
            body=(
                f"3 consecutive down ticks with above-average magnitude.\n\n"
                f"Δ ticks:   {deltas[0]:+}  {deltas[1]:+}  {deltas[2]:+}\n"
                f"Total:     {total_move:+,} ({pct_move:+.1f}%)\n"
                f"Slot σ:    {int(slot['std']):,}\n\n"
                f"Signal: SHORT-TERM SHORT — sustained selling pressure."
            ),
            color=COL["red"],
            confidence="Medium",
        ))
    return signals


# ── Master runner ─────────────────────────────────────────────────────────────

def run_trend_analysis(game, data, ccu, now_est, send_fn=None):
    """
    Run all trend detectors. Call once per tick after record_snapshot().

    Parameters
    ----------
    game      : game config dict from config.GAMES
    data      : loaded Redis data dict (will be mutated to store cooldown state)
    ccu       : current CCU int
    now_est   : datetime in EST
    send_fn   : optional callable(game, title, body, sig_dict) — if provided,
                fires Discord embeds for each new signal. Pass your notify()
                function here, or None to skip Discord and just return signals.

    Returns
    -------
    list[TrendSignal]
    """
    if not _min_samples(data, n=5):
        return []  # not enough history to say anything meaningful

    dow          = now_est.weekday()
    hour         = now_est.hour
    cooldown_h   = game.get("trend_alert_cooldown_h", 6)

    all_signals = []

    detectors = [
        lambda: _detect_dow_spike(game, data, now_est, cooldown_h),
        lambda: _detect_reversal(game, data, ccu, dow, hour, now_est, cooldown_h),
        lambda: _detect_peak_breakdown(game, data, ccu, dow, hour, now_est, cooldown_h),
        lambda: _detect_decay_acceleration(game, data, ccu, dow, now_est, cooldown_h),
        lambda: _detect_macro_trend(game, data, now_est, cooldown_h),
        lambda: _detect_anomaly(game, data, ccu, dow, hour, now_est, cooldown_h),
        lambda: _detect_peak_hour(game, data, dow, hour, now_est, cooldown_h),
        lambda: _detect_intraday_momentum(game, data, ccu, dow, hour, now_est, cooldown_h),
    ]

    for detector in detectors:
        try:
            results = detector()
            all_signals.extend(results)
        except Exception as e:
            log.warning(f"[{game['name']}] Trend detector error: {e}", exc_info=True)

    # Fire Discord notifications
    if send_fn and all_signals:
        for sig in all_signals:
            try:
                send_fn(game, sig.title, sig.body, {"signal": "HOLD", "color_override": sig.color})
            except Exception as e:
                log.warning(f"[{game['name']}] Trend notify error: {e}")

    if all_signals:
        names = [s.kind for s in all_signals]
        log.info(f"[{game['name']}] Trend signals: {names}")

    return all_signals
