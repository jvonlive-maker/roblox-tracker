import requests
import time
import json
import os
from datetime import datetime, timezone, timedelta

# --- CONFIGURATION ---
UNIVERSE_ID = "3150475059" 
GAME_NAME = "FF2"
NTFY_TOPIC = "CCU_TICKER8312010" 
CHECK_INTERVAL = 900 
VOLATILITY_THRESHOLD = 20.0
TIMEZONE_OFFSET = -5  # EST
DATA_FILE = "ccu_stats_v2.json"

HEADERS = {"User-Agent": "Mozilla/5.0"}

# --- DATABASE LOGIC (Weekend vs Weekday Aware) ---
def load_stats():
    if os.path.exists(DATA_FILE):
        with open(DATA_FILE, 'r') as f: return json.load(f)
    return {"weekday": {}, "weekend": {}}

def save_stats(is_weekend, hour, ccu):
    stats = load_stats()
    group = "weekend" if is_weekend else "weekday"
    hour_key = str(hour)
    
    if hour_key not in stats[group]: stats[group][hour_key] = []
    stats[group][hour_key].append(ccu)
    stats[group][hour_key] = stats[group][hour_key][-30:] # Keep 30-day window
    
    with open(DATA_FILE, 'w') as f: json.dump(stats, f)
    return sum(stats[group][hour_key]) / len(stats[group][hour_key])

# --- TRACKING STATE ---
history = {"last_tick": None, "last_hour": None, "midnight_est": None, "last_day": None}

def get_diff_data(current, historical):
    """Returns (percent_float, formatted_string)"""
    if historical is None or historical == 0: 
        return 0.0, "n/a"
    diff = current - historical
    pct = (diff / historical * 100)
    sign = "+" if diff >= 0 else ""
    return pct, f"{sign}{diff:,} ({sign}{pct:.1f}%)"

def run_tick():
    global history
    now_est = datetime.now(timezone.utc) + timedelta(hours=TIMEZONE_OFFSET)
    current_hour = now_est.hour
    is_weekend = now_est.weekday() >= 5 # 5=Sat, 6=Sun
    
    try:
        r = requests.get(f"https://games.roblox.com/v1/games?universeIds={UNIVERSE_ID}", headers=HEADERS)
        ccu = r.json()['data'][0]['playing']

        # 1. Update/Get Historical Average (Smart grouping)
        avg_for_hour = save_stats(is_weekend, current_hour, ccu)
        
        # 2. Daily Logic Reset
        if history["last_day"] != now_est.date():
            history["midnight_est"] = ccu
            history["last_day"] = now_est.date()

        # 3. Calculate All Deltas (Unpacking fixed here)
        pct_15m, d_15m = get_diff_data(ccu, history["last_tick"])
        pct_24h, d_24h = get_diff_data(ccu, history["midnight_est"])
        pct_avg, d_avg = get_diff_data(ccu, avg_for_hour)

        # 4. Determine Volatility/Trend
        is_volatile = abs(pct_15m) >= VOLATILITY_THRESHOLD and history["last_tick"] is not None
        trend = "🟢" if ccu >= avg_for_hour else "🔴"
        day_type = "WEEKEND" if is_weekend else "WEEKDAY"
        
        # 5. Build Notification
        title = f"{'🚨' if is_volatile else trend} {GAME_NAME} @ {ccu:,}"
        message = (
            f"● vs {day_type} AVG ({now_est.strftime('%I%p')}): {d_avg}\n"
            f"● 15m Tick: {d_15m}\n"
            f"● Since 12AM EST: {d_24h}\n"
            f"---------------------------\n"
            f"VOLATILITY: {pct_15m:.1f}%\n"
            f"SIGNAL: {'🔥 BREAKOUT' if pct_avg > 15 else '📉 BELOW AVG' if pct_avg < -15 else '⚖️ NORMAL'}"
        )

        requests.post(f"https://ntfy.sh/{NTFY_TOPIC}", data=message.encode('utf-8'),
                      headers={
                          "Title": title, 
                          "Priority": "5" if is_volatile else "3", 
                          "Tags": "chart_with_upwards_trend,football"
                      })

        history["last_tick"] = ccu
        print(f"[{now_est.strftime('%H:%M')}] CCU: {ccu} | Avg: {avg_for_hour:.0f} | Vol: {pct_15m:.1f}%")

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    print(f"Ticker v3.1 Started. Tracking {GAME_NAME}...")
    while True:
        run_tick()
        time.sleep(CHECK_INTERVAL)
