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
DATA_FILE = "ccu_stats.json"

HEADERS = {"User-Agent": "Mozilla/5.0"}

# --- DATABASE LOGIC ---
def load_stats():
    if os.path.exists(DATA_FILE):
        with open(DATA_FILE, 'r') as f: return json.load(f)
    return {} # Format: {"14": [list of historical CCUs for 2pm]}

def save_stats(hour, ccu):
    stats = load_stats()
    hour_key = str(hour)
    if hour_key not in stats: stats[hour_key] = []
    stats[hour_key].append(ccu)
    # Keep only last 30 days of data for the average
    stats[hour_key] = stats[hour_key][-30:] 
    with open(DATA_FILE, 'w') as f: json.dump(stats, f)
    return sum(stats[hour_key]) / len(stats[hour_key])

# --- TRACKING STATE ---
history = {"last_tick": None, "last_hour": None, "midnight_est": None, "last_day": None}

def get_diff_text(current, historical):
    if historical is None or historical == 0: return "n/a"
    diff = current - historical
    pct = (diff / historical * 100)
    return pct, f"{'+' if diff >= 0 else ''}{diff:,} ({'+' if diff >= 0 else ''}{pct:.1f}%)"

def run_tick():
    global history
    now_est = datetime.now(timezone.utc) + timedelta(hours=TIMEZONE_OFFSET)
    current_hour = now_est.hour
    
    try:
        r = requests.get(f"https://games.roblox.com/v1/games?universeIds={UNIVERSE_ID}", headers=HEADERS)
        ccu = r.json()['data'][0]['playing']

        # 1. Update/Get Historical Average for this hour
        avg_for_hour = save_stats(current_hour, ccu)
        
        # 2. Daily/Tick Logic
        if history["last_day"] != now_est.date():
            history["midnight_est"] = ccu
            history["last_day"] = now_est.date()

        # 3. Comparisons
        pct_15m, d_15m = get_diff_text(ccu, history["last_tick"])
        _, d_24h = get_diff_text(ccu, history["midnight_est"])
        pct_avg, d_avg = get_diff_text(ccu, avg_for_hour)

        # 4. Determine Signal
        is_volatile = abs(pct_15m) >= VOLATILITY_THRESHOLD and history["last_tick"] is not None
        trend = "🟢" if ccu >= avg_for_hour else "🔴"
        
        title = f"{'🚨' if is_volatile else trend} {GAME_NAME} @ {ccu:,}"
        message = (
            f"● Vs Avg for {now_est.strftime('%I %p')}: {d_avg}\n"
            f"● 15m Change: {d_15m}\n"
            f"● Since 12AM EST: {d_24h}\n"
            f"---------------------------\n"
            f"VOLATILITY: {pct_15m:.1f}%\n"
            f"MARKET: {'🔥 BREAKOUT' if pct_avg > 15 else '📉 BELOW AVG' if pct_avg < -15 else '⚖️ NORMAL'}"
        )

        requests.post(f"https://ntfy.sh/{NTFY_TOPIC}", data=message.encode('utf-8'),
                      headers={"Title": title, "Priority": "5" if is_volatile else "3", 
                               "Tags": "heavy_dollar_sign,football" if pct_avg > 10 else "football"})

        history["last_tick"] = ccu
        print(f"Update Sent: {ccu} (Avg for {current_hour}:h: {avg_for_hour:.0f})")

    except Exception as e: print(f"Error: {e}")

if __name__ == "__main__":
    while True:
        run_tick()
        time.sleep(CHECK_INTERVAL)
