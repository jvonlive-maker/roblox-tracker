import requests
import time
import json
import os
from datetime import datetime, timezone, timedelta

# --- CONFIGURATION ---
UNIVERSE_ID = "3150475059" 
GAME_NAME = "FF2"
NTFY_TOPIC = "CCU_TICKER8312010" 
CHECK_INTERVAL = 900 # 15 Minutes
TIMEZONE_OFFSET = -5  
DATA_FILE = "ccu_stats_v4.json"

# Set a manual floor for ATH so it doesn't ping for small numbers initially
ROBLOX_HISTORICAL_PEAK = 23798 

HEADERS = {"User-Agent": "Mozilla/5.0"}

def load_data():
    if os.path.exists(DATA_FILE):
        with open(DATA_FILE, 'r') as f: return json.load(f)
    return {"stats": {"weekday": {}, "weekend": {}}, "ath": ROBLOX_HISTORICAL_PEAK}

def save_snapshot(is_weekend, hour, ccu):
    data = load_data()
    group = "weekend" if is_weekend else "weekday"
    hour_key = str(hour)
    
    if hour_key not in data["stats"][group]: data["stats"][group][hour_key] = []
    data["stats"][group][hour_key].append(ccu)
    # Keep last 30 entries (approx 1 week of data per hour slot)
    data["stats"][group][hour_key] = data["stats"][group][hour_key][-30:]
    
    is_new_ath = False
    if ccu > data["ath"]:
        data["ath"] = ccu
        is_new_ath = True
        
    with open(DATA_FILE, 'w') as f: json.dump(data, f)
    avg = sum(data["stats"][group][hour_key]) / len(data["stats"][group][hour_key])
    return avg, data["ath"], is_new_ath

def get_diff_data(current, historical):
    if historical is None or historical == 0: 
        return 0.0, "First Run..."
    
    diff = current - historical
    pct = (diff / historical * 100)
    sign = "+" if diff >= 0 else ""
    # Returns raw percent and a formatted string
    return pct, f"{sign}{diff:,} ({sign}{pct:.1f}%)"

# Persistent session data
history = {"last_tick": None, "midnight_est": None, "last_day": None}

def run_tick():
    global history
    now_est = datetime.now(timezone.utc) + timedelta(hours=TIMEZONE_OFFSET)
    is_weekend = now_est.weekday() >= 5
    
    try:
        # 1. Fetch Data
        r = requests.get(f"https://games.roblox.com/v1/games?universeIds={UNIVERSE_ID}", headers=HEADERS)
        if r.status_code != 200:
            print(f"API Error: {r.status_code}")
            return
            
        ccu = r.json()['data'][0]['playing']
        avg_hour, ath, is_new_ath = save_snapshot(is_weekend, now_est.hour, ccu)
        
        # 2. Handle Time Logic
        if history["last_day"] != now_est.date():
            history["midnight_est"] = ccu
            history["last_day"] = now_est.date()

        # 3. Calculate Deltas
        pct_15m, d_15m = get_diff_data(ccu, history["last_tick"])
        pct_avg, d_avg = get_diff_data(ccu, avg_hour)
        _, d_24h = get_diff_data(ccu, history["midnight_est"])

        # 4. Determine Notification Urgency
        if is_new_ath:
            title = f"🏆 NEW RECORD: {ccu:,}"
            priority = "5"
            tags = "tada,fire"
        elif abs(pct_15m) > 20 and history["last_tick"] is not None:
            title = f"⚠️ VOLATILITY: {ccu:,}"
            priority = "4"
            tags = "chart_with_upwards_trend,warning"
        else:
            title = f"{GAME_NAME} Ticker: {ccu:,}"
            priority = "3"
            tags = "football"

        # 5. Format Beautiful Message
        trend_status = "🔥 BREAKOUT" if pct_avg > 15 else "📉 BELOW AVG" if pct_avg < -15 else "⚖️ STABLE"
        
        # Using Markdown-style formatting for better readability
        message = (
            f"**Current Status:** {trend_status}\n"
            f"**━━━━━━━━━━━━━━━━━━━━**\n"
            f"📊 **Performance vs {now_est.strftime('%I %p')} Avg:**\n"
            f"└ {d_avg}\n\n"
            f"⏱️ **Intervals:**\n"
            f"• 15m Change: {d_15m}\n"
            f"• Since Midnight: {d_24h}\n\n"
            f"🌟 **Session High:** {ath:,}\n"
            f"**━━━━━━━━━━━━━━━━━━━━**"
        ).strip()

        # 6. Send to ntfy
        requests.post(
            f"https://ntfy.sh/{NTFY_TOPIC}",
            data=message.encode('utf-8'),
            headers={
                "Title": title.strip(),
                "Priority": priority,
                "Tags": tags,
                "Markdown": "yes" # Tells ntfy to render bolding/bullets
            }
        )

        history["last_tick"] = ccu
        print(f"[{now_est.strftime('%H:%M')}] Tick: {ccu} | Avg: {avg_hour:.0f}")

    except Exception as e:
        print(f"Loop Error: {e}")

if __name__ == "__main__":
    print(f"Starting {GAME_NAME} Ticker on topic: {NTFY_TOPIC}")
    while True:
        run_tick()
        time.sleep(CHECK_INTERVAL)
