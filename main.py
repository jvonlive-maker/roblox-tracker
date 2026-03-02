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
DATA_FILE = "ccu_stats_v3.json"

HEADERS = {"User-Agent": "Mozilla/5.0"}

# --- DATABASE LOGIC ---
def load_data():
    if os.path.exists(DATA_FILE):
        with open(DATA_FILE, 'r') as f: return json.load(f)
    return {"stats": {"weekday": {}, "weekend": {}}, "ath": 0}

def save_snapshot(is_weekend, hour, ccu):
    data = load_data()
    group = "weekend" if is_weekend else "weekday"
    hour_key = str(hour)
    
    # Update Averages
    if hour_key not in data["stats"][group]: data["stats"][group][hour_key] = []
    data["stats"][group][hour_key].append(ccu)
    data["stats"][group][hour_key] = data["stats"][group][hour_key][-30:]
    
    # Update ATH
    is_new_ath = False
    if ccu > data["ath"]:
        data["ath"] = ccu
        is_new_ath = True
        
    with open(DATA_FILE, 'w') as f: json.dump(data, f)
    return sum(data["stats"][group][hour_key]) / len(data["stats"][group][hour_key]), data["ath"], is_new_ath

def get_diff_data(current, historical):
    if historical is None or historical == 0: return 0.0, "n/a"
    diff = current - historical
    pct = (diff / historical * 100)
    sign = "+" if diff >= 0 else ""
    return pct, f"{sign}{diff:,} ({sign}{pct:.1f}%)"

# --- TRACKING STATE ---
history = {"last_tick": None, "midnight_est": None, "last_day": None}

def run_tick():
    global history
    now_est = datetime.now(timezone.utc) + timedelta(hours=TIMEZONE_OFFSET)
    is_weekend = now_est.weekday() >= 5
    
    try:
        r = requests.get(f"https://games.roblox.com/v1/games?universeIds={UNIVERSE_ID}", headers=HEADERS)
        ccu = r.json()['data'][0]['playing']

        avg_hour, ath, is_new_ath = save_snapshot(is_weekend, now_est.hour, ccu)
        
        if history["last_day"] != now_est.date():
            history["midnight_est"] = ccu
            history["last_day"] = now_est.date()

        pct_15m, d_15m = get_diff_data(ccu, history["last_tick"])
        pct_24h, d_24h = get_diff_data(ccu, history["midnight_est"])
        pct_avg, d_avg = get_diff_data(ccu, avg_hour)

        # --- LOGIC & FORMATTING ---
        is_volatile = abs(pct_15m) >= VOLATILITY_THRESHOLD and history["last_tick"] is not None
        
        # We use standard text for Title to avoid the Latin-1 encoding error
        title = f"FF2 Update: {ccu:,} CCU"
        if is_new_ath: title = f"🏆 NEW RECORD: {ccu:,} CCU"
        elif is_volatile: title = f"VOLATILITY ALERT: {ccu:,} CCU"

        # TradingView Style Body (Encoded to UTF-8 to handle emojis)
        trend_emoji = "🟩" if ccu >= avg_hour else "🟥"
        message = (
            f"{trend_emoji} vs {now_est.strftime('%I%p')} Avg: {d_avg}\n"
            f"● 15m Change: {d_15m}\n"
            f"● Since 12AM EST: {d_24h}\n"
            f"● ATH (Session): {ath:,}\n"
            f"---------------------------\n"
            f"MARKET: {'🔥 BREAKOUT' if pct_avg > 15 else '📉 BELOW AVG' if pct_avg < -15 else '⚖️ NORMAL'}"
        )

        # Send to ntfy
        requests.post(
            f"https://ntfy.sh/{NTFY_TOPIC}",
            data=message.encode('utf-8'), # Encoding the BODY is safe
            headers={
                "Title": title.encode('ascii', 'ignore').decode('ascii'), # Strips emojis from title to fix your error
                "Priority": "5" if (is_volatile or is_new_ath) else "3",
                "Tags": "chart_with_upwards_trend,football" if ccu >= avg_hour else "chart_with_downwards_trend,football"
            }
        )

        history["last_tick"] = ccu
        print(f"[{now_est.strftime('%H:%M')}] CCU: {ccu} | ATH: {ath}")

    except Exception as e:
        print(f"Loop Error: {e}")

if __name__ == "__main__":
    while True:
        run_tick()
        time.sleep(CHECK_INTERVAL)
