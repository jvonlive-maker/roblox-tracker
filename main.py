import requests
import time

# --- CONFIGURATION ---
UNIVERSE_ID = "3150475059" 
GAME_NAME = "Football Fusion 2"
NTFY_TOPIC = "CCU_TICKER8312010" 
CHECK_INTERVAL = 900 

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}
# ---------------------

last_ccu = None

def run_tick():
    global last_ccu
    url = f"https://games.roblox.com/v1/games?universeIds={UNIVERSE_ID}"
    
    try:
        response = requests.get(url, headers=HEADERS)
        if response.status_code == 200:
            data = response.json()['data'][0]
            current_ccu = data['playing']
            
            if last_ccu is None:
                diff_text = "First check"
                status_emoji = "🏈"
            else:
                diff = current_ccu - last_ccu
                diff_text = f"{diff:+d}" 
                status_emoji = "📈" if diff > 0 else "📉" if diff < 0 else "↔️"

            msg = f"{status_emoji} {GAME_NAME}\nCCU: {current_ccu:,}\nChange: {diff_text}"
            requests.post(f"https://ntfy.sh/{NTFY_TOPIC}", data=msg.encode('utf-8'), headers={"Title": "CCU Update"})
            
            print(f"[{time.strftime('%H:%M:%S')}] {current_ccu} ({diff_text})")
            last_ccu = current_ccu
        else:
            print(f"Error {response.status_code}: {response.text}")
    except Exception as e:
        print(f"Update error: {e}")

while True:
    run_tick()
    time.sleep(CHECK_INTERVAL)
