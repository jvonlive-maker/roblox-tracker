"""
lookup.py
Give it one or more Roblox place IDs and it returns:
  - Game name
  - Universe ID
  - ATH (peak players from Rolimons)
  - Ready-to-paste games.csv line

Usage:
  python3 lookup.py 480700
  python3 lookup.py 480700 1537690962 142823291
  python3 lookup.py --file place_ids.txt   (one place ID per line)

Requires: pip install requests
"""

import re, sys, time
import requests

http = requests.Session()
http.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Referer":    "https://www.rolimons.com/",
})

# ── API calls ─────────────────────────────────────────────────────────────────

def rolimons_game(place_id):
    """
    Hits rolimons.com/gameapi/game/<place_id>.
    Returns the full JSON dict or raises on failure.
    Known fields: name, universe_id, active_players, peak_player_count, visits, ...
    """
    url = f"https://www.rolimons.com/gameapi/game/{place_id}"
    r   = http.get(url, timeout=15)
    r.raise_for_status()
    return r.json()

def roblox_universe(place_id):
    """Fallback: Roblox API to get universe ID if Rolimons doesn't return it."""
    r = http.get(
        f"https://apis.roblox.com/universes/v1/places/{place_id}/universe",
        timeout=10
    )
    r.raise_for_status()
    return r.json()["universeId"]

# ── Core lookup ───────────────────────────────────────────────────────────────

def lookup(place_id):
    place_id = str(place_id).strip()
    print(f"\n🔍 Place ID: {place_id}")

    # Pull from Rolimons
    try:
        data = rolimons_game(place_id)
    except Exception as e:
        print(f"   ❌ Rolimons failed: {e}")
        return None

    name        = data.get("name") or data.get("game_name") or "UNKNOWN"
    universe_id = data.get("universe_id") or data.get("universeId")
    live_ccu    = data.get("active_players", 0) or 0
    ath         = data.get("peak_player_count") or data.get("peak_ccu") or data.get("all_time_peak")

    # If Rolimons didn't return universe_id, fall back to Roblox API
    if not universe_id:
        try:
            universe_id = roblox_universe(place_id)
        except Exception as e:
            print(f"   ⚠️  Universe ID fallback failed: {e}")
            universe_id = "UNKNOWN"

    # If no ATH from Rolimons, use live CCU as floor
    ath_src = "Rolimons peak"
    if not ath:
        ath     = live_ccu
        ath_src = "live CCU floor — check Rolimons manually for true peak"

    # Safe csv_base: lowercase alphanumeric, max 20 chars
    csv_base = re.sub(r"[^a-z0-9]", "_", name.lower())[:20].strip("_")

    print(f"   Name        : {name}")
    print(f"   Universe ID : {universe_id}")
    print(f"   Live CCU    : {live_ccu:,}")
    print(f"   ATH         : {ath:,}  ({ath_src})")
    print(f"   games.csv   : {name},{universe_id},{ath},{csv_base},false")

    # Print all raw fields in debug mode
    if "--debug" in sys.argv:
        import json
        print(f"\n   Raw response:")
        print(json.dumps(data, indent=4))

    return {
        "place_id":    place_id,
        "universe_id": str(universe_id),
        "name":        name,
        "live_ccu":    live_ccu,
        "ath":         ath,
        "csv_base":    csv_base,
    }

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    args = [a for a in sys.argv[1:] if not a.startswith("--")]

    if not args:
        print("Usage:")
        print("  python3 lookup.py <place_id> [place_id2] ...")
        print("  python3 lookup.py --file place_ids.txt")
        print("  python3 lookup.py 3150475059 --debug   (show raw API response)")
        sys.exit(1)

    place_ids = []

    if "--file" in sys.argv:
        idx = sys.argv.index("--file")
        if idx + 1 >= len(sys.argv):
            print("ERROR: --file requires a filename")
            sys.exit(1)
        with open(sys.argv[idx + 1], encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#"):
                    place_ids.append(line)
    else:
        place_ids = args

    results = []
    for pid in place_ids:
        r = lookup(pid)
        if r:
            results.append(r)
        time.sleep(0.5)  # be polite to Rolimons

    if len(results) > 1:
        print(f"\n{'='*60}")
        print("Paste into games.csv (scanner section):")
        print(f"{'='*60}")
        for r in results:
            print(f"{r['name']},{r['universe_id']},{r['ath']},{r['csv_base']},false")

if __name__ == "__main__":
    main()
