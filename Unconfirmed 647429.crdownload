"""
lookup_server.py
Tiny web server that runs game lookups via HTTP.
Deploy alongside ticker on Railway, hit the URL to look up games.

Usage (after deploying):
  Single game:
    https://your-app.railway.app/lookup?id=8204899140

  Multiple games:
    https://your-app.railway.app/lookup?id=8204899140&id=2753915549

  Returns JSON + a ready-to-paste games.csv block.

Requires: pip install flask requests
"""

import re, os
import requests as req
from flask import Flask, request, jsonify

app = Flask(__name__)

http = req.Session()
http.headers.update({"User-Agent": "Mozilla/5.0"})

# ── Roblox API ────────────────────────────────────────────────────────────────

def get_universe_id(place_id):
    r = http.get(
        f"https://apis.roblox.com/universes/v1/places/{place_id}/universe",
        timeout=10
    )
    r.raise_for_status()
    return r.json()["universeId"]

def get_game_info(universe_id):
    r = http.get(
        f"https://games.roblox.com/v1/games?universeIds={universe_id}",
        timeout=10
    )
    r.raise_for_status()
    d = r.json()["data"][0]
    return d["name"], d["playing"], d["rootPlaceId"]

def lookup(place_id):
    place_id = str(place_id).strip()
    try:
        universe_id = get_universe_id(place_id)
    except Exception as e:
        return {"place_id": place_id, "error": f"universe lookup failed: {e}"}

    try:
        name, live_ccu, root_place_id = get_game_info(universe_id)
    except Exception as e:
        return {"place_id": place_id, "error": f"game info failed: {e}"}

    csv_base = re.sub(r"[^a-z0-9]", "_", name.lower())[:20].strip("_")

    return {
        "place_id":    str(root_place_id),
        "universe_id": str(universe_id),
        "name":        name,
        "live_ccu":    live_ccu,
        "csv_base":    csv_base,
        "csv_line":    f"{name},{universe_id},0,{csv_base},false",
    }

# ── Routes ────────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return (
        "<h2>Game Lookup</h2>"
        "<p>Hit <code>/lookup?id=PLACE_ID</code> to look up a game.</p>"
        "<p>Multiple: <code>/lookup?id=111&id=222&id=333</code></p>"
        "<p>Returns JSON with name, universe ID, and games.csv line.</p>"
    )

@app.route("/lookup")
def lookup_route():
    place_ids = request.args.getlist("id")
    if not place_ids:
        return jsonify({"error": "provide at least one ?id=PLACE_ID"}), 400

    results  = []
    csv_lines = []
    for pid in place_ids:
        r = lookup(pid)
        results.append(r)
        if "csv_line" in r:
            csv_lines.append(r["csv_line"])

    return jsonify({
        "results":   results,
        "csv_block": "\n".join(csv_lines),
    })

# ── Run ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
