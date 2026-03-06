"""
config.py
Games are loaded from games.csv — just edit that file to add/remove games.
No need to touch this file.
"""

import csv, os

# ── Load games from CSV ───────────────────────────────────────────────────────
# games.csv columns: name, universe_id, ath_floor, csv_base, watchlist
# watchlist: true = full tracking (GAMES), false = scanner only (SCANNER_GAMES)
#
# Example row:
#   FF2,3150475059,47636,ff2,true

def _load_games():
    here     = os.path.dirname(os.path.abspath(__file__))
    csv_path = os.path.join(here, "games.csv")

    if not os.path.exists(csv_path):
        raise FileNotFoundError(
            f"games.csv not found at {csv_path}\n"
            f"Create it with columns: name,universe_id,ath_floor,csv_base,watchlist"
        )

    games   = []
    scanner = []
    seen    = set()

    with open(csv_path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            name = row["name"].strip()
            if not name or name.startswith("#"):
                continue  # skip blank lines and comments

            key = name.lower()
            if key in seen:
                print(f"[config] WARNING: duplicate game '{name}' in games.csv — skipped")
                continue
            seen.add(key)

            entry = {
                "name":        name,
                "universe_id": row["universe_id"].strip(),
                "redis_key":   f"ccu:{key}",
                "ntfy_topic":  "",
                "ath_floor":   int(row.get("ath_floor", 0) or 0),
                "csv_base":    row.get("csv_base", key).strip() or key,
            }

            if row.get("watchlist", "false").strip().lower() == "true":
                games.append(entry)
            else:
                # Scanner games get a separate redis key prefix
                entry["redis_key"] = f"scanner:{key}"
                scanner.append(entry)

    return games, scanner

GAMES, SCANNER_GAMES = _load_games()

# ── Shared settings ───────────────────────────────────────────────────────────
TIMEZONE_OFFSET = -5
DISCORD_WEBHOOK = "https://discord.com/api/webhooks/1478404038595969256/Pq71BDkMY9QavLKpep2MA9aqujUFgEmvCsyUD9FBYDdMOtNxwroPOpgqGVzVVevTTr7a"

# Alert thresholds
VOLATILITY_PCT = 20    # 15m change % to trigger SPIKE/DROP title
BREAKOUT_PCT   = 15    # % above slot avg to label BREAKOUT
DROP_AVG_PCT   = 15    # % below slot avg to label BELOW AVG

# Signal thresholds
SIGNAL_MIN_SAMPLES = 3    # minimum slot observations before signals fire
SIGNAL_LONG_PCT    = 8    # CCU must be this % below avg to consider LONG
SIGNAL_SHORT_PCT   = 8    # CCU must be this % above avg to consider SHORT
SIGNAL_TREND_PCT   = 3    # next-slot avg must move this % to confirm trend

# Legacy — kept for compatibility
CANDLES          = 5
TICKS_PER_CANDLE = 4
