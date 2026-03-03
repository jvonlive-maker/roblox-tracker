"""
config.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Add or remove games here. Each game gets its own Redis key
and ntfy topic. Discord webhook is shared across all games.

To add a new game:
  1. Add an entry to GAMES below
  2. Drop its CSV (named <csv_seed>) into the repo root
  3. Run seed_redis.py — seeds ALL games automatically
  4. Redeploy the ticker

To stop tracking a game: remove its entry and redeploy.
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

GAMES = [
    {
        "name":        "FF2",
        "universe_id": "3150475059",
        "redis_key":   "ccu:ff2",
        "ntfy_topic":  "CCU_TICKER8312010",
        "ath_floor":   23798,
        "csv_seed":    "ff2_players.csv",       # rename your CSV to this
    },
    # Add more games below — uncomment and fill in:
    # {
    #     "name":        "Jailbreak",
    #     "universe_id": "606849621",
    #     "redis_key":   "ccu:jailbreak",
    #     "ntfy_topic":  "CCU_TICKER_JB",
    #     "ath_floor":   0,
    #     "csv_seed":    "jailbreak_players.csv",
    # },
]

# ── Shared settings ───────────────────────────────────────────
TIMEZONE_OFFSET = -5   # EST

# Discord webhook — all games post here (set "" to disable)
DISCORD_WEBHOOK = "https://discord.com/api/webhooks/1478404038595969256/Pq71BDkMY9QavLKpep2MA9aqujUFgEmvCsyUD9FBYDdMOtNxwroPOpgqGVzVVevTTr7a"

# Alert thresholds
VOLATILITY_PCT     = 20
BREAKOUT_PCT       = 15
DROP_AVG_PCT       = 15

# Signal engine
SIGNAL_MIN_SAMPLES = 3
SIGNAL_LONG_PCT    = 8
SIGNAL_SHORT_PCT   = 8
SIGNAL_TREND_PCT   = 3

# Chart
CANDLES            = 5
TICKS_PER_CANDLE   = 4
