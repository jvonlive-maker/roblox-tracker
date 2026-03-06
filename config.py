"""
config.py — edit this to add/remove games.

csv_base: prefix for the three CSV files:
  <csv_base>_daily.csv    (daily since release)
  <csv_base>_hourly.csv   (hourly, last month)
  <csv_base>_10min.csv    (10-min, last week)
"""

GAMES = [
    {
        "name":        "FF2",
        "universe_id": "3150475059",
        "redis_key":   "ccu:ff2",
        "ntfy_topic":  "",
        "ath_floor":   47636,
        "csv_base":    "ff2",
    },
    {
        "name":        "UFLU",
        "universe_id": "184199275",
        "redis_key":   "ccu:uflu",
        "ntfy_topic":  "",
        "ath_floor":   49793,
        "csv_base":    "uflu",
    },
    {
        "name":        "ENPT",
        "universe_id": "324740177",
        "redis_key":   "ccu:enpt",
        "ntfy_topic":  "",
        "ath_floor":   5378,
        "csv_base":    "enpt",
    },
    {
        "name":        "FLSV",
        "universe_id": "3747388906",
        "redis_key":   "ccu:flsv",
        "ntfy_topic":  "",
        "ath_floor":   7348,
        "csv_base":    "flsv",
    },
    
]

# Shared settings
TIMEZONE_OFFSET = -5

DISCORD_WEBHOOK = "https://discord.com/api/webhooks/1478404038595969256/Pq71BDkMY9QavLKpep2MA9aqujUFgEmvCsyUD9FBYDdMOtNxwroPOpgqGVzVVevTTr7a"

VOLATILITY_PCT     = 20
BREAKOUT_PCT       = 15
DROP_AVG_PCT       = 15

SIGNAL_MIN_SAMPLES = 3
SIGNAL_LONG_PCT    = 8
SIGNAL_SHORT_PCT   = 8
SIGNAL_TREND_PCT   = 3

CANDLES          = 5
TICKS_PER_CANDLE = 4
