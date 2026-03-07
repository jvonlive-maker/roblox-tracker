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
        # sim price — calibrate with elasticity_finder.lua, recalibrate every few days
        "sim_base_price": 133.64,
        "sim_elasticity": -0.0105,
        "sim_best_model": "rev",
        "sim_rev_alpha": 0.0500,
        "sim_ema_alpha": 0.3000,
        "sim_price_std": 1.0566,
        "sim_ci95": 2.0709,
    },
    {
        "name":        "UFLU",
        "universe_id": "184199275",
        "redis_key":   "ccu:uflu",
        "ntfy_topic":  "",
        "ath_floor":   49793,
        "csv_base":    "uflu",
        # sim price — run elasticity_finder.lua on UFLU and paste values here
        "sim_base_price": 283.71,
        "sim_elasticity": 0.0472,
        "sim_best_model": "ens",
        "sim_rev_alpha": 0.0500,
        "sim_ema_alpha": 0.3000,
        "sim_holt_alpha": 0.6000,
        "sim_holt_beta": 0.2000,
    },
    {
        "name":        "ENPT",
        "universe_id": "324740177",
        "redis_key":   "ccu:enpt",
        "ntfy_topic":  "",
        "ath_floor":   5378,
        "csv_base":    "enpt",
        # sim price — from elasticity_finder.lua (Rev R²=0.879)
        "sim_base_price": 10.24,
        "sim_elasticity": -0.1760,
        "sim_best_model": "rev",
        "sim_rev_alpha": 0.0500,
        "sim_ema_alpha": 0.0500,
        "sim_price_std": 0.0824,
        "sim_ci95": 0.1614,
    },
    {
        "name":        "FLSV",
        "universe_id": "3747388906",
        "redis_key":   "ccu:flsv",
        "ntfy_topic":  "",
        "ath_floor":   7348,
        "csv_base":    "flsv",
        # sim price — run elasticity_finder.lua on FLSV and paste values here
        "sim_base_price": 37.75,
        "sim_elasticity": -0.0883,
        "sim_best_model": "rev",
        "sim_rev_alpha": 0.0500,
        "sim_ema_alpha": 0.0500,
        "sim_price_std": 0.2658,
        "sim_ci95": 0.5210,
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
