"""
test_tick.py
Runs one immediate tick for every game in config.py without
waiting for the 15-min clock. Sends real Discord notifications
so you can see exactly what the message looks like.

Usage:
  UPSTASH_REDIS_REST_URL=x UPSTASH_REDIS_REST_TOKEN=y python3 test_tick.py

Optional flags:
  --dry-run     skip Discord, just print the message to terminal
  --game FF2    only test one game
"""

import sys, os
sys.path.insert(0, ".")

# ── Parse args ────────────────────────────────────────────
dry_run    = "--dry-run" in sys.argv
filter_game = None
if "--game" in sys.argv:
    idx = sys.argv.index("--game")
    if idx + 1 < len(sys.argv):
        filter_game = sys.argv[idx + 1].upper()

# ── Patch notify to print instead of sending (dry-run) ───
import ticker

if dry_run:
    def fake_notify(game, title, message, sig):
        print(f"\n{'='*55}")
        print(f"TITLE:   {title}")
        print(f"SIGNAL:  {sig.get('signal')} ({sig.get('confidence')})")
        print(f"{'─'*55}")
        print(message)
        print(f"{'='*55}\n")
    ticker.notify = fake_notify
    print("DRY RUN — Discord notifications suppressed\n")

# ── Run ───────────────────────────────────────────────────
import config

games = [g for g in config.GAMES
         if filter_game is None or g["name"].upper() == filter_game]

if not games:
    print(f"No game matching '{filter_game}' found in config.py")
    sys.exit(1)

for game in games:
    print(f"Testing: {game['name']}")
    data = ticker.load_data(game)
    ticker.restore_state(game, data)
    try:
        ticker.run_tick(game)
        print(f"  ✓ {game['name']} tick completed")
    except Exception as e:
        import traceback
        print(f"  ✗ {game['name']} FAILED:")
        traceback.print_exc()

print("\nDone.")
