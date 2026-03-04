Starting Container
[11:33:57] INFO: [FF2] Resumed — last CCU: 273
[11:33:57] INFO: Tracking: FF2  (universe 3150475059)
[11:33:57] INFO: [UFLU] Resumed — last CCU: 737
[11:33:57] INFO: Tracking: UFLU  (universe 184199275)
[11:33:57] INFO: Redis: https://probable-elf-61889.upstash.io...
[11:33:57] INFO: Discord: enabled
[11:33:57] INFO: Syncing — next tick in 663s
  File "/app/ticker.py", line 597, in run_tick
    peak_str = f"Peak  {pk[0]} {pk[2]:02d}:00  avg {int(pk[1]):,}" if pk else ""
[11:45:01] ERROR: [UFLU] Unhandled error: Unknown format code 'd' for object of type 'float'
               ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Traceback (most recent call last):
    peak_str = f"Peak  {pk[0]} {pk[2]:02d}:00  avg {int(pk[1]):,}" if pk else ""
  File "/app/ticker.py", line 687, in <module>
               ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    run_tick(game)
ValueError: Unknown format code 'd' for object of type 'float'
[11:45:00] INFO: [FF2] Next tick: 407 (+50 / +14.0%)  Medium  [baseline +60 | momentum +38]
[11:45:00] ERROR: [FF2] Unhandled error: Unknown format code 'd' for object of type 'float'
Traceback (most recent call last):
[11:45:00] INFO: [FF2] Signal: HOLD (Low) cv=51.2% — -0.7% vs adj. avg — avg rises +66.7% → +86.7% over 2h
  File "/app/ticker.py", line 687, in <module>
    run_tick(game)
  File "/app/ticker.py", line 597, in run_tick
[11:45:01] INFO: [UFLU] Signal: HOLD (Low) cv=49.9% — +8.0% vs adj. avg — avg rises +59.1% → +76.6% over 2h
[11:45:01] INFO: [UFLU] Next tick: 1,165 (+142 / +13.9%)  Medium  [baseline +140 | momentum +145]
ValueError: Unknown format code 'd' for object of type 'float'
[11:45:06] INFO: Syncing — next tick in 894s
