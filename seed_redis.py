"""
seed_redis.py - seeds ALL games in config.GAMES from CSV files.
Run once before starting the ticker:
  UPSTASH_REDIS_REST_URL=x UPSTASH_REDIS_REST_TOKEN=y python3 seed_redis.py

CSV format (Rolimons): DateTime,Players
"""
import csv, json, os, sys
from collections import defaultdict
from datetime import datetime, timedelta
from urllib.parse import quote
import requests, config

REDIS_URL   = os.environ.get("UPSTASH_REDIS_REST_URL",   "")
REDIS_TOKEN = os.environ.get("UPSTASH_REDIS_REST_TOKEN", "")
MAX_SAMPLES = 30
http = requests.Session()

def _hdrs(): return {"Authorization": f"Bearer {REDIS_TOKEN}"}
def redis_get(k):
    r = http.get(f"{REDIS_URL}/get/{quote(k,safe='')}", headers=_hdrs(), timeout=10)
    r.raise_for_status()
    v = r.json().get("result")
    return v if isinstance(v, str) else None
def redis_set(k, v):
    http.get(f"{REDIS_URL}/set/{quote(k,safe='')}/{quote(v,safe='')}", headers=_hdrs(), timeout=15).raise_for_status()

def parse_csv(path):
    rows = []
    with open(path, encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            dt = datetime.strptime(row["DateTime"].strip(), "%Y-%m-%d %H:%M:%S") + timedelta(hours=config.TIMEZONE_OFFSET)
            rows.append((dt, int(row["Players"])))
    return rows

def seed_game(game):
    print(f"Seeding {game['name']}...")
    if not os.path.exists(game["csv_seed"]):
        print(f"  SKIP: {game['csv_seed']} not found"); return
    raw = parse_csv(game["csv_seed"])
    print(f"  {len(raw)} rows ({raw[0][0].date()} to {raw[-1][0].date()})")
    dedup = {}
    for dt, ccu in raw: dedup[dt] = ccu
    rows = sorted(dedup.items())
    bkts = {"weekday": defaultdict(list), "weekend": defaultdict(list)}
    for dt, ccu in rows:
        bkts["weekend" if dt.weekday()>=5 else "weekday"][dt.hour].append((dt,ccu))
    stats = {"weekday":{}, "weekend":{}}
    for g in ("weekday","weekend"):
        for hr, ents in bkts[g].items():
            clk = defaultdict(list)
            for dt,ccu in ents: clk[(dt.date(),dt.hour)].append(ccu)
            stats[g][str(hr)] = [c for _,c in sorted((k,round(sum(v)/len(v))) for k,v in clk.items())][-MAX_SAMPLES:]
    ticks = [{"ts":dt.strftime("%Y-%m-%dT%H:%M"),"ccu":ccu} for dt,ccu in rows][-120:]
    ath   = max(max(c for _,c in rows), game["ath_floor"])
    ex = {}
    try:
        r = redis_get(game["redis_key"])
        if r: ex = json.loads(r); print(f"  Found existing ATH={ex.get('ath'):,}")
        else: print("  No existing data")
    except Exception as e: print(f"  Warning: {e}")
    ms = {"weekday":{},"weekend":{}}
    for g in ("weekday","weekend"):
        eg = ex.get("stats",{}).get(g,{})
        for h in set(list(stats[g])+list(eg)):
            cs,ls = stats[g].get(h,[]), eg.get(h,[])
            ms[g][h] = ls if len(ls)>=len(cs) else (cs+ls)[-MAX_SAMPLES:]
    et = ex.get("ticks",[])
    ets = {t["ts"] for t in et}
    mt = ([t for t in ticks if t["ts"] not in ets]+et)[-120:]
    ma = max(ath, ex.get("ath",0))
    data = {"stats":ms,"ath":ma,"session":ex.get("session",{}),"ticks":mt}
    redis_set(game["redis_key"], json.dumps(data, separators=(",",":")))
    print(f"  Done! ATH={ma:,}  ticks={len(mt)}")
    for g in ("weekday","weekend"):
        print(f"  {g.upper()}")
        for h in sorted(ms[g],key=int):
            v=ms[g][h]; print(f"    {int(h):02d}:00  n={len(v)}  avg={sum(v)/len(v):.0f}")

def main():
    if not REDIS_URL or not REDIS_TOKEN: print("ERROR: set Redis env vars"); sys.exit(1)
    for game in config.GAMES: seed_game(game)
    print("All done. Run: python3 ticker.py")

if __name__=="__main__": main()
