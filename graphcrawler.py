import json, os, sys, time
from datetime import datetime, timezone
from pathlib import Path
import requests

SOURCE = os.getenv(
    "SEED_URL",
    "https://raw.githubusercontent.com/JCH1231/fifa-simulation-data/main/price_history.json",
)
OUT = Path("price_history.json")
ENDPOINT = os.getenv(
    "FC_PRICE_ENDPOINT",
    "https://m.fconline.nexon.com/datacenter/PlayerPriceGraph",
)
GRADES = [8, 9, 10, 11]
TIMEOUT = 20

session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0 (Linux; Android 10) AppleWebKit/537.36 Chrome/126 Safari/537.36",
    "Accept": "application/json,text/plain,*/*",
    "Referer": "https://m.fconline.nexon.com/datacenter",
})

def load_history():
    if OUT.exists() and OUT.stat().st_size > 10:
        return json.loads(OUT.read_text(encoding="utf-8"))
    r = session.get(SOURCE, timeout=60)
    r.raise_for_status()
    data = r.json()
    if not isinstance(data, dict) or not data:
        raise RuntimeError("seed price_history.json is empty")
    return data

def parse_chart(payload):
    # FC Online responses have changed shape before, so search common chart containers.
    if isinstance(payload, dict):
        for key in ("chartData", "data", "result"):
            if key in payload:
                found = parse_chart(payload[key])
                if found:
                    return found
        # mapping timestamp -> price
        pairs = []
        for k, v in payload.items():
            try:
                price = int(str(v).replace(",", ""))
                pairs.append((str(k), price))
            except Exception:
                pass
        if pairs:
            return pairs
    if isinstance(payload, list):
        pairs = []
        for row in payload:
            if isinstance(row, dict):
                t = row.get("date") or row.get("dt") or row.get("time") or row.get("x")
                v = row.get("price") or row.get("value") or row.get("y")
                if t is not None and v is not None:
                    try: pairs.append((str(t), int(str(v).replace(",", ""))))
                    except Exception: pass
            elif isinstance(row, (list, tuple)) and len(row) >= 2:
                try: pairs.append((str(row[0]), int(str(row[1]).replace(",", ""))))
                except Exception: pass
        if pairs:
            return pairs
    return []

def fetch(spid, grade):
    # Uses the same public DataCenter graph request fields used by the mobile page.
    r = session.post(
        ENDPOINT,
        data={"spid": str(spid), "n1strong": str(grade)},
        timeout=TIMEOUT,
    )
    r.raise_for_status()
    try:
        payload = r.json()
    except Exception:
        return []
    return parse_chart(payload)

def norm_time(x):
    s = str(x).strip()
    if s.isdigit():
        n = int(s)
        if n > 10_000_000_000: n //= 1000
        return datetime.fromtimestamp(n, timezone.utc).isoformat()
    return s

def merge_player(player, grade, pairs):
    if not pairs:
        return False
    times = list(player.get("times") or [])
    values = player.setdefault("values", {})
    vals = list(values.get(str(grade)) or [])
    # Preserve existing aligned history.
    existing = {}
    for i, t in enumerate(times):
        if i < len(vals) and vals[i] not in (None, 0, "0"):
            existing[str(t)] = vals[i]
    for t, v in pairs:
        if v > 0:
            existing[norm_time(t)] = int(v)
    if not existing:
        return False

    # Build a shared time axis without deleting other grades.
    all_times = set(map(str, times)) | set(existing.keys())
    def key(t):
        try:
            return datetime.fromisoformat(t.replace("Z","+00:00")).timestamp()
        except Exception:
            try: return float(t)
            except Exception: return 0
    new_times = sorted(all_times, key=key)

    old_values = player.get("values", {})
    rebuilt = {}
    old_index = {str(t): i for i, t in enumerate(times)}
    for g, old in old_values.items():
        m = {}
        for t, i in old_index.items():
            if i < len(old) and old[i] not in (None, 0, "0"):
                m[t] = old[i]
        if str(g) == str(grade):
            m.update(existing)
        rebuilt[str(g)] = [m.get(t) for t in new_times]

    player["times"] = new_times
    player["values"] = rebuilt
    return True

def main():
    history = load_history()
    total = len(history)
    changed = 0
    errors = 0

    # Safety: update the existing player universe only; metadata/name/season/teamColors are preserved.
    for idx, (spid, player) in enumerate(history.items(), 1):
        for grade in GRADES:
            try:
                pairs = fetch(spid, grade)
                if merge_player(player, grade, pairs):
                    changed += 1
            except Exception as e:
                errors += 1
            time.sleep(0.04)
        if idx % 250 == 0:
            print(f"{idx}/{total} players, changed={changed}, errors={errors}", flush=True)

    # Never overwrite good history with an empty/broken result.
    if not history or total < 100:
        raise RuntimeError("safety stop: history unexpectedly small")

    tmp = OUT.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(history, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
    tmp.replace(OUT)
    print(f"done players={total}, changed={changed}, errors={errors}")

if __name__ == "__main__":
    main()
