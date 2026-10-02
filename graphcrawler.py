import json
import os
import re
import time
from pathlib import Path

import requests

SOURCE = os.getenv(
    "SEED_URL",
    "https://raw.githubusercontent.com/JCH1231/fifa-simulation-data/main/price_history.json",
)

ENDPOINT = "https://m.fconline.nexon.com/datacenter/PlayerPriceGraph"
OUT = Path("price_history.json")

GRADES = [8, 9, 10, 11]

session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0 (Linux; Android 10) AppleWebKit/537.36 Chrome/126 Safari/537.36",
    "Accept": "*/*",
    "Referer": "https://m.fconline.nexon.com/datacenter",
    "X-Requested-With": "XMLHttpRequest",
})


def load_history():
    if OUT.exists() and OUT.stat().st_size > 10:
        return json.loads(OUT.read_text(encoding="utf-8"))

    r = session.get(SOURCE, timeout=60)
    r.raise_for_status()
    return r.json()


def extract_array(block, name):
    m = re.search(
        rf"{name}\s*:\s*\[(.*?)\]",
        block,
        re.S
    )

    if not m:
        return []

    raw = m.group(1)

    return [
        x.strip().strip("'\"")
        for x in raw.split(",")
        if x.strip()
    ]


def extract_chart(html):
    # FC Online 실제 응답:
    # var chartData = {
    #     time: [...],
    #     value: [...]
    # }

    m = re.search(
        r"var\s+chartData\s*=\s*\{(.*?)\}\s*;",
        html,
        re.S
    )

    if not m:
        return []

    block = m.group(1)

    times = extract_array(block, "time")
    values = extract_array(block, "value")

    pairs = []

    for t, v in zip(times, values):
        try:
            price = int(
                str(v)
                .replace(",", "")
                .replace('"', "")
                .replace("'", "")
                .strip()
            )

            if price > 0:
                pairs.append((str(t).strip(), price))

        except Exception:
            pass

    return pairs


def fetch_price(spid, grade):
    r = session.post(
        ENDPOINT,
        data={
            "spid": str(spid),
            "n1strong": str(grade),
        },
        timeout=30,
    )

    r.raise_for_status()

    return extract_chart(r.text)


def merge(player, grade, pairs):
    if not pairs:
        return False

    old_times = list(player.get("times") or [])
    old_values = player.get("values") or {}

    maps = {}

    # 기존 가격 보존
    for g, arr in old_values.items():

        maps[str(g)] = {}

        for i, t in enumerate(old_times):

            if i < len(arr):
                v = arr[i]

                if v not in (None, 0, "0"):
                    maps[str(g)][str(t)] = v

    grade = str(grade)

    if grade not in maps:
        maps[grade] = {}

    before = dict(maps[grade])

    # 새 가격 추가/갱신
    for t, price in pairs:
        maps[grade][str(t)] = int(price)

    all_times = set()

    for data in maps.values():
        all_times.update(data.keys())

    def sort_date(x):
        try:
            month, day = str(x).split(".")
            return int(month), int(day)
        except Exception:
            return 0, 0

    new_times = sorted(all_times, key=sort_date)

    rebuilt = {}

    for g, data in maps.items():
        rebuilt[g] = [
            data.get(t)
            for t in new_times
        ]

    player["times"] = new_times
    player["values"] = rebuilt

    return before != maps[grade]


def main():

    history = load_history()

    total = len(history)

    success = 0
    changed = 0
    errors = 0

    print("PLAYERS =", total, flush=True)

    for index, (spid, player) in enumerate(history.items(), 1):

        for grade in GRADES:

            try:

                pairs = fetch_price(spid, grade)

                if pairs:

                    success += 1

                    if merge(player, grade, pairs):
                        changed += 1

            except Exception as e:

                errors += 1

                if errors <= 10:
                    print(
                        "ERROR",
                        spid,
                        grade,
                        repr(e),
                        flush=True
                    )

            time.sleep(0.03)

        if index % 100 == 0:

            print(
                f"{index}/{total} "
                f"success={success} "
                f"changed={changed} "
                f"errors={errors}",
                flush=True
            )

        # 또 1시간 헛돌지 않도록 안전장치
        if index == 100 and success == 0:

            raise RuntimeError(
                "STOP: first 100 players returned zero chart data"
            )

    if success == 0:
        raise RuntimeError("No price data extracted")

    tmp = Path("price_history.tmp.json")

    tmp.write_text(
        json.dumps(
            history,
            ensure_ascii=False,
            separators=(",", ":")
        ),
        encoding="utf-8"
    )

    tmp.replace(OUT)

    print(
        f"DONE players={total} "
        f"success={success} "
        f"changed={changed} "
        f"errors={errors}",
        flush=True
    )


if __name__ == "__main__":
    main()
