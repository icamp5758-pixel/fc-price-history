import requests

SOURCE = "https://raw.githubusercontent.com/JCH1231/fifa-simulation-data/main/price_history.json"
ENDPOINT = "https://m.fconline.nexon.com/datacenter/PlayerPriceGraph"

s = requests.Session()
s.headers.update({
    "User-Agent": "Mozilla/5.0",
    "Accept": "*/*",
    "Referer": "https://m.fconline.nexon.com/datacenter",
    "X-Requested-With": "XMLHttpRequest",
})

print("=== FC ONLINE PROBE ===")

r = s.get(SOURCE, timeout=60)
r.raise_for_status()
history = r.json()
print("players =", len(history))

for spid, player in history.items():
    values = player.get("values") or {}

    grades = [
        g for g in ("8", "9", "10", "11")
        if any(v not in (None, 0, "0") for v in (values.get(g) or []))
    ]

    if not grades:
        continue

    print("PLAYER =", spid, player.get("name"), player.get("season"))

    for grade in grades[:2]:
        x = s.post(
            ENDPOINT,
            data={"spid": str(spid), "n1strong": grade},
            timeout=30
        )

        print("GRADE =", grade)
        print("STATUS =", x.status_code)
        print("TYPE =", x.headers.get("content-type"))
        print("LENGTH =", len(x.content))
        print("BODY =", x.text[:3000])

        try:
            obj = x.json()
            print("JSON TYPE =", type(obj).__name__)
            if isinstance(obj, dict):
                print("JSON KEYS =", list(obj.keys()))
        except Exception as e:
            print("JSON ERROR =", repr(e))

    break

print("=== PROBE FINISHED ===")
