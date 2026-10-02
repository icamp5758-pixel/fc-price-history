import re
import requests

SOURCE = "https://raw.githubusercontent.com/JCH1231/fifa-simulation-data/main/price_history.json"
ENDPOINT = "https://m.fconline.nexon.com/datacenter/PlayerPriceGraph"

s = requests.Session()
s.headers.update({
    "User-Agent": "Mozilla/5.0 (Linux; Android 10) AppleWebKit/537.36 Chrome/126 Safari/537.36",
    "Accept": "*/*",
    "Referer": "https://m.fconline.nexon.com/datacenter",
    "X-Requested-With": "XMLHttpRequest",
})

history = s.get(SOURCE, timeout=60).json()

spid = next(iter(history))
player = history[spid]

print("=== SORTEDVALUES PROBE ===")
print("PLAYER =", spid, player.get("name"))

r = s.post(
    ENDPOINT,
    data={"spid": str(spid), "n1strong": "8"},
    timeout=30
)

print("STATUS =", r.status_code)
print("LENGTH =", len(r.text))

html = r.text

# sortedValues가 등장하는 모든 위치 확인
positions = [m.start() for m in re.finditer(r"sortedValues", html)]
print("sortedValues COUNT =", len(positions))

for i, pos in enumerate(positions[:10], 1):
    print("\n========== MATCH", i, "==========")
    start = max(0, pos - 1500)
    end = min(len(html), pos + 5000)
    print(html[start:end])

print("\n=== PROBE FINISHED ===")
