"""FC Online graph crawler: original parser/merge, bounded parallel I/O."""
import copy
import json
import math
import os
import random
import re
import tempfile
import threading
import time
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path

import requests

SOURCE = os.getenv(
    "SEED_URL",
    "https://raw.githubusercontent.com/JCH1231/fifa-simulation-data/main/price_history.json",
)
ENDPOINT = "https://m.fconline.nexon.com/datacenter/PlayerPriceGraph"
OUT = Path("price_history.json")
GRADES = [8, 9, 10, 11]
WORKERS = int(os.getenv("WORKERS", "12"))
REQUESTS_PER_SECOND = float(os.getenv("REQUESTS_PER_SECOND", "16"))
MAX_ATTEMPTS = 4  # 최초 요청 포함
CHECKPOINT_SECONDS = 300
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Linux; Android 10) AppleWebKit/537.36 Chrome/126 Safari/537.36",
    "Accept": "*/*",
    "Referer": "https://m.fconline.nexon.com/datacenter",
    "X-Requested-With": "XMLHttpRequest",
}
STOP = threading.Event()
LOCAL = threading.local()


class RateLimiter:
    """모든 스레드/재시도에 동일한 시작 간격과 서버 대기시간 적용."""

    def __init__(self, rate):
        if not math.isfinite(rate) or rate <= 0:
            raise ValueError("REQUESTS_PER_SECOND must be finite and > 0")
        self.interval = 1.0 / rate
        self.next_at = 0.0
        self.paused_until = 0.0
        self.lock = threading.Lock()

    def acquire(self):
        while not STOP.is_set():
            with self.lock:
                now = time.monotonic()
                delay = max(self.next_at, self.paused_until) - now
                if delay <= 0:
                    self.next_at = now + self.interval
                    return
            STOP.wait(delay)
        raise RuntimeError("Collection stopped")

    def pause(self, seconds):
        with self.lock:
            self.paused_until = max(
                self.paused_until, time.monotonic() + seconds
            )


LIMITER = RateLimiter(REQUESTS_PER_SECOND)


def get_session():
    # requests.Session을 스레드 간 공유하지 않는다.
    if not hasattr(LOCAL, "session"):
        LOCAL.session = requests.Session()
        LOCAL.session.headers.update(HEADERS)
    return LOCAL.session


def retry_after(value):
    if not value:
        return 0.0
    try:
        seconds = float(value)
    except (ValueError, TypeError):
        try:
            date = parsedate_to_datetime(value)
            if date.tzinfo is None:
                date = date.replace(tzinfo=timezone.utc)
            seconds = (date - datetime.now(timezone.utc)).total_seconds()
        except (ValueError, TypeError, OverflowError):
            return 0.0
    return max(0.0, seconds) if math.isfinite(seconds) else 0.0


def request_data(method, url, decode, **kwargs):
    for attempt in range(MAX_ATTEMPTS):
        LIMITER.acquire()
        delay = min(30.0, 2.0 ** attempt) + random.uniform(0.0, 0.5)
        try:
            with get_session().request(
                method, url, timeout=(10, 30), **kwargs
            ) as response:
                if response.status_code in (429, 503):
                    delay = max(
                        delay, retry_after(response.headers.get("Retry-After"))
                    )
                    # 해당 스레드뿐 아니라 전체 요청을 쉬게 한다.
                    LIMITER.pause(delay)
                response.raise_for_status()
                return decode(response)
        except requests.HTTPError as exc:
            status = exc.response.status_code
            if status not in (408, 429, 500, 502, 503, 504):
                raise
            error = exc
        except (requests.RequestException, ValueError) as exc:
            error = exc
        if attempt == MAX_ATTEMPTS - 1:
            raise error
        if STOP.wait(delay):
            raise RuntimeError("Collection stopped") from error
    raise RuntimeError("Unreachable retry state")


def load_history():
    # 기존 파일이 있으면 손상/빈 파일이어도 시드로 덮어쓰지 않고 중단.
    if OUT.exists():
        history = json.loads(OUT.read_text(encoding="utf-8"))
    else:
        history = request_data("GET", SOURCE, lambda r: r.json())
    if not isinstance(history, dict) or not history:
        raise ValueError("History must be a non-empty SPID dictionary")
    for spid, player in history.items():
        if not isinstance(player, dict):
            raise ValueError(f"Invalid player: {spid}")
        times = player.get("times") or []
        values = player.get("values") or {}
        if not isinstance(times, list) or not isinstance(values, dict):
            raise ValueError(f"Invalid history arrays: {spid}")
        if any(
            not isinstance(arr, list) or len(arr) > len(times)
            for arr in values.values()
        ):
            raise ValueError(f"Unaligned history arrays: {spid}")
    return history


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


def fetch_price(spid, grade):
    def decode(response):
        pairs = extract_chart(response.text)
        if not pairs:
            # 빈 응답/HTML 변경을 성공으로 집계하지 않는다.
            raise ValueError("No chart data extracted")
        return pairs

    return request_data(
        "POST", ENDPOINT, decode,
        data={"spid": str(spid), "n1strong": str(grade)},
    )


def fetch_player(spid):
    # 한 선수의 강화 4종을 묶어 처리. 동시 HTTP 요청은 WORKERS 이하.
    results = []
    for grade in GRADES:
        if STOP.is_set():
            break
        try:
            results.append((grade, fetch_price(spid, grade), None))
        except Exception as exc:
            results.append((grade, None, repr(exc)))
    return results


def save_history(history):
    # 같은 디렉터리에서 flush/fsync 완료 후 원자적으로 교체.
    temp_name = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", dir=OUT.parent,
            prefix=OUT.name + ".", suffix=".tmp", delete=False
        ) as file:
            temp_name = file.name
            json.dump(
                history, file, ensure_ascii=False,
                separators=(",", ":"), allow_nan=False
            )
            file.flush()
            os.fsync(file.fileno())
        os.replace(temp_name, OUT)
    finally:
        if temp_name and os.path.exists(temp_name):
            os.unlink(temp_name)


def main():
    if not 1 <= WORKERS <= 15:
        raise ValueError("WORKERS must be between 1 and 15")
    STOP.clear()
    history = load_history()
    total = len(history)
    success = changed = errors = completed = no_data_streak = 0
    started = last_save = time.monotonic()
    print("PLAYERS =", total, flush=True)
    print(
        f"WORKERS={WORKERS} MAX_RPS={REQUESTS_PER_SECOND:g}",
        flush=True,
    )
    players = iter(history)
    pool = ThreadPoolExecutor(max_workers=WORKERS)
    pending = {}

    def submit_one():
        spid = next(players, None)
        if spid is not None:
            pending[pool.submit(fetch_player, spid)] = spid

    def progress():
        print(
            f"{completed}/{total} success={success} "
            f"changed={changed} errors={errors}",
            flush=True,
        )
        elapsed = time.monotonic() - started
        eta = elapsed / completed * (total - completed)
        print(
            f"ELAPSED={elapsed / 60:.1f}min ETA={eta / 60:.1f}min",
            flush=True,
        )

    try:
        # 전체 선수를 한꺼번에 큐에 넣지 않음: 최대 2 * WORKERS.
        for _ in range(min(total, WORKERS * 2)):
            submit_one()
        while pending:
            done, _ = wait(
                pending, timeout=1, return_when=FIRST_COMPLETED
            )
            for future in done:
                spid = pending.pop(future)
                try:
                    results = future.result()
                except Exception as exc:
                    results = [(g, None, repr(exc)) for g in GRADES]
                player_success = 0
                for grade, pairs, error in results:
                    if error is None:
                        try:
                            # 작업 스레드는 history를 수정하지 않음.
                            # 병합 실패 시 원래 선수 데이터를 그대로 유지.
                            candidate = copy.deepcopy(history[spid])
                            did_change = merge(candidate, grade, pairs)
                            history[spid] = candidate
                            success += 1
                            player_success += 1
                            changed += int(did_change)
                        except Exception as exc:
                            error = repr(exc)
                    if error is not None:
                        errors += 1
                        if errors <= 10:
                            print(
                                "ERROR", spid, grade, error, flush=True
                            )
                completed += 1
                no_data_streak = (
                    0 if player_success else no_data_streak + 1
                )
                if completed % 100 == 0 or completed == total:
                    progress()
                if no_data_streak >= 100:
                    raise RuntimeError(
                        "STOP: 100 consecutive completed players returned no data"
                    )
                submit_one()
            if (
                success
                and time.monotonic() - last_save >= CHECKPOINT_SECONDS
            ):
                save_history(history)
                last_save = time.monotonic()
                print(
                    f"CHECKPOINT players={completed}", flush=True
                )
    finally:
        STOP.set()
        for future in pending:
            future.cancel()
        pool.shutdown(wait=True, cancel_futures=True)
        # 일부 실패/중단에도 이미 병합한 데이터는 보존.
        if success:
            save_history(history)
    if success == 0:
        raise RuntimeError("No price data extracted")
    print(
        f"DONE players={completed} success={success} "
        f"changed={changed} errors={errors}",
        flush=True,
    )


if __name__ == "__main__":
    main()
