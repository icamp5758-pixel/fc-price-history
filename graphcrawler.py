"""FC Online price crawler - keep one daily value for the latest 90 days."""
import copy, json, math, os, random, re, tempfile, threading, time
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from datetime import date, datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
import requests

SOURCE=os.getenv('SEED_URL','https://raw.githubusercontent.com/JCH1231/fifa-simulation-data/main/price_history.json')
ENDPOINT='https://m.fconline.nexon.com/datacenter/PlayerPriceGraph'
OUT=Path('price_history.json')
GRADES=[8,9,10,11]
TEST_LIMIT=int(os.getenv('TEST_LIMIT','20'))
KEEP_DAYS=int(os.getenv('KEEP_DAYS','90'))
WORKERS=int(os.getenv('WORKERS','12'))
RPS=float(os.getenv('REQUESTS_PER_SECOND','16'))
MAX_ATTEMPTS=4
CHECKPOINT_SECONDS=300
HEADERS={'User-Agent':'Mozilla/5.0 (Linux; Android 10) AppleWebKit/537.36 Chrome/126 Safari/537.36','Accept':'*/*','Referer':'https://m.fconline.nexon.com/datacenter','X-Requested-With':'XMLHttpRequest'}
STOP=threading.Event(); LOCAL=threading.local()

class RateLimiter:
    def __init__(self,rate):
        if not math.isfinite(rate) or rate<=0: raise ValueError('bad rate')
        self.interval=1/rate; self.next_at=0.; self.paused_until=0.; self.lock=threading.Lock()
    def acquire(self):
        while not STOP.is_set():
            with self.lock:
                now=time.monotonic(); delay=max(self.next_at,self.paused_until)-now
                if delay<=0: self.next_at=now+self.interval; return
            STOP.wait(delay)
        raise RuntimeError('Collection stopped')
    def pause(self,s):
        with self.lock: self.paused_until=max(self.paused_until,time.monotonic()+s)
LIMITER=RateLimiter(RPS)

def session():
    if not hasattr(LOCAL,'s'):
        LOCAL.s=requests.Session(); LOCAL.s.headers.update(HEADERS)
    return LOCAL.s

def retry_after(v):
    if not v:return 0.
    try:s=float(v)
    except Exception:
        try:
            d=parsedate_to_datetime(v); d=d if d.tzinfo else d.replace(tzinfo=timezone.utc)
            s=(d-datetime.now(timezone.utc)).total_seconds()
        except Exception:return 0.
    return max(0.,s) if math.isfinite(s) else 0.

def request(method,url,decode,**kw):
    for attempt in range(MAX_ATTEMPTS):
        LIMITER.acquire(); delay=min(30.,2.**attempt)+random.uniform(0,.5)
        try:
            with session().request(method,url,timeout=(10,30),**kw) as r:
                if r.status_code in (429,503):
                    delay=max(delay,retry_after(r.headers.get('Retry-After'))); LIMITER.pause(delay)
                r.raise_for_status(); return decode(r)
        except requests.HTTPError as e:
            if e.response.status_code not in (408,429,500,502,503,504): raise
            err=e
        except (requests.RequestException,ValueError) as e: err=e
        if attempt==MAX_ATTEMPTS-1: raise err
        if STOP.wait(delay): raise RuntimeError('Collection stopped') from err

def load_history():
    if OUT.exists(): data=json.loads(OUT.read_text('utf-8'))
    else: data=request('GET',SOURCE,lambda r:r.json())
    if not isinstance(data,dict) or not data: raise ValueError('History must be non-empty')
    return data

def extract_array(block,name):
    m=re.search(rf'{name}\s*:\s*\[(.*?)\]',block,re.S)
    if not m:return []
    return [x.strip().strip("'\"") for x in m.group(1).split(',') if x.strip()]

def extract_chart(html):
    m=re.search(r'var\s+chartData\s*=\s*\{(.*?)\}\s*;',html,re.S)
    if not m:return []
    ts=extract_array(m.group(1),'time'); vs=extract_array(m.group(1),'value'); out=[]
    for t,v in zip(ts,vs):
        try:
            p=int(str(v).replace(',','').replace('"','').replace("'",'').strip())
            if p>0: out.append((str(t).strip(),p))
        except Exception: pass
    return out

def as_day(raw,today):
    s=str(raw).strip()
    # millisecond/second unix timestamps
    if s.isdigit():
        n=int(s); n=n/1000 if n>10_000_000_000 else n
        try:return datetime.fromtimestamp(n,timezone.utc).date()
        except Exception:return None
    # ISO date/time
    try:return datetime.fromisoformat(s.replace('Z','+00:00')).date()
    except Exception: pass
    # FC chart M.D: choose the most recent occurrence not after today
    m=re.fullmatch(r'(\d{1,2})\.(\d{1,2})',s)
    if m:
        mo,da=map(int,m.groups())
        for y in (today.year,today.year-1):
            try:
                d=date(y,mo,da)
                if d<=today:return d
            except ValueError: pass
    return None

def compact_player(player,today):
    cutoff=today-timedelta(days=KEEP_DAYS-1)
    times=list(player.get('times') or []); values=player.get('values') or {}
    maps={str(g):{} for g in values}
    for i,t in enumerate(times):
        d=as_day(t,today)
        if not d or d<cutoff or d>today: continue
        key=d.isoformat()
        for g,arr in values.items():
            if i<len(arr) and arr[i] not in (None,0,'0'):
                maps.setdefault(str(g),{})[key]=arr[i]
    days=sorted({d for m in maps.values() for d in m})
    player['times']=days
    player['values']={g:[m.get(d) for d in days] for g,m in maps.items()}

def merge(player,grade,pairs,today):
    compact_player(player,today)
    times=list(player.get('times') or []); values=player.get('values') or {}
    maps={}
    for g,arr in values.items():
        maps[str(g)]={t:arr[i] for i,t in enumerate(times) if i<len(arr) and arr[i] not in (None,0,'0')}
    g=str(grade); maps.setdefault(g,{})
    before=dict(maps[g]); cutoff=today-timedelta(days=KEEP_DAYS-1)
    for t,p in pairs:
        d=as_day(t,today)
        if d and cutoff<=d<=today: maps[g][d.isoformat()]=int(p)
    # remove old days from every grade, then rebuild one shared daily axis
    for gg in maps:
        maps[gg]={k:v for k,v in maps[gg].items() if (lambda d: cutoff<=d<=today)(date.fromisoformat(k))}
    days=sorted({d for m in maps.values() for d in m})
    player['times']=days; player['values']={gg:[m.get(d) for d in days] for gg,m in maps.items()}
    return before!=maps[g]

def fetch_price(spid,grade):
    def decode(r):
        p=extract_chart(r.text)
        if not p: raise ValueError('No chart data extracted')
        return p
    return request('POST',ENDPOINT,decode,data={'spid':str(spid),'n1strong':str(grade)})

def fetch_player(spid):
    out=[]
    for g in GRADES:
        try: out.append((g,fetch_price(spid,g),None))
        except Exception as e: out.append((g,None,repr(e)))
    return out

def save(data):
    with tempfile.NamedTemporaryFile('w',encoding='utf-8',dir='.',prefix='price_history.',suffix='.tmp',delete=False) as f:
        tmp=f.name; json.dump(data,f,ensure_ascii=False,separators=(',',':')); f.flush(); os.fsync(f.fileno())
    os.replace(tmp,OUT)

def main():
    if not 1<=WORKERS<=15: raise ValueError('WORKERS must be 1..15')
    today=datetime.now(timezone(timedelta(hours=9))).date()
    history=load_history()
    # Critical: compact before crawling, so checkpoints never grow without bound.
    for p in history.values(): compact_player(p,today)
    if TEST_LIMIT>0: history=dict(list(history.items())[:TEST_LIMIT])
    total=len(history); success=changed=errors=completed=streak=0; started=last_save=time.monotonic()
    print(f'PLAYERS={total} KEEP_DAYS={KEEP_DAYS} DATE={today}',flush=True)
    players=iter(history); pool=ThreadPoolExecutor(max_workers=WORKERS); pending={}
    def submit():
        spid=next(players,None)
        if spid is not None: pending[pool.submit(fetch_player,spid)]=spid
    for _ in range(min(total,WORKERS*2)): submit()
    try:
        while pending:
            done,_=wait(pending,timeout=1,return_when=FIRST_COMPLETED)
            for fut in done:
                spid=pending.pop(fut)
                try: results=fut.result()
                except Exception as e: results=[(g,None,repr(e)) for g in GRADES]
                ps=0
                        for g,pairs,err in results:
            if err is None:
                try:
                    cand=copy.deepcopy(history[spid])
                    did=merge(cand,g,pairs,today)
                    history[spid]=cand
                    success+=1
                    ps+=1
                    changed+=int(did)
                except Exception as e:
                    err=repr(e)
            if err is not None:
                errors+=1
                if errors<=10:
                    print('ERROR',spid,g,err,flush=True)
                completed+=1; streak=0 if ps else streak+1
                if completed%100==0 or completed==total:
                    elapsed=time.monotonic()-started; eta=elapsed/completed*(total-completed)
                    print(f'{completed}/{total} success={success} changed={changed} errors={errors} ETA={eta/60:.1f}min',flush=True)
                if streak>=100: raise RuntimeError('STOP: 100 consecutive players returned no data')
                submit()
            if success and TEST_LIMIT==0 and time.monotonic()-last_save>=CHECKPOINT_SECONDS:
                save(history); last_save=time.monotonic(); print('CHECKPOINT',completed,flush=True)
    finally:
        STOP.set()
        for f in pending:f.cancel()
        pool.shutdown(wait=True,cancel_futures=True)
        if success and TEST_LIMIT==0: save(history)
    if success==0: raise RuntimeError('No price data extracted')
    print(f'DONE players={completed} success={success} changed={changed} errors={errors} bytes={OUT.stat().st_size}',flush=True)
if __name__=='__main__': main()
