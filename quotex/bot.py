"""
Quotex Charts — bot.py (v4 — إصلاح شامل + تحسين الأداء)

الإصلاحات:
1. الشموع مطابقة 100% لـ Quotex — لا شموع وهمية، لا gap مصطنع
2. فقط البيانات الرسمية من Quotex تُبنى عليها الشموع
3. Tick يُحدّث الشمعة الحالية فقط — لا يُنشئ شمعة بدون server time
4. تحسينات شاملة في الأداء والاستقرار
5. SQLite محسّن بـ WAL mode + indexes
6. Cache ذكي في الذاكرة
7. إزالة Race Conditions و Memory Leaks
"""

import asyncio
import json
import time
import threading
import logging
import sqlite3
import os
import collections
from typing import Optional, Dict, List, Tuple

# ── Logging ──────────────────────────────────────────────────
logging.basicConfig(level=logging.WARNING, format='%(asctime)s [%(levelname)s] %(message)s')
log = logging.getLogger(__name__)
for _noisy in ('websockets', 'pyquotex', 'asyncio', 'urllib3', 'httpx'):
    logging.getLogger(_noisy).setLevel(logging.CRITICAL)

# ── import pyquotex ──────────────────────────────────────────
try:
    from pyquotex.stable_api import Quotex
    PYQUOTEX_OK = True
except ModuleNotFoundError:
    PYQUOTEX_OK = False

# ── credentials ─────────────────────────────────────────────
try:
    with open('credentials.json') as f:
        _creds = json.load(f)
    EMAIL    = _creds.get('email', '').strip()
    PASSWORD = _creds.get('password', '').strip()
except Exception:
    EMAIL = PASSWORD = ''

# ════════════════════════════════════════════════════════════
#  SQLITE — WAL mode للأداء الأقصى
# ════════════════════════════════════════════════════════════
DB_PATH  = 'candles_cache.db'
_db_lock = threading.Lock()

def _get_conn():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False, timeout=10)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA cache_size=10000")
    conn.execute("PRAGMA temp_store=MEMORY")
    conn.execute("PRAGMA mmap_size=268435456")  # 256MB
    return conn

def _init_db():
    with _db_lock:
        conn = _get_conn()
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS candles (
                asset  TEXT    NOT NULL,
                period INTEGER NOT NULL,
                ts     INTEGER NOT NULL,
                open   REAL    NOT NULL,
                high   REAL    NOT NULL,
                low    REAL    NOT NULL,
                close  REAL    NOT NULL,
                PRIMARY KEY (asset, period, ts)
            );
            CREATE INDEX IF NOT EXISTS idx_c_apts ON candles(asset, period, ts);
            CREATE INDEX IF NOT EXISTS idx_c_ap ON candles(asset, period);
        """)
        conn.commit()
        conn.close()

_init_db()

def _db_upsert(asset: str, period: int, candles: list):
    """حفظ شموع في قاعدة البيانات — رافض للشموع الوهمية.

    سياسة صارمة لحماية القابات (Gaps) الحقيقية:
    - الشمعة المغلقة (انتهت فترتها الزمنية بالكامل) لا تُعدَّل أبداً بعد إدراجها،
      حتى لو أعاد Quotex إرسال بيانات مختلفة لها لاحقاً (مثلاً عند إعادة الاتصال
      أو التبديل بين الأزواج). هذا يمنع اختفاء أي Gap حقيقي كان قد ظهر بشكل صحيح.
    - يُسمح فقط بتحديث آخر شمعة غير المغلقة (التي لا تزال فترتها جارية).
    - أي شمعة جديدة (لم تكن موجودة أصلاً) تُدرج بشكل طبيعي.
    """
    if not candles:
        return
    now = int(time.time())
    rows = []
    for c in candles:
        try:
            ts = int(float(c.get('time', 0)))
            op = float(c.get('open', 0))
            hi = float(c.get('high', 0))
            lo = float(c.get('low',  0))
            cl = float(c.get('close',0))
            # تحقق من صحة البيانات — رفض الشموع الفارغة أو الخاطئة
            if ts > 0 and cl > 0 and op > 0 and hi >= max(op, cl) and lo <= min(op, cl):
                rows.append((asset, period, ts, op, hi, lo, cl, period, now))
        except Exception:
            pass
    if not rows:
        return
    with _db_lock:
        conn = _get_conn()
        try:
            conn.executemany(
                """
                INSERT INTO candles (asset, period, ts, open, high, low, close)
                VALUES (?,?,?,?,?,?,?)
                ON CONFLICT(asset, period, ts) DO UPDATE SET
                    open  = excluded.open,
                    high  = excluded.high,
                    low   = excluded.low,
                    close = excluded.close
                WHERE (candles.ts + ?) > ?
                """,
                rows
            )
            conn.commit()
        finally:
            conn.close()

def _db_load(asset: str, period: int, limit: int = 0) -> list:
    with _db_lock:
        conn = _get_conn()
        try:
            if limit > 0:
                cur = conn.execute(
                    "SELECT ts,open,high,low,close FROM candles "
                    "WHERE asset=? AND period=? ORDER BY ts DESC LIMIT ?",
                    (asset, period, limit)
                )
                rows = list(reversed(cur.fetchall()))
            else:
                cur = conn.execute(
                    "SELECT ts,open,high,low,close FROM candles "
                    "WHERE asset=? AND period=? ORDER BY ts ASC",
                    (asset, period)
                )
                rows = cur.fetchall()
        finally:
            conn.close()
    return [{'time':r[0],'open':r[1],'high':r[2],'low':r[3],'close':r[4]} for r in rows]

def _db_newest(asset: str, period: int) -> Optional[int]:
    with _db_lock:
        conn = _get_conn()
        try:
            row = conn.execute(
                "SELECT MAX(ts) FROM candles WHERE asset=? AND period=?", (asset, period)
            ).fetchone()
            return row[0] if row and row[0] else None
        finally:
            conn.close()

def _db_oldest(asset: str, period: int) -> Optional[int]:
    with _db_lock:
        conn = _get_conn()
        try:
            row = conn.execute(
                "SELECT MIN(ts) FROM candles WHERE asset=? AND period=?", (asset, period)
            ).fetchone()
            return row[0] if row and row[0] else None
        finally:
            conn.close()

def _db_count(asset: str, period: int) -> int:
    with _db_lock:
        conn = _get_conn()
        try:
            row = conn.execute(
                "SELECT COUNT(*) FROM candles WHERE asset=? AND period=?", (asset, period)
            ).fetchone()
            return row[0] if row else 0
        finally:
            conn.close()

def _db_load_range(asset: str, period: int, from_ts: int, to_ts: int) -> list:
    with _db_lock:
        conn = _get_conn()
        try:
            cur = conn.execute(
                "SELECT ts,open,high,low,close FROM candles "
                "WHERE asset=? AND period=? AND ts>=? AND ts<=? ORDER BY ts ASC",
                (asset, period, from_ts, to_ts)
            )
            rows = cur.fetchall()
        finally:
            conn.close()
    return [{'time':r[0],'open':r[1],'high':r[2],'low':r[3],'close':r[4]} for r in rows]

# ════════════════════════════════════════════════════════════
#  CANDLE HELPERS — صارم في صحة البيانات
# ════════════════════════════════════════════════════════════

def _validate_candle(c: dict) -> Optional[dict]:
    """تحقق من صحة الشمعة — رفض أي بيانات غير سليمة."""
    try:
        ts = int(float(c.get('time', 0)))
        op = float(c.get('open',  c.get('close', 0)))
        hi = float(c.get('high',  c.get('close', 0)))
        lo = float(c.get('low',   c.get('close', 0)))
        cl = float(c.get('close', 0))
        if ts <= 0 or cl <= 0 or op <= 0:
            return None
        # ضمان صحة High/Low
        hi = max(hi, op, cl)
        lo = min(lo, op, cl)
        return {'time': ts, 'open': op, 'high': hi, 'low': lo, 'close': cl}
    except Exception:
        return None

def _clean(raw: list) -> list:
    """تنظيف وترتيب قائمة الشموع — فقط الصالحة."""
    seen = {}
    for c in (raw or []):
        v = _validate_candle(c)
        if v:
            ts = v['time']
            if ts not in seen:
                seen[ts] = v
            else:
                # دمج: حافظ على open الأقدم، high الأعلى، low الأدنى، close الأحدث
                ex = seen[ts]
                ex['high']  = max(ex['high'],  v['high'])
                ex['low']   = min(ex['low'],   v['low'])
                ex['close'] = v['close']
    return sorted(seen.values(), key=lambda x: x['time'])

def _merge(base: list, overlay: list) -> list:
    """دمج قائمتين من الشموع — overlay تكسب في close."""
    m: Dict[int, dict] = {c['time']: dict(c) for c in base}
    for c in overlay:
        ts = c['time']
        if ts in m:
            m[ts]['high']  = max(m[ts]['high'],  c['high'])
            m[ts]['low']   = min(m[ts]['low'],   c['low'])
            m[ts]['close'] = c['close']
        else:
            m[ts] = dict(c)
    return sorted(m.values(), key=lambda x: x['time'])

# ════════════════════════════════════════════════════════════
#  IN-MEMORY CACHE — لسرعة الاستجابة
# ════════════════════════════════════════════════════════════

class CandleCache:
    """Cache ذكي للشموع في الذاكرة."""
    def __init__(self):
        self._lock = threading.Lock()
        # asset → period → sorted list of candles
        self._data: Dict[str, Dict[int, list]] = {}
        # asset → period → {ts: candle} للبحث السريع
        self._idx:  Dict[str, Dict[int, Dict[int, dict]]] = {}

    def get(self, asset: str, period: int) -> list:
        with self._lock:
            return list(self._data.get(asset, {}).get(period, []))

    def update(self, asset: str, period: int, candles: list, now: Optional[int] = None):
        """
        تحديث الكاش — الشمعة المغلقة (انتهت فترتها) لا تُعدَّل أبداً بمجرد
        إدراجها. التحديث اللاحق مسموح فقط لآخر شمعة لا تزال فترتها جارية.
        """
        if not candles:
            return
        if now is None:
            now = int(time.time())
        with self._lock:
            ap_idx = self._idx.setdefault(asset, {}).setdefault(period, {})
            ap_lst = self._data.setdefault(asset, {}).setdefault(period, [])
            changed = False
            for c in candles:
                ts = c['time']
                if ts not in ap_idx:
                    entry = dict(c)
                    ap_idx[ts] = entry
                    ap_lst.append(entry)
                    changed = True
                else:
                    if (ts + period) <= now:
                        # شمعة مغلقة بالفعل — ثابتة، لا تُعدَّل
                        continue
                    ex = ap_idx[ts]
                    nh = max(ex['high'], c['high'])
                    nl = min(ex['low'],  c['low'])
                    nc = c['close']
                    if nh != ex['high'] or nl != ex['low'] or nc != ex['close']:
                        ex['high']  = nh
                        ex['low']   = nl
                        ex['close'] = nc
                        changed = True
            if changed:
                ap_lst.sort(key=lambda x: x['time'])

    def set_all(self, asset: str, period: int, candles: list):
        with self._lock:
            idx = {c['time']: c for c in candles}
            self._idx.setdefault(asset, {})[period]  = idx
            self._data.setdefault(asset, {})[period] = sorted(candles, key=lambda x: x['time'])

    def count(self, asset: str, period: int) -> int:
        with self._lock:
            return len(self._data.get(asset, {}).get(period, []))

    def clear_asset(self, asset: str):
        with self._lock:
            self._data.pop(asset, None)
            self._idx.pop(asset, None)

    def has_asset(self, asset: str) -> bool:
        with self._lock:
            return asset in self._data and bool(self._data[asset])

_cache = CandleCache()

# ════════════════════════════════════════════════════════════
#  LIVE PRICE — تتبع السعر الحي
# ════════════════════════════════════════════════════════════

_live_lock = threading.Lock()
_live: Dict[str, dict] = {}   # asset → {price, time}

def _push_live(asset: str, price: float, ts: float):
    with _live_lock:
        _live[asset] = {'price': price, 'time': ts}

# ════════════════════════════════════════════════════════════
#  SSE SUBSCRIBERS — بث فوري بدل الـ Polling (يحل مشكلة التأخير)
# ════════════════════════════════════════════════════════════
_sse_lock: threading.Lock = threading.Lock()
_sse_subs: Dict[str, List['collections.deque']] = {}   # asset → [queue, ...]

def sse_subscribe(asset: str):
    """يسجّل مستمعًا جديدًا (queue) لتلقي تيكات هذا الزوج فور وصولها."""
    q = collections.deque(maxlen=50)
    with _sse_lock:
        _sse_subs.setdefault(asset, []).append(q)
    return q

def sse_unsubscribe(asset: str, q):
    with _sse_lock:
        lst = _sse_subs.get(asset)
        if lst and q in lst:
            lst.remove(q)
            if not lst:
                _sse_subs.pop(asset, None)

def _sse_broadcast(asset: str, payload: dict):
    with _sse_lock:
        subs = list(_sse_subs.get(asset, []))
    for q in subs:
        try:
            q.append(payload)
        except Exception:
            pass

# ════════════════════════════════════════════════════════════
#  CURRENT CANDLE — الشمعة الجارية (من Ticks)
# ════════════════════════════════════════════════════════════

_cur_lock = threading.Lock()
_cur_candles: Dict[str, Dict[int, dict]] = {}   # asset → period → candle

def _tick_to_candle(asset: str, price: float, ts: float):
    """تحديث الشمعة الحالية من tick — الشمعة تُنشأ فقط بـ server time.

    القاعدة الذهبية (مطابقة Quotex الحقيقية):
        Open الشمعة الجديدة = أول Tick حقيقي يصل فعليًا بعد بداية الفترة —
        وليس Close الشمعة السابقة أبداً.
    هذا يحافظ على أي Gap حقيقي حصل في السوق (بدل إخفائه)، وفي نفس الوقت
    يمنع ظهور أي Gap وهمي غير موجود أصلاً في Quotex، لأن السعر يُؤخذ حصريًا
    من أول تيك فعلي وليس من قيمة مصطنعة.
    """
    with _cur_lock:
        for period in [5, 10, 15, 30, 60, 120, 180, 300, 600, 900, 1800, 3600, 14400, 86400]:
            candle_ts = int(ts) // period * period
            ap = _cur_candles.setdefault(asset, {})
            existing = ap.get(period)

            if existing is None or existing['time'] != candle_ts:
                # شمعة جديدة تبدأ — أغلق القديمة في الكاش أولاً كما هي نهائيًا (لا تعديل لاحق)
                if existing is not None and existing['time'] < candle_ts:
                    _cache.update(asset, period, [dict(existing)])

                # Open = أول Tick حقيقي وصل فعلاً لهذه الفترة (لا نستخدم Close السابق إطلاقًا)
                ap[period] = {
                    'time':  candle_ts,
                    'open':  price,
                    'high':  price,
                    'low':   price,
                    'close': price,
                }
            else:
                c = existing
                if price > c['high']:  c['high']  = price
                if price < c['low']:   c['low']   = price
                c['close'] = price

def _get_cur_candle(asset: str, period: int) -> Optional[dict]:
    with _cur_lock:
        c = _cur_candles.get(asset, {}).get(period)
        return dict(c) if c else None

# ════════════════════════════════════════════════════════════
#  SHARED STATE
# ════════════════════════════════════════════════════════════
_state_lock  = threading.Lock()
_connected   = False
_client      = None
_conn_status = {'state': 'disconnected', 'message': '', 'attempt': 0}
_conn_lock   = threading.Lock()

# الأزواج المُراقَبة حاليًا (تُبَث كلها بالتوازي — تبديل الزوج = إضافة فقط)
_watched: set = set()

# آخر خطأ مسجَّل لكل زوج (لتشخيص أزواج مثل USD/BRL OTC التي قد لا تكون مدعومة)
_asset_errors_lock = threading.Lock()
_asset_errors: Dict[str, dict] = {}   # asset → {'error': str, 'time': float}

def _set_asset_error(asset: str, msg: str):
    with _asset_errors_lock:
        _asset_errors[asset] = {'error': msg, 'time': time.time()}

def _clear_asset_error(asset: str):
    with _asset_errors_lock:
        _asset_errors.pop(asset, None)

def get_asset_error(asset: str) -> Optional[dict]:
    with _asset_errors_lock:
        v = _asset_errors.get(asset)
        return dict(v) if v else None

def _set_status(state: str, msg: str = '', attempt: int = 0):
    with _conn_lock:
        _conn_status.update({'state': state, 'message': msg, 'attempt': attempt})

def _extract_price(item) -> Optional[Tuple[float, float]]:
    """استخراج السعر والوقت من أي صيغة."""
    try:
        if isinstance(item, dict):
            price = float(item.get('price') or item.get('close') or 0)
            ts    = float(item.get('time')  or item.get('timestamp') or time.time())
        elif isinstance(item, (list, tuple)) and len(item) >= 2:
            ts, price = float(item[0]), float(item[1])
        elif isinstance(item, (int, float)):
            price, ts = float(item), time.time()
        else:
            return None
        return (price, ts) if price > 0 else None
    except Exception:
        return None

# ════════════════════════════════════════════════════════════
#  DB CACHE LOADER
# ════════════════════════════════════════════════════════════

# جميع الفريمات المدعومة (مطابقة Quotex)
ALL_PERIODS = [5, 10, 15, 30, 60, 120, 180, 300, 600, 900, 1800, 3600, 14400, 86400]

# جميع الأزواج المعروضة في الواجهة (مطابقة OTC_PAIRS في static/index.html) —
# تُستخدم لبدء بث كل الأزواج فور تشغيل البوت بدل انتظار ضغط المستخدم على كل زوج.
DEFAULT_ASSETS = [
    # Forex
    'NZDUSD_otc', 'NZDJPY_otc', 'AUDNZD_otc', 'NZDCAD_otc', 'USDMXN_otc',
    'USDPHP_otc', 'NZDCHF_otc', 'USDINR_otc', 'USDBRL_otc', 'CADCHF_otc',
    'GBPNZD_otc', 'USDCOP_otc', 'USDIDR_otc', 'USDZAR_otc', 'USDDZD_otc',
    'USDBDT_otc', 'USDNGN_otc', 'USDPKR_otc', 'USDARS_otc', 'USDEGP_otc',
    'EURNZD_otc',
    # Crypto
    'BTCUSD_otc', 'ETHUSD_otc', 'LTCUSD_otc', 'XRPUSD_otc', 'BCHUSD_otc',
    'BNBUSD_otc', 'SOLUSD_otc', 'DOTUSD_otc', 'AVAXUSD_otc', 'LINKUSD_otc',
    'AXSUSD_otc', 'ATOMUSD_otc', 'TONUSD_otc', 'ETCUSD_otc', 'ZECUSD_otc',
    'DASHUSD_otc', 'TRUMPUSD_otc',
    # Commodity
    'USOIL_otc', 'BRENTOIL_otc',
]

async def _load_db_cache(asset: str):
    """تحميل الكاش من قاعدة البيانات إلى الذاكرة."""
    for period in ALL_PERIODS:
        # حمّل آخر 2000 شمعة لكل فريم
        candles = _db_load(asset, period, limit=2000)
        if candles:
            _cache.set_all(asset, period, candles)

# ════════════════════════════════════════════════════════════
#  HISTORY FETCH — جلب التاريخ بشكل صحيح
# ════════════════════════════════════════════════════════════

async def _fetch_tf(client, asset: str, period: int, window_sec: int):
    """جلب إطار زمني — يسد الفجوة منذ آخر معروف حتى الآن."""
    now = int(time.time())
    newest = _db_newest(asset, period)

    # الفجوة: اجلب ما فات منذ آخر شمعة
    if newest and (now - newest) > period * 2:
        gap_sec = now - newest + period * 5
        try:
            candles = await asyncio.wait_for(
                client.get_candles(asset, float(now), gap_sec, period),
                timeout=20
            )
            if candles:
                clean = _clean(candles)
                new_only = [c for c in clean if c['time'] > newest]
                if new_only:
                    _db_upsert(asset, period, new_only)
                    _cache.update(asset, period, new_only)
        except Exception:
            pass

    # الجلب الرئيسي
    newest = _db_newest(asset, period) or 0
    if (now - newest) < window_sec * 0.3:
        return  # الكاش طازج

    try:
        candles = await asyncio.wait_for(
            client.get_candles(asset, float(now), window_sec, period),
            timeout=25
        )
        if candles:
            clean = _clean(candles)
            if clean:
                _db_upsert(asset, period, clean)
                _cache.update(asset, period, clean)
    except Exception:
        pass

async def _seed_history_fast(client, asset: str):
    """جلب متوازي لأهم الفريمات أولاً ثم بقية الفريمات في الخلفية."""
    # المرحلة 1: الفريمات الأساسية في آنٍ واحد
    primary = [
        _fetch_tf(client, asset, 5,     1800),   # S5   - 30 دقيقة
        _fetch_tf(client, asset, 10,    3600),   # S10  - 1 ساعة
        _fetch_tf(client, asset, 15,    7200),   # S15  - 2 ساعة
        _fetch_tf(client, asset, 30,    14400),  # S30  - 4 ساعات
        _fetch_tf(client, asset, 60,    21600),  # M1   - 6 ساعات
        _fetch_tf(client, asset, 300,   86400),  # M5   - 24 ساعة
        _fetch_tf(client, asset, 900,   259200), # M15  - 3 أيام
        _fetch_tf(client, asset, 3600,  604800), # H1   - 7 أيام
    ]
    await asyncio.gather(*primary, return_exceptions=True)

    # المرحلة 2: الفريمات الأكبر في الخلفية
    async def _bg_fetch():
        secondary = [
            _fetch_tf(client, asset, 120,   43200),   # M2   - 12 ساعة
            _fetch_tf(client, asset, 180,   86400),   # M3   - 24 ساعة
            _fetch_tf(client, asset, 600,   172800),  # M10  - 2 يوم
            _fetch_tf(client, asset, 1800,  432000),  # M30  - 5 أيام
            _fetch_tf(client, asset, 14400, 2592000), # H4   - 30 يوم
            _fetch_tf(client, asset, 86400, 7776000), # D1   - 90 يوم
        ]
        await asyncio.gather(*secondary, return_exceptions=True)
    asyncio.ensure_future(_bg_fetch())

# ════════════════════════════════════════════════════════════
#  STREAM LOOP — بث السعر الحي
# ════════════════════════════════════════════════════════════

# تجهيز عدد كبير من الأزواج دفعة واحدة (تحميل تاريخ + اشتراك بث لكل زوج)
# عبر اتصال واحد فقط قد يُثقل ذلك الاتصال، فتفشل أو "تتجمّد" بعض الاشتراكات
# بصمت دون أي خطأ ظاهر (وهذا ما يسبب أزواجًا لا تتحرك أبدًا بعد فتحها).
# لذلك نُجهّز الأزواج على دفعات محدودة بالتوازي بدل تجهيزها كلها في نفس اللحظة،
# بينما يبقى البث اللحظي لكل زوج تم تجهيزه بالفعل يعمل بالتوازي الكامل بدون أي قيد.
_ONBOARD_CONCURRENCY = 5
_onboard_sem: Optional[asyncio.Semaphore] = None

async def _subscribe_stream(client, asset: str) -> bool:
    """اشتراك بث زوج واحد. يُستخدم عند التجهيز الأول، وأيضًا لإعادة المحاولة
    ذاتيًا إن "تجمّد" الزوج (توقف عن استقبال أي تيك) بعد نجاح الاشتراك ظاهريًا."""
    try:
        await asyncio.wait_for(client.start_candles_stream(asset, 0), timeout=10)
        return True
    except Exception as e:
        # فشل الاشتراك بالزوج — غالبًا لأن الرمز غير مدعوم أو غير صحيح
        _set_asset_error(
            asset,
            f'فشل الاشتراك بالزوج "{asset}": {type(e).__name__}: {e}. '
            f'الأسباب المحتملة: الرمز غير موجود في حساب Quotex، أو السوق مغلق '
            f'لهذا الزوج حاليًا، أو اسم الـ Symbol المُرسَل لا يطابق ما يتوقعه الخادم.'
        )
        return False

async def _stream_loop(client, asset: str):
    """
    بث السعر الحي لزوج واحد. تنتهي هذه المهمة عند توقف المراقبة لهذا الزوج
    فقط (وليس عند انقطاع الاتصال العام، الذي تتولاه الحلقة الخارجية).
    """
    no_tick_count = 0
    NO_TICK_LIMIT = 100   # ~10 ثوانٍ بدون أي تيك (عند sleep=0.1) → نشتبه بمشكلة في الزوج

    while _connected and asset in _watched:
        try:
            prices = await asyncio.wait_for(
                client.get_realtime_price(asset), timeout=5
            )
            if prices:
                pair = _extract_price(prices[-1])
                if pair:
                    price, ts = pair
                    _clear_asset_error(asset)
                    no_tick_count = 0
                    _push_live(asset, price, ts)
                    # تحديث الشمعة الحالية من server time
                    if ts > 1e9:
                        _tick_to_candle(asset, price, ts)
                    # بث فوري لكل المستمعين على هذا الزوج (SSE)
                    _sse_broadcast(asset, {'price': price, 'time': ts})
                else:
                    no_tick_count += 1
            else:
                no_tick_count += 1
        except asyncio.TimeoutError:
            no_tick_count += 2
        except asyncio.CancelledError:
            break
        except Exception as e:
            _set_asset_error(asset, f'انقطع البث لـ "{asset}": {type(e).__name__}: {e}')
            break

        if no_tick_count >= NO_TICK_LIMIT:
            _set_asset_error(
                asset,
                f'لا يصل أي تيك للزوج "{asset}" منذ ~{NO_TICK_LIMIT/10:.0f} ثوانٍ رغم نجاح '
                f'الاشتراك — تُجرى محاولة إعادة اشتراك تلقائية. الأسباب المحتملة: السوق '
                f'مغلق لهذا الزوج (شائع في بعض أزواج OTC خارج ساعات معينة)، أو أن الاشتراك '
                f'الأول لم يُسجَّل فعليًا لدى الخادم بسبب زحام في تجهيز عدة أزواج دفعة واحدة.'
            )
            # إعادة محاولة اشتراك ذاتية (self-heal) بدل بقاء الزوج مجمَّدًا للأبد —
            # هذا هو الإصلاح المباشر لمشكلة الأزواج التي "تفتح لكنها لا تتحرك أبدًا".
            try:
                async with _onboard_sem:
                    await _subscribe_stream(client, asset)
            except Exception:
                pass
            no_tick_count = 0  # لا نكرر التنبيه كل ثانية، فقط كل دورة

        await asyncio.sleep(0.1)   # 10 FPS

    try:
        await client.stop_candles_stream(asset)
    except Exception:
        pass
    _watched.discard(asset)

# ════════════════════════════════════════════════════════════
#  AUTO-RECONNECT
# ════════════════════════════════════════════════════════════

async def _connect_and_stream(initial_asset: str):
    """
    معمارية متعددة الأزواج (تطابق Quotex):
    اتصال واحد فقط بالخادم، وكل الأزواج المُشاهَدة (watched) تُبَث بالتوازي.
    تبديل الزوج لا يُعيد الاتصال أبدًا — فقط يضيف الزوج لقائمة المراقبة
    إن لم يكن مراقَبًا أصلاً، والبيانات (تاريخ + بث حي) تكون جاهزة فورًا.
    """
    global _connected, _client

    # تحميل الكاش فوراً قبل الاتصال لكل الأزواج المراقَبة (وليس فقط الزوج الأول)
    # حتى يظهر أي زوج محدَّثاً مسبقاً بمجرد الضغط عليه في الواجهة.
    _watched.add(initial_asset)
    await asyncio.gather(
        *(_load_db_cache(a) for a in list(_watched)), return_exceptions=True
    )

    if not PYQUOTEX_OK:
        _set_status('error', 'pyquotex غير موجود — راجع requirements.txt')
        return
    if not EMAIL or not PASSWORD:
        _set_status('error', 'credentials.json غير موجود أو فارغ — أضف بيانات الدخول')
        return

    backoff = 5.0
    attempt = 0

    while _connected:
        attempt += 1
        _set_status('connecting', f'محاولة #{attempt}', attempt)

        try:
            client = Quotex(EMAIL, PASSWORD)
            _client = client

            ok, reason = await asyncio.wait_for(client.connect(), timeout=35)
            if not ok:
                raise ConnectionError(reason or 'login rejected')

            backoff = 5.0
            _set_status('connected', f'متصل', attempt)

            # مؤقّت التزامن: يمنع تجهيز عشرات الأزواج في نفس اللحظة على اتصال واحد
            global _onboard_sem
            _onboard_sem = asyncio.Semaphore(_ONBOARD_CONCURRENCY)

            # شغّل مهمة بث مستقلة لكل زوج مُراقَب حاليًا، بالتوازي
            running_tasks: Dict[str, asyncio.Task] = {}

            async def _onboard_and_run(asset: str):
                async with _onboard_sem:
                    await _seed_history_fast(client, asset)
                    ok = await _subscribe_stream(client, asset)
                if not ok:
                    _watched.discard(asset)
                    return
                await _stream_loop(client, asset)

            async def _ensure_task(asset: str):
                if asset in running_tasks and not running_tasks[asset].done():
                    return
                running_tasks[asset] = asyncio.ensure_future(_onboard_and_run(asset))

            # تشغيل جميع الأزواج المراقَبة بالتوازي (وليس واحداً تلو الآخر) —
            # بذلك تبدأ كل الأزواج استقبال بياناتها فور تشغيل البوت بأقصى سرعة.
            await asyncio.gather(
                *(_ensure_task(a) for a in list(_watched)), return_exceptions=True
            )

            # حلقة الإشراف: تراقب طلبات مشاهدة أزواج جديدة + تنظف المهام الميتة
            while _connected:
                for a in list(_watched):
                    if a not in running_tasks or running_tasks[a].done():
                        await _ensure_task(a)
                # تحقق هل أي مهمة بث ماتت بسبب خطأ في الاتصال نفسه (وليس فقط الزوج)
                if all(t.done() for t in running_tasks.values()) and running_tasks:
                    break
                await asyncio.sleep(0.5)

            for t in running_tasks.values():
                t.cancel()
            await asyncio.gather(*running_tasks.values(), return_exceptions=True)

        except asyncio.TimeoutError:
            _set_status('reconnecting', 'انتهت المهلة', attempt)
        except ConnectionError:
            _set_status('reconnecting', 'رُفض تسجيل الدخول')
            backoff = min(backoff * 2, 60.0)
        except asyncio.CancelledError:
            break
        except Exception as e:
            _set_status('reconnecting', f'خطأ: {type(e).__name__}')
        finally:
            try:
                if _client:
                    await _client.close()
            except Exception:
                pass
            _client = None

        if not _connected:
            break
        await asyncio.sleep(backoff)
        backoff = min(backoff * 1.5, 60.0)

    _set_status('disconnected', 'متوقف')

# ════════════════════════════════════════════════════════════
#  THREAD MANAGEMENT
# ════════════════════════════════════════════════════════════

_loop:   asyncio.AbstractEventLoop = None
_thread: threading.Thread          = None

async def _cancel_all_tasks():
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    for t in tasks:
        t.cancel()
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)

def start_service(asset: str = 'EURUSD_otc'):
    global _loop, _thread, _connected
    if _connected and _thread and _thread.is_alive():
        # الخدمة تعمل بالفعل — فقط أضف الزوج لقائمة المراقبة (بدون أي إعادة اتصال)
        watch_asset(asset)
        return
    stop_service()
    _connected = True
    _watched.clear()
    # الزوج المطلوب أولاً بالأولوية، ثم كل بقية الأزواج تُضاف فوراً أيضاً
    # حتى تبدأ جميعها استقبال البيانات في الخلفية منذ لحظة تشغيل البوت.
    _watched.add(asset)
    for a in DEFAULT_ASSETS:
        _watched.add(a)
    _loop = asyncio.new_event_loop()

    def _run():
        asyncio.set_event_loop(_loop)
        try:
            _loop.run_until_complete(_connect_and_stream(asset))
        except Exception:
            pass
        finally:
            try:
                pending = asyncio.all_tasks(_loop)
                if pending:
                    _loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
            except Exception:
                pass
            try:
                _loop.close()
            except Exception:
                pass

    _thread = threading.Thread(target=_run, daemon=True, name='quotex-stream')
    _thread.start()

def watch_asset(asset: str):
    """
    يضيف زوجًا جديدًا لقائمة المراقبة فيُبَث فورًا بالتوازي مع البقية —
    هذا هو سرّ التبديل الفوري بين الأزواج (بدون قطع/إعادة اتصال).
    إن كان الزوج مراقَبًا أصلاً، لا يفعل شيئًا (idempotent).
    """
    _clear_asset_error(asset)
    _watched.add(asset)
    if not _cache.has_asset(asset):
        # حمّل الكاش المحلي لهذا الزوج فورًا (لا ننتظر الشبكة لعرض شيء)
        if _loop and not _loop.is_closed() and _loop.is_running():
            asyncio.run_coroutine_threadsafe(_load_db_cache(asset), _loop)

def unwatch_asset(asset: str):
    """يوقف بث زوج معيّن (اختياري — يقلل الحمل إن كان لديك أزواج كثيرة)."""
    _watched.discard(asset)

def stop_service():
    global _connected, _loop, _thread
    _connected = False
    _watched.clear()

    if _loop and not _loop.is_closed():
        try:
            if _loop.is_running():
                future = asyncio.run_coroutine_threadsafe(_cancel_all_tasks(), _loop)
                try:
                    future.result(timeout=3)
                except Exception:
                    pass
                _loop.call_soon_threadsafe(_loop.stop)
        except Exception:
            pass

    if _thread and _thread.is_alive():
        _thread.join(timeout=6)

    _loop   = None
    _thread = None

# ════════════════════════════════════════════════════════════
#  PUBLIC API
# ════════════════════════════════════════════════════════════

def get_live_price(asset: str) -> Optional[dict]:
    with _live_lock:
        v = _live.get(asset)
        return dict(v) if v else None

def get_candles(asset: str, tf_min: float) -> list:
    """
    إرجاع الشموع المكتملة من الكاش + الشمعة الجارية.
    الشموع فقط من البيانات الرسمية — لا شموع وهمية.
    """
    period = _tf_to_period(tf_min)
    hist   = _cache.get(asset, period)

    # الشمعة الجارية من tick
    cur = _get_cur_candle(asset, period)
    if cur:
        # لا تُضف الشمعة الجارية إلى الكاش — فقط للعرض الحي
        if hist and hist[-1]['time'] == cur['time']:
            # حدّث آخر شمعة في الكاش
            result = hist[:-1] + [cur]
        else:
            result = hist + [cur]
    else:
        result = hist

    return result

def get_history(asset: str, tf_min: float) -> list:
    """الشموع المكتملة فقط من الكاش."""
    period = _tf_to_period(tf_min)
    return _cache.get(asset, period)

def get_history_status(asset: str) -> dict:
    """
    نفس المخرجات السابقة تماماً، لكن باستعلام واحد على قاعدة البيانات
    بدل 3 استعلامات منفصلة × 14 فريم (42 استعلام) في كل استدعاء.
    """
    tf_map = [
        (5/60, 'S5'), (10/60, 'S10'), (15/60, 'S15'), (30/60, 'S30'),
        (1, 'M1'), (2, 'M2'), (3, 'M3'), (5, 'M5'), (10, 'M10'),
        (15, 'M15'), (30, 'M30'), (60, 'H1'), (240, 'H4'), (1440, 'D1'),
    ]
    with _db_lock:
        conn = _get_conn()
        try:
            cur = conn.execute(
                "SELECT period, COUNT(*), MIN(ts), MAX(ts) FROM candles "
                "WHERE asset=? GROUP BY period",
                (asset,)
            )
            by_period = {r[0]: r for r in cur.fetchall()}
        finally:
            conn.close()

    result = {}
    for tf_min, label in tf_map:
        period = _tf_to_period(tf_min)
        r = by_period.get(period)
        result[label] = {
            'count':  r[1] if r else 0,
            'oldest': r[2] if r else None,
            'newest': r[3] if r else None,
            'deep_fetch_done': True,
        }
    return result

def is_connected() -> bool:
    return _connected and _conn_status.get('state') == 'connected'

def get_conn_status() -> dict:
    with _conn_lock:
        return dict(_conn_status)

def _tf_to_period(tf_min: float) -> int:
    """تحويل tf بالدقائق إلى ثوانٍ — مطابق لـ Quotex."""
    return max(5, int(round(tf_min * 60)))

# ════════════════════════════════════════════════════════════
#  BULK HISTORY FETCH
# ════════════════════════════════════════════════════════════

_bulk_status: Dict[str, dict] = {}
_bulk_lock = threading.Lock()

def get_bulk_status(asset: str, period: int) -> dict:
    key = f"{asset}_{period}"
    with _bulk_lock:
        return dict(_bulk_status.get(key, {'running': False, 'fetched': 0, 'gaps_filled': 0, 'msg': 'لم يبدأ'}))

def _set_bulk(asset: str, period: int, **kw):
    key = f"{asset}_{period}"
    with _bulk_lock:
        if key not in _bulk_status:
            _bulk_status[key] = {'running': False, 'fetched': 0, 'gaps_filled': 0, 'msg': ''}
        _bulk_status[key].update(kw)

async def _do_bulk_fetch(asset: str, period: int):
    """الجلب الضخم — فقط من Quotex مباشرة، لا شموع وهمية."""
    _set_bulk(asset, period, running=True, fetched=0, gaps_filled=0, msg='جارٍ الجلب الضخم…')
    total = 0

    try:
        if not PYQUOTEX_OK:
            _set_bulk(asset, period, running=False, msg='pyquotex غير موجود')
            return

        client = _client
        need_close = False
        if client is None:
            if not EMAIL or not PASSWORD:
                _set_bulk(asset, period, running=False, msg='credentials.json فارغ')
                return
            client = Quotex(EMAIL, PASSWORD)
            need_close = True
            ok, reason = await asyncio.wait_for(client.connect(), timeout=35)
            if not ok:
                _set_bulk(asset, period, running=False, msg=f'فشل الاتصال: {reason}')
                return

        now = int(time.time())
        batch_size  = min(1000, 86400 // period * 7) if period >= 60 else 2000
        max_batches = 15
        oldest = _db_oldest(asset, period) or now

        for i in range(max_batches):
            _set_bulk(asset, period, msg=f'دفعة {i+1}/{max_batches} — {total:,} شمعة…')
            end_ts   = oldest - i * (batch_size * period)
            fetch_sec = batch_size * period

            if end_ts < now - 2 * 365 * 86400:
                break

            try:
                candles = await asyncio.wait_for(
                    client.get_candles(asset, float(end_ts), fetch_sec, period),
                    timeout=30
                )
                if not candles:
                    break
                clean = _clean(candles)
                if clean:
                    _db_upsert(asset, period, clean)
                    _cache.update(asset, period, clean)
                    total += len(clean)
                    _set_bulk(asset, period, fetched=total)
                else:
                    break
            except asyncio.TimeoutError:
                _set_bulk(asset, period, msg=f'انتهت مهلة الدفعة {i+1}، متابعة…')
                continue
            except Exception as e:
                _set_bulk(asset, period, msg=f'خطأ: {type(e).__name__}')
                break

            await asyncio.sleep(0.2)

        # تحديث الكاش بكامل البيانات
        all_c = _db_load(asset, period, limit=5000)
        if all_c:
            _cache.set_all(asset, period, all_c)

        if need_close:
            try:
                await client.close()
            except Exception:
                pass

        _set_bulk(asset, period, running=False, fetched=total,
                  msg=f'✓ اكتمل — {total:,} شمعة من Quotex')

    except Exception as e:
        _set_bulk(asset, period, running=False, msg=f'خطأ: {type(e).__name__}: {e}')

def trigger_bulk_fetch(asset: str, period: int) -> dict:
    key = f"{asset}_{period}"
    with _bulk_lock:
        if _bulk_status.get(key, {}).get('running'):
            return {'status': 'already_running', 'msg': 'الجلب يعمل بالفعل'}

    if _loop and not _loop.is_closed() and _loop.is_running():
        asyncio.run_coroutine_threadsafe(_do_bulk_fetch(asset, period), _loop)
        return {'status': 'started', 'msg': 'بدأ الجلب'}
    return {'status': 'error', 'msg': 'غير متصل — اضغط اتصال أولاً'}