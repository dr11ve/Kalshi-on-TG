import os
import time
import asyncio
import logging
from typing import Dict, Any, List, Tuple, Optional
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
import html as _html

import aiosqlite
import httpx
from dotenv import load_dotenv

from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.filters import Command
from aiogram.exceptions import TelegramNetworkError
from aiogram.types import (
    Message,
    ReplyKeyboardMarkup,
    KeyboardButton,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
log = logging.getLogger("valshi")

load_dotenv()
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "")
PRIMARY  = os.getenv("KALSHI_BASE_URL", "https://api.kalshi.com/trade-api/v2")
FALLBACK = "https://api.elections.kalshi.com/trade-api/v2"
POLL_INTERVAL  = int(os.getenv("POLL_INTERVAL", "10"))
DEFAULT_THRESH = float(os.getenv("DEFAULT_THRESH", "5000"))
DB_PATH = os.getenv("DB_PATH", "/root/apps/valshi/valshi.db")

if not TELEGRAM_TOKEN:
    raise SystemExit("Missing TELEGRAM_TOKEN in .env")

bot = Bot(token=TELEGRAM_TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
dp  = Dispatcher()

MAIN_KB = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="▶️ Alerts ON"), KeyboardButton(text="⏹ Alerts OFF")],
        [KeyboardButton(text="🧾 Recent"), KeyboardButton(text="🏆 Top 24h")],
        [KeyboardButton(text="💵 Set Threshold"), KeyboardButton(text="🧭 Set Topic")],
        [KeyboardButton(text="🌍 Set Timezone"), KeyboardButton(text="📦 Menu/Help")],
    ],
    resize_keyboard=True,
    one_time_keyboard=False
)

class KalshiHTTP:
    def __init__(self):
        self.hosts = [PRIMARY] if os.getenv("KALSHI_BASE_URL") else [PRIMARY, FALLBACK]
        self.host: Optional[str] = None
        self.client = httpx.AsyncClient(timeout=10.0, headers={"User-Agent": "Valshi/1.6"})
        self.min_ts_supported: Optional[bool] = None

    async def pick_host(self):
        for h in self.hosts:
            try:
                r = await self.client.get(h + "/markets", params={"limit": 1, "status": "open"})
                if r.status_code == 200:
                    self.host = h
                    log.info("Using Kalshi host %s", h)
                    return
            except Exception as e:
                log.warning("Host %s failed probe (%s). Trying next…", h, e.__class__.__name__)
        self.host = FALLBACK
        log.warning("All probes failed. Defaulting to %s", self.host)

    async def resilient_get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        if self.host is None:
            await self.pick_host()
        order = [self.host] + [h for h in self.hosts if h != self.host]
        last_err: Optional[Exception] = None
        for h in order:
            url = h + path
            try:
                r = await self.client.get(url, params=params or {})
                if r.status_code >= 500:
                    last_err = RuntimeError(f"{r.status_code} server error")
                    log.warning("GET %s -> %s; trying next host…", url, r.status_code)
                    continue
                r.raise_for_status()
                if h != self.host:
                    self.host = h
                    log.info("Pinned Kalshi host to %s after failover", h)
                return r.json()
            except (httpx.ConnectError, httpx.ReadTimeout, httpx.NetworkError) as e:
                last_err = e
                log.warning("GET %s failed (%s). Trying next host…", url, e.__class__.__name__)
                continue
            except Exception as e:
                last_err = e
                log.error("GET %s failed: %s", url, e)
                break
        raise last_err or RuntimeError("All hosts failed")

    async def get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        return await self.resilient_get(path, params)

KALSHI = KalshiHTTP()

SCHEMA_SQL = """
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS subs (
  user_id     INTEGER PRIMARY KEY,
  alerts_on   INTEGER NOT NULL DEFAULT 0,
  thresh_usd  REAL    NOT NULL DEFAULT 5000,
  topic       TEXT    NOT NULL DEFAULT 'all',
  tz          TEXT    NOT NULL DEFAULT 'UTC',
  created_at  INTEGER NOT NULL DEFAULT (strftime('%s','now'))
);

CREATE TABLE IF NOT EXISTS prints (
  trade_id     TEXT PRIMARY KEY,
  ts_ms        INTEGER NOT NULL,
  ticker       TEXT    NOT NULL,
  side         TEXT    NOT NULL,
  count        INTEGER NOT NULL,
  yes_cents    INTEGER NOT NULL,
  notional_usd REAL    NOT NULL,
  flags        TEXT    NOT NULL DEFAULT ''
);

CREATE INDEX IF NOT EXISTS idx_prints_ts ON prints(ts_ms);
CREATE INDEX IF NOT EXISTS idx_prints_ticker_ts ON prints(ticker, ts_ms);

CREATE TABLE IF NOT EXISTS stats (
  ticker           TEXT PRIMARY KEY,
  last_trade_ts_ms INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS meta (
  key   TEXT PRIMARY KEY,
  value TEXT NOT NULL
);
"""

async def db_init():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.executescript(SCHEMA_SQL)
        try:
            await db.execute("ALTER TABLE subs ADD COLUMN tz TEXT NOT NULL DEFAULT 'UTC'")
            await db.commit()
        except Exception:
            pass
        await db.commit()

async def db_get(db, key: str, default: str = "") -> str:
    cur = await db.execute("SELECT value FROM meta WHERE key=?", (key,))
    row = await cur.fetchone()
    await cur.close()
    return row[0] if row else default

async def db_set(db, key: str, value: str):
    await db.execute(
        "INSERT INTO meta(key,value) VALUES(?,?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (key, value)
    )
    await db.commit()

def now_ms() -> int:
    return int(time.time() * 1000)

def parse_ts_to_ms(ts: Any) -> int:
    if ts is None:
        return now_ms()
    s = str(ts)
    if s.isdigit():
        v = int(s)
        return v if v > 10_000_000_000 else v * 1000
    try:
        return int(datetime.fromisoformat(s.replace("Z","+00:00")).timestamp() * 1000)
    except Exception:
        return now_ms()

def pct_str(p: float) -> str:
    return f"{int(round(p*100))}%"

def categorize_ticker(tk: str) -> str:
    t = (tk or "").upper()
    if "BTC" in t or "CRYPTO" in t: return "crypto"
    if any(k in t for k in ("CPI","FED","RATE","UNEMP","GDP","PCE")): return "macro"
    if any(k in t for k in ("NFL","NBA","MLB","NHL","EPL")): return "sports"
    return "other"

def topic_match(user_topic: str, tk: str) -> bool:
    return user_topic == "all" or categorize_ticker(tk) == user_topic

def median(lst: List[float]) -> float:
    if not lst: return 0.0
    a = sorted(lst); n = len(a); mid = n // 2
    return float(a[mid]) if n % 2 else (a[mid-1] + a[mid]) / 2.0

def local_time_str(ts_ms: int, tz_str: str) -> str:
    try:
        dt = datetime.fromtimestamp(ts_ms/1000, tz=ZoneInfo(tz_str))
        return dt.strftime("%b %d %H:%M %Z")
    except Exception:
        return datetime.fromtimestamp(ts_ms/1000, tz=timezone.utc).strftime("%b %d %H:%M UTC")

def kalshi_market_url_from_ticker(ticker: str) -> str:
    return f"https://kalshi.com/?search={ticker.upper()}"

_titles_cache: Dict[str, str] = {}

async def get_market_title(ticker: str) -> Optional[str]:
    tk = (ticker or "").upper()
    if not tk:
        return None
    if tk in _titles_cache:
        return _titles_cache[tk]
    try:
        data = await KALSHI.get(f"/markets/{tk}")
        obj = data.get("market") or data.get("data") or data
        title = (obj or {}).get("title") or (obj or {}).get("name")
        if title:
            _titles_cache[tk] = str(title)
            return _titles_cache[tk]
    except Exception as e:
        log.debug("Title lookup failed for %s: %s", tk, e)
    return None

async def ensure_sub(db, user_id: int):
    await db.execute(
        "INSERT INTO subs(user_id, alerts_on, thresh_usd, topic, tz) VALUES(?,?,?,?,?) "
        "ON CONFLICT(user_id) DO NOTHING",
        (user_id, 0, DEFAULT_THRESH, 'all', 'UTC')
    )
    await db.commit()

async def get_user_prefs(user_id: int) -> Tuple[int, float, str, str]:
    async with aiosqlite.connect(DB_PATH) as db:
        await ensure_sub(db, user_id)
        cur = await db.execute("SELECT alerts_on, thresh_usd, topic, tz FROM subs WHERE user_id=?", (user_id,))
        row = await cur.fetchone()
        await cur.close()
    if not row:
        return 0, DEFAULT_THRESH, "all", "UTC"
    return int(row[0]), float(row[1]), str(row[2]), str(row[3])

async def fetch_trades_since(min_ts_ms: int, limit: int = 1000) -> List[Dict[str, Any]]:
    if KALSHI.min_ts_supported is None:
        try:
            await KALSHI.get("/markets/trades", params={"limit": 1, "min_ts": min_ts_ms})
            KALSHI.min_ts_supported = True
            log.info("min_ts supported by host")
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 400:
                KALSHI.min_ts_supported = False
                log.info("min_ts NOT supported; switching to latest-trades mode")
            else:
                raise
        except Exception:
            KALSHI.min_ts_supported = False
            log.info("min_ts probe inconclusive; using latest-trades mode")

    params = {"limit": limit}
    if KALSHI.min_ts_supported:
        params["min_ts"] = min_ts_ms

    try:
        data = await KALSHI.get("/markets/trades", params=params)
        return data.get("trades") or data.get("data") or data.get("results") or []
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 400 and KALSHI.min_ts_supported:
            KALSHI.min_ts_supported = False
            log.info("Got 400 with min_ts; disabling min_ts for this session")
            data = await KALSHI.get("/markets/trades", params={"limit": limit})
            return data.get("trades") or data.get("data") or data.get("results") or []
        raise

def trade_notional_usd(tr: Dict[str, Any]) -> float:
    count = int(tr.get("count") or tr.get("size") or 0)
    dollars = None
    if "yes_price_dollars" in tr:
        try: dollars = float(tr["yes_price_dollars"])
        except Exception: dollars = None
    cents = None
    if "yes_price" in tr:
        try: cents = float(tr["yes_price"])
        except Exception: cents = None
    elif "price" in tr:
        try: cents = float(tr["price"])
        except Exception: cents = None
    if dollars is not None: return count * dollars
    if cents is not None: return count * (cents / 100.0)
    return 0.0

def trade_core_fields(tr: Dict[str, Any]) -> Tuple[str, int, str, str, int, int]:
    trade_id = str(tr.get("id") or tr.get("trade_id") or f"t_{tr.get('ticker','')}_{tr.get('created_time','')}_{tr.get('count','')}")
    ts_ms = parse_ts_to_ms(tr.get("created_time") or tr.get("ts"))
    ticker = str(tr.get("ticker") or tr.get("market") or "").upper()
    side   = str(tr.get("side") or tr.get("direction") or "").upper() or "UNKNOWN"
    count  = int(tr.get("count") or tr.get("size") or 0)
    yes_cents = int(float(tr.get("yes_price") or tr.get("price") or 0))
    return trade_id, ts_ms, ticker, side, count, yes_cents

async def ticker_recent_counts_24h(db, ticker: str) -> List[int]:
    since = now_ms() - 24*3600*1000
    cur = await db.execute("SELECT count FROM prints WHERE ticker=? AND ts_ms>=? ORDER BY ts_ms DESC LIMIT 5000", (ticker, since))
    rows = await cur.fetchall()
    await cur.close()
    return [int(r[0]) for r in rows]

async def last_trade_ts_for_ticker(db, ticker: str) -> int:
    cur = await db.execute("SELECT last_trade_ts_ms FROM stats WHERE ticker=?", (ticker,))
    row = await cur.fetchone()
    await cur.close()
    return int(row[0]) if row else 0

async def update_last_trade_ts(db, ticker: str, ts_ms: int):
    await db.execute(
        "INSERT INTO stats(ticker,last_trade_ts_ms) VALUES(?,?) "
        "ON CONFLICT(ticker) DO UPDATE SET last_trade_ts_ms=excluded.last_trade_ts_ms",
        (ticker, ts_ms)
    )
    await db.commit()

async def store_print(db, trade_id: str, ts_ms: int, ticker: str, side: str, count: int, yes_cents: int, notional_usd: float, flags: List[str]):
    try:
        await db.execute(
            "INSERT INTO prints(trade_id, ts_ms, ticker, side, count, yes_cents, notional_usd, flags) VALUES (?,?,?,?,?,?,?,?)",
            (trade_id, ts_ms, ticker, side, count, yes_cents, notional_usd, ",".join(flags))
        )
        await db.commit()
    except aiosqlite.IntegrityError:
        pass

async def load_subscribers(db) -> List[Tuple[int, int, float, str, str]]:
    cur = await db.execute("SELECT user_id, alerts_on, thresh_usd, topic, tz FROM subs")
    rows = await cur.fetchall()
    await cur.close()
    return [(int(r[0]), int(r[1]), float(r[2]), str(r[3]), str(r[4])) for r in rows]

def esc(s: str) -> str:
    return _html.escape(s or "")

def kalshi_btn_for(ticker: str) -> InlineKeyboardMarkup:
    url = kalshi_market_url_from_ticker(ticker)
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=f"🔗 Open {ticker.upper()}", url=url)]])

async def notify_user(uid: int, text: str, kb=None):
    try:
        await bot.send_message(uid, text, reply_markup=kb or MAIN_KB, disable_web_page_preview=True)
    except TelegramNetworkError as e:
        log.warning("Notify %s failed: %s", uid, e)

def clean_title(t: Optional[str], fallback: str) -> str:
    if t and t.strip():
        return t.strip()
    return fallback

def fmt_alert_text(title: str, ticker: str, side: str, notional: float, count: int, yes_cents: int, ts_ms: int, flags: List[str], tz_str: str) -> str:
    yes_price = yes_cents / 100.0 if yes_cents else 0.0
    when = local_time_str(ts_ms, tz_str)
    flag_str = " + ".join(flags) if flags else "Whale"
    prob = pct_str(yes_price)
    return (
        f"🐋 <b>Whale Print Detected</b>\n\n"
        f"🪧 <b>{esc(title)}</b>\n"
        f"🏷️ <b>Ticker:</b> <code>{esc(ticker)}</code>\n"
        f"↔️ <b>Side:</b> {esc(side)}\n"
        f"💰 <b>Notional:</b> <b>${notional:,.0f}</b>  •  {count} @ {yes_price:.2f}  •  Yes {prob}\n"
        f"🕒 <b>When:</b> {esc(when)}\n"
        f"⚡ <b>Signal:</b> {esc(flag_str)}"
    )

async def ingest_loop():
    await db_init()
    async with aiosqlite.connect(DB_PATH) as db:
        v = await db_get(db, "last_ts_ms", "")
        last_ts = int(v) if v.isdigit() else (now_ms() - 10*60*1000)
        log.info("Starting ingest from %s", last_ts)

    while True:
        try:
            trades = await fetch_trades_since(last_ts, limit=1000)
        except Exception as e:
            log.error("Trade fetch failed: %s", e)
            await asyncio.sleep(POLL_INTERVAL)
            continue

        if not trades:
            await asyncio.sleep(POLL_INTERVAL)
            continue

        max_seen = last_ts
        async with aiosqlite.connect(DB_PATH) as db:
            subs = await load_subscribers(db)
            active_subs = [(uid, thr, top, tz) for (uid, on, thr, top, tz) in subs if on == 1]

            for tr in trades:
                trade_id, ts_ms, ticker, side, count, yes_cents = trade_core_fields(tr)
                notional = trade_notional_usd(tr)
                if notional <= 0:
                    continue

                flags: List[str] = []

                last_tick_ts = await last_trade_ts_for_ticker(db, ticker)
                if last_tick_ts and (ts_ms - last_tick_ts) >= 2*3600*1000:
                    flags.append("Silent-breaker")
                await update_last_trade_ts(db, ticker, ts_ms)

                counts24 = await ticker_recent_counts_24h(db, ticker)
                med = median(counts24)
                if med > 0 and count >= 5*med:
                    flags.append("Unusual size")

                await store_print(db, trade_id, ts_ms, ticker, side, count, yes_cents, notional, flags)

                since10 = ts_ms - 10*60*1000
                cur = await db.execute(
                    "SELECT COUNT(*) FROM prints WHERE ticker=? AND ts_ms>=? AND notional_usd>=?",
                    (ticker, since10, DEFAULT_THRESH)
                )
                n10 = (await cur.fetchone())[0]
                await cur.close()
                if n10 >= 3:
                    flags.append("Accumulation")

                for uid, thr, top, tz in active_subs:
                    if notional >= thr and topic_match(top, ticker):
                        title = await get_market_title(ticker)
                        title = clean_title(title, ticker)
                        await notify_user(
                            uid,
                            fmt_alert_text(title, ticker, side, notional, count, yes_cents, ts_ms, flags, tz),
                            kb=kalshi_btn_for(ticker),
                        )

                if ts_ms > max_seen:
                    max_seen = ts_ms

        if max_seen > last_ts:
            last_ts = max_seen
            async with aiosqlite.connect(DB_PATH) as db2:
                await db_set(db2, "last_ts_ms", str(last_ts))

        await asyncio.sleep(POLL_INTERVAL)

HELP_TEXT = (
    "<b>Valshi — Whale Alerts for Kalshi</b>\n"
    "Tracks large trades that may signal high conviction.\n\n"
    "<b>Buttons:</b>\n"
    "• <b>▶️ Alerts ON</b> / <b>⏹ Alerts OFF</b>\n"
    "• <b>🧾 Recent</b> — last whale prints (≥ your threshold)\n"
    "• <b>🏆 Top 24h</b> — biggest prints last 24h\n"
    "• <b>💵 Set Threshold</b> — send a number (e.g., 8000)\n"
    "• <b>🧭 Set Topic</b> — macro / crypto / sports / all\n"
    "• <b>🌍 Set Timezone</b> — send Region/City (e.g., America/New_York)\n"
    "• <b>📦 Menu/Help</b> — show this menu"
)

@dp.message(Command("start"))
async def cmd_start(m: Message):
    await m.answer(HELP_TEXT, reply_markup=MAIN_KB, disable_web_page_preview=True)

@dp.message(F.text == "📦 Menu/Help")
async def btn_menu(m: Message):
    await m.answer(HELP_TEXT, reply_markup=MAIN_KB, disable_web_page_preview=True)

@dp.message(F.text == "▶️ Alerts ON")
async def btn_on(m: Message):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO subs(user_id, alerts_on, thresh_usd, topic, tz) VALUES(?,?,?,?,?) "
            "ON CONFLICT(user_id) DO UPDATE SET alerts_on=1",
            (m.from_user.id, 1, DEFAULT_THRESH, 'all', 'UTC')
        )
        await db.commit()
    await m.answer("✅ Alerts <b>ON</b>.", reply_markup=MAIN_KB)

@dp.message(F.text == "⏹ Alerts OFF")
async def btn_off(m: Message):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO subs(user_id, alerts_on, thresh_usd, topic, tz) VALUES(?,?,?,?,?) "
            "ON CONFLICT(user_id) DO UPDATE SET alerts_on=0",
            (m.from_user.id, 0, DEFAULT_THRESH, 'all', 'UTC')
        )
        await db.commit()
    await m.answer("🛑 Alerts <b>OFF</b>.", reply_markup=MAIN_KB)

@dp.message(F.text == "💵 Set Threshold")
async def btn_thresh(m: Message):
    await m.answer("Send a number like <code>8000</code> for the minimum $ size you want alerts for.", reply_markup=MAIN_KB)

@dp.message(Command("thresh"))
async def cmd_thresh(m: Message):
    await handle_thresh_input(m)

@dp.message(lambda msg: msg.text and msg.text.strip().isdigit())
async def handle_thresh_input(m: Message):
    val = float(m.text.strip())
    if val < 500:
        return await m.answer("Min threshold is <b>$500</b> to avoid spam.", reply_markup=MAIN_KB)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO subs(user_id, alerts_on, thresh_usd, topic, tz) VALUES(?,?,?,?,?) "
            "ON CONFLICT(user_id) DO UPDATE SET thresh_usd=?",
            (m.from_user.id, 1, val, 'all', 'UTC', val)
        )
        await db.commit()
    await m.answer(f"✅ Threshold set to <b>${val:,.0f}</b>.", reply_markup=MAIN_KB)

@dp.message(F.text == "🧭 Set Topic")
async def btn_topic(m: Message):
    await m.answer("Reply with one of: <code>macro</code>, <code>crypto</code>, <code>sports</code>, <code>all</code>.", reply_markup=MAIN_KB)

@dp.message(Command("topic"))
async def cmd_topic(m: Message):
    parts = m.text.split(maxsplit=1)
    if len(parts) < 2:
        return await m.answer("Usage: <code>/topic macro|crypto|sports|all</code>", reply_markup=MAIN_KB)
    await set_topic_value(m, parts[1].strip())

@dp.message(lambda msg: msg.text and msg.text.lower() in ("macro","crypto","sports","all"))
async def handle_topic_word(m: Message):
    await set_topic_value(m, m.text.strip().lower())

async def set_topic_value(m: Message, t: str):
    if t not in ("macro", "crypto", "sports", "all"):
        return await m.answer("Pick one of: <code>macro</code>, <code>crypto</code>, <code>sports</code>, <code>all</code>", reply_markup=MAIN_KB)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO subs(user_id, alerts_on, thresh_usd, topic, tz) VALUES(?,?,?,?,?) "
            "ON CONFLICT(user_id) DO UPDATE SET topic=?",
            (m.from_user.id, 1, DEFAULT_THRESH, t, 'UTC', t)
        )
        await db.commit()
    await m.answer(f"✅ Topic filter: <b>{_html.escape(t)}</b>", reply_markup=MAIN_KB)

@dp.message(F.text == "🌍 Set Timezone")
async def btn_tz(m: Message):
    await m.answer("Send an IANA timezone like <code>Europe/London</code>, <code>America/New_York</code>, <code>Asia/Dubai</code>.", reply_markup=MAIN_KB)

@dp.message(Command("tz"))
async def cmd_tz(m: Message):
    parts = m.text.split(maxsplit=2)
    if len(parts) == 1:
        _, _, _, tz = await get_user_prefs(m.from_user.id)
        return await m.answer(f"Your timezone: <b>{_html.escape(tz)}</b>\nSet with: <code>/tz set Europe/Paris</code>", reply_markup=MAIN_KB)
    if len(parts) >= 3 and parts[1].lower() == "set":
        return await set_timezone_value(m, parts[2].strip())
    return await m.answer("Usage: <code>/tz</code> or <code>/tz set Region/City</code>", reply_markup=MAIN_KB)

@dp.message(lambda msg: msg.text and "/" in msg.text and msg.text.count("/") == 1 and msg.text[0].isalpha())
async def handle_tz_text(m: Message):
    await set_timezone_value(m, m.text.strip())

async def set_timezone_value(m: Message, tz_arg: str):
    try:
        _ = ZoneInfo(tz_arg)
    except Exception:
        return await m.answer("Invalid timezone. Use IANA names like <code>Europe/London</code> or <code>America/New_York</code>.", reply_markup=MAIN_KB)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO subs(user_id, alerts_on, thresh_usd, topic, tz) VALUES(?,?,?,?,?) "
            "ON CONFLICT(user_id) DO UPDATE SET tz=?",
            (m.from_user.id, 0, DEFAULT_THRESH, 'all', tz_arg, tz_arg)
        )
        await db.commit()
    await m.answer(f"✅ Timezone set to <b>{_html.escape(tz_arg)}</b>.", reply_markup=MAIN_KB)

def list_keyboard(tickers: List[str]) -> Optional[InlineKeyboardMarkup]:
    rows = []
    for tk in tickers[:8]:
        url = kalshi_market_url_from_ticker(tk)
        rows.append([InlineKeyboardButton(text=f"🔗 Open {tk.upper()}", url=url)])
    return InlineKeyboardMarkup(inline_keyboard=rows) if rows else None

@dp.message(F.text == "🧾 Recent")
async def btn_recent(m: Message):
    await show_recent(m)

@dp.message(F.text == "🏆 Top 24h")
async def btn_top(m: Message):
    await show_top(m)

@dp.message(Command("recent"))
async def cmd_recent(m: Message):
    await show_recent(m)

@dp.message(Command("top"))
async def cmd_top(m: Message):
    await show_top(m)

async def show_recent(m: Message):
    _, thr, _, tz = await get_user_prefs(m.from_user.id)
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT ts_ms,ticker,side,notional_usd,count,yes_cents,flags "
            "FROM prints WHERE notional_usd>=? ORDER BY ts_ms DESC LIMIT 10",
            (thr,)
        )
        rows = await cur.fetchall()
        await cur.close()
    if not rows:
        return await m.answer("No whale prints (≥ your threshold) yet.", reply_markup=MAIN_KB)

    lines = [f"<b>Recent whale prints (≥ ${thr:,.0f})</b>"]
    tickers = []
    for idx, (ts_ms, tk, side, notional, count, yes_cents, flags) in enumerate(rows, start=1):
        title = await get_market_title(tk)
        title = (title.strip() if title else tk)
        price = (yes_cents or 0)/100.0
        when = local_time_str(ts_ms, tz)
        fl = f" <i>{_html.escape(flags)}</i>" if flags else ""
        lines.append(
            f"{idx}. <b>{_html.escape(title)}</b>  (<code>{_html.escape(tk)}</code>)\n"
            f"   💰 ${notional:,.0f} • {count} @ {price:.2f} • {when}{fl}"
        )
        tickers.append(tk)

    kb = list_keyboard(tickers)
    await m.answer("\n".join(lines), reply_markup=kb or MAIN_KB, disable_web_page_preview=True)

async def show_top(m: Message):
    _, _, _, tz = await get_user_prefs(m.from_user.id)
    nowm = now_ms()
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT ticker, side, MAX(notional_usd), MAX(ts_ms) "
            "FROM prints WHERE ts_ms>=? GROUP BY ticker, side "
            "ORDER BY MAX(notional_usd) DESC LIMIT 10",
            (nowm - 24*3600*1000,)
        )
        rows = await cur.fetchall()
        await cur.close()
    if not rows:
        return await m.answer("No whale prints in the last 24h.", reply_markup=MAIN_KB)

    lines = ["🏆 <b>Top prints (24h)</b>"]
    tickers = []
    for idx, (tk, side, notional, ts_ms) in enumerate(rows, start=1):
        title = await get_market_title(tk)
        title = (title.strip() if title else tk)
        when = local_time_str(ts_ms, tz)
        lines.append(
            f"{idx}. <b>{_html.escape(title)}</b>  (<code>{_html.escape(tk)}</code>)\n"
            f"   💰 ${float(notional):,.0f} • {when}"
        )
        tickers.append(tk)

    kb = list_keyboard(tickers)
    await m.answer("\n".join(lines), reply_markup=kb or MAIN_KB, disable_web_page_preview=True)

@dp.message(Command("health"))
async def cmd_health(m: Message):
    try:
        _ = await KALSHI.get("/markets", params={"limit": 1, "status": "open"})
        kalshi = f"✅ host={KALSHI.host}  min_ts={KALSHI.min_ts_supported}"
    except Exception as e:
        kalshi = f"❌ {e.__class__.__name__} host={KALSHI.host or 'none'}  min_ts={KALSHI.min_ts_supported}"
    await m.answer(
        f"Health:\n• Telegram: ✅\n• Kalshi API: {kalshi}\n"
        f"• Poll interval: {POLL_INTERVAL}s\n• Default thresh: ${DEFAULT_THRESH:,.0f}",
        reply_markup=MAIN_KB,
        disable_web_page_preview=True
    )

async def main():
    await db_init()
    asyncio.create_task(ingest_loop())
    log.info("Valshi starting…")
    try:
        await dp.start_polling(bot)
    finally:
        await KALSHI.client.aclose()
        await bot.session.close()
        log.info("Shutdown complete.")

if __name__ == "__main__":
    asyncio.run(main())
