#!/usr/bin/env python3
"""
بوت "عبد القادر - 10%": يراقب عملات Binance (USDT) ويرسل تنبيه تليجرام فقط.
لا يشتري ولا يبيع أبداً (لا يحتاج صلاحية تداول).

الفكرة: عملة "نايمة" ساعتين -> يرتفع حجمها فجأة -> يبدأ سعرها بالصعود -> نرسل تنبيه.
كل إشارة تُتابَع تلقائياً (هدف / ستوب) ليظهر لك نسبة النجاح الحقيقية.

التشغيل:
    python main.py              # التشغيل العادي (6 مساءً - 2 فجراً بتوقيت بغداد)
    python main.py --test       # يرسل رسالة تجربة لتليجرام ويخرج
    python main.py --anytime    # يتجاهل النافذة الزمنية (للتجربة فقط، حالة منفصلة)
"""
import argparse
import asyncio
import json
import logging
import os
import statistics
import sys
import time
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import aiohttp
from binance import AsyncClient
from binance.exceptions import BinanceAPIException, BinanceRequestException
from dotenv import load_dotenv

# ============================== الإعدادات ==============================
TZ = ZoneInfo("Asia/Baghdad")
WINDOW_START_MIN = 18 * 60          # بداية النافذة: 6 مساءً (المغرب)
WINDOW_END_MIN = 2 * 60             # نهاية النافذة: 2 فجراً (تعبر منتصف الليل)
MAX_SIGNALS_PER_DAY = 2             # صفقتين باليوم فقط
MIN_GAP_BETWEEN_SIGNALS_MIN = 10    # فاصل أدنى بين إشارتين

TARGET_PCT = 10.0                   # هدف الربح
STOP_PCT = 3.0                      # وقف الخسارة

PRICE_MIN, PRICE_MAX = 0.01, 3.0    # نطاق السعر المسموح (دولار)

# --- فلتر الإشارة ---
SLEEP_MINUTES = 120                 # مدة "النوم"
SLEEP_MAX_RANGE_PCT = 1.0           # التذبذب خلال النوم أقل من 1%
SPIKE_MINUTES = 5                   # نافذة قياس ارتفاع الحجم
MIN_SPIKE_PCT = 200.0               # الحجم أعلى من المعتاد بأكثر من 200%
RISE_FROM_SLEEP_PCT = 1.0           # السعر صعد 1% على الأقل من مستوى النوم
BTC_MAX_DROP_PCT = 1.0              # البيتكوين ما ينزل أكثر من 1% خلال آخر ساعة

# --- شروط إضافية للتشدد (حماية من الإشارات الوهمية) ---
MAX_RISE_FROM_SLEEP_PCT = 4.0       # لا نطارد: إذا صعدت أكثر من 4% فات الأوان
MIN_BUY_RATIO = 0.55                # 55% على الأقل من حجم آخر 5 دقائق شراء
MIN_RECENT_QUOTE_USD = 15_000       # أقل حجم مقبول بآخر 5 دقائق (دولار)
MIN_QUOTE_VOLUME_24H = 2_000_000    # أقل سيولة يومية (دولار)
MAX_TICK_PCT = 0.15                 # أصغر خطوة سعر <= 0.15% من السعر (تجنب العملات الخشنة)

SCAN_INTERVAL = 30                  # ثانية بين كل فحص
CONCURRENCY = 8                     # عدد طلبات الشموع المتوازية
TRACK_TIMEOUT_HOURS = 24            # إنهاء متابعة الإشارة بعد هذه المدة

BASE_DIR = Path(__file__).resolve().parent

# قائمة العملات (الرمز فقط). غير الموجود على Binance يُتجاهل تلقائياً.
COINS = """
XRP SUI ENA ARB XLM TRX ADA DOT ATOM FIL OP TIA HBAR ALGO VET GRT FET OCEAN RNDR
THETA AIOZ AKT STORJ HNT ANKR FLUX GLM KDA ONE LSK CKB KAVA CFX CORE STX LTO RLC
AUDIO BAT PYTH BAND API3 BLZ IOTX ARKM WLD ROSE SCRT ONT ICX ZRX HIVE STEEM QTUM
WAVES ZIL ELA NEBL ARDR DYM MANTA MNT POL STRK ZK SEI JASMY IOTA KLAY WAXP CELO
LRC SKL CVC BTT HOT SC DGB RVN KAS POKT RUNE OGN GAS VTHO BICO HIGH LIT ACA GLMR
ASTR CFG KILT PIVX NAV SYS ONG OXT
""".split()
# أسماء تغيّرت على Binance
ALIASES = {"RNDR": "RENDER", "KLAY": "KAIA"}

log = logging.getLogger("abdulqadir")


# ============================== الحالة (ملف JSON) ==============================
class State:
    """يحفظ الحد اليومي والإشارات المفتوحة والإحصائيات لتبقى بعد إعادة التشغيل."""

    def __init__(self, path: Path):
        self.path = path
        self.data = {
            "day": "", "count": 0, "symbols_today": [], "last_signal_ts": 0,
            "session_open": False, "session_count": 0,
            "open": [], "stats": {"win": 0, "loss": 0, "expired": 0},
            "chat_id": "",
        }
        if path.exists():
            try:
                self.data.update(json.loads(path.read_text(encoding="utf-8")))
            except (OSError, ValueError):
                log.warning("تعذر قراءة ملف الحالة، سنبدأ من جديد")

    def save(self):
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps(self.data, ensure_ascii=False, indent=1), encoding="utf-8")
        tmp.replace(self.path)

    def roll_day(self, today: str):
        if self.data["day"] != today:
            self.data.update(day=today, count=0, symbols_today=[], last_signal_ts=0)
            self.save()


# ============================== تليجرام ==============================
class Telegram:
    def __init__(self, token: str, chat_id: str, state: State):
        self.token, self.state = token, state
        self.chat_id = chat_id or state.data.get("chat_id", "")
        self.http: aiohttp.ClientSession | None = None

    async def _session(self):
        if self.http is None or self.http.closed:
            self.http = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=20))
        return self.http

    async def resolve_chat_id(self) -> str:
        """إذا TELEGRAM_CHAT_ID فارغ: نلتقطه من آخر رسالة أرسلتها لبوتك (/start)."""
        if self.chat_id:
            return self.chat_id
        try:
            http = await self._session()
            async with http.get(f"https://api.telegram.org/bot{self.token}/getUpdates") as r:
                data = await r.json()
            for upd in reversed(data.get("result", [])):
                msg = upd.get("message") or upd.get("channel_post") or {}
                if msg.get("chat", {}).get("id") is not None:
                    self.chat_id = str(msg["chat"]["id"])
                    self.state.data["chat_id"] = self.chat_id
                    self.state.save()
                    log.info("تم اكتشاف TELEGRAM_CHAT_ID وحفظه")
                    return self.chat_id
        except Exception as exc:  # لا نطبع الرابط لأنه يحتوي التوكن
            log.error("فشل جلب chat id (%s)", type(exc).__name__)
        log.warning("لا يوجد chat id: افتح بوتك في تليجرام وأرسل /start")
        return ""

    async def send(self, text: str) -> bool:
        if not self.token:
            log.error("TELEGRAM_TOKEN غير موجود في .env")
            return False
        chat_id = await self.resolve_chat_id()
        if not chat_id:
            return False
        for attempt in range(3):
            try:
                http = await self._session()
                async with http.post(
                    f"https://api.telegram.org/bot{self.token}/sendMessage",
                    json={"chat_id": chat_id, "text": text[:4000],
                          "disable_web_page_preview": True},
                ) as r:
                    if r.status == 429:
                        retry = (await r.json()).get("parameters", {}).get("retry_after", 3)
                        await asyncio.sleep(min(int(retry), 30))
                        continue
                    if r.status == 200:
                        return True
                    log.error("تليجرام رد بالحالة %s", r.status)
                    return False
            except Exception as exc:
                log.error("فشل إرسال تليجرام (%s) محاولة %d", type(exc).__name__, attempt + 1)
                await asyncio.sleep(2)
        return False

    async def close(self):
        if self.http and not self.http.closed:
            await self.http.close()


# ============================== المنطق (دوال نقية قابلة للاختبار) ==============================
def in_window(now: datetime) -> bool:
    """هل الوقت داخل النافذة (6 مساءً -> 2 فجراً) بتوقيت بغداد؟ تدعم عبور منتصف الليل."""
    minutes = now.hour * 60 + now.minute
    if WINDOW_START_MIN <= WINDOW_END_MIN:
        return WINDOW_START_MIN <= minutes < WINDOW_END_MIN
    return minutes >= WINDOW_START_MIN or minutes < WINDOW_END_MIN


def trading_day(now: datetime) -> str:
    """يوم التداول: ما بعد منتصف الليل (حتى نهاية النافذة) يتبع جلسة اليوم السابق،
    فالحد اليومي (صفقتين) لا يتصفّر عند 12 ليلاً."""
    shift = WINDOW_END_MIN if WINDOW_START_MIN > WINDOW_END_MIN else 0
    return datetime.fromtimestamp(now.timestamp() - shift * 60, TZ).date().isoformat()


def window_text() -> str:
    f = lambda m: f"{(m // 60) % 24:02d}:{m % 60:02d}"
    return f"{f(WINDOW_START_MIN)} إلى {f(WINDOW_END_MIN)}"


def fmt_price(p: float) -> str:
    return f"{p:.6f}".rstrip("0").rstrip(".") if p < 1 else f"{p:.4f}"


def analyze(klines: list, now_ms: int):
    """
    يفحص شموع الدقيقة (آخر ساعتين + 5 دقائق). يرجع (dict | None, سبب الرفض).
    شكل الشمعة (Binance): [0 وقت الفتح, 1 افتتاح, 2 أعلى, 3 أدنى, 4 إغلاق, 5 حجم,
                           6 وقت الإغلاق, 7 حجم بالدولار, 8 صفقات, 9 شراء(عملة), 10 شراء(دولار)]
    """
    closed = [k for k in klines if int(k[6]) < now_ms]   # نتجاهل الشمعة الجارية
    need = SLEEP_MINUTES + SPIKE_MINUTES
    if len(closed) < need:
        return None, "بيانات غير كافية"
    window = closed[-need:]
    sleep, recent = window[:SLEEP_MINUTES], window[SLEEP_MINUTES:]

    # 1) العملة كانت نايمة: تذبذب أقل من 1% خلال الساعتين
    hi = max(float(k[2]) for k in sleep)
    lo = min(float(k[3]) for k in sleep)
    if lo <= 0:
        return None, "سعر غير صالح"
    sleep_range = (hi - lo) / lo * 100
    if sleep_range >= SLEEP_MAX_RANGE_PCT:
        return None, f"مو نايمة (تذبذب {sleep_range:.2f}%)"

    # 2) حجم آخر 5 دقائق مقابل متوسط كل 5 دقائق خلال النوم
    avg5 = sum(float(k[7]) for k in sleep) / (SLEEP_MINUTES / SPIKE_MINUTES)
    rec_quote = sum(float(k[7]) for k in recent)
    if avg5 <= 0:
        return None, "لا يوجد تداول خلال النوم"
    spike = (rec_quote / avg5 - 1) * 100
    if spike < MIN_SPIKE_PCT:
        return None, f"الحجم ما ارتفع كفاية ({spike:.0f}%)"
    if rec_quote < MIN_RECENT_QUOTE_USD:
        return None, "الحجم الفعلي صغير"

    # 3) ضغط شراء حقيقي وليس بيع
    buy_ratio = sum(float(k[10]) for k in recent) / rec_quote if rec_quote else 0
    if buy_ratio < MIN_BUY_RATIO:
        return None, f"ضغط الشراء ضعيف ({buy_ratio * 100:.0f}%)"

    # 4) السعر بدأ يصعد 1% من مستوى النوم (ولم يفُت الأوان)
    base = statistics.median(float(k[4]) for k in sleep)
    price = float(recent[-1][4])
    rise = (price / base - 1) * 100
    if rise < RISE_FROM_SLEEP_PCT:
        return None, f"السعر لم يصعد بعد ({rise:.2f}%)"
    if rise > MAX_RISE_FROM_SLEEP_PCT:
        return None, f"صعد كثير ({rise:.1f}%) - مطاردة قمة"

    return {"spike": spike, "rise": rise, "buy_ratio": buy_ratio,
            "sleep_range": sleep_range, "rec_quote": rec_quote}, ""


def check_outcome(signal: dict, klines: list):
    """هل ضرب الستوب أو الهدف؟ شمعة تلمس الاثنين = خسارة (تحفظاً). يرجع (حالة, سعر) أو None."""
    since = signal["last_checked_ms"]
    fresh = [k for k in klines if int(k[0]) >= since]
    for k in fresh:
        if float(k[3]) <= signal["stop"]:
            return "loss", signal["stop"], fresh
        if float(k[2]) >= signal["target"]:
            return "win", signal["target"], fresh
    return None, None, fresh


# ============================== Binance ==============================
async def load_universe(client: AsyncClient) -> dict:
    """يرجع {الرمز: خطوة السعر} للعملات في قائمتك الموجودة فعلاً على Binance مقابل USDT."""
    info = await client.get_exchange_info()
    spot = {}
    for s in info["symbols"]:
        if s["status"] == "TRADING" and s["quoteAsset"] == "USDT" and s.get("isSpotTradingAllowed"):
            tick = next((float(f["tickSize"]) for f in s["filters"]
                         if f["filterType"] == "PRICE_FILTER"), 0.0)
            spot[s["baseAsset"]] = (s["symbol"], tick)
    universe, missing = {}, []
    for coin in dict.fromkeys(COINS):  # بدون تكرار
        base = ALIASES.get(coin, coin)
        if base in spot:
            symbol, tick = spot[base]
            universe[symbol] = tick
        else:
            missing.append(coin)
    log.info("عملات محمّلة: %d | غير متوفرة على Binance: %d", len(universe), len(missing))
    if missing:
        log.info("غير المتوفرة (تم تجاهلها): %s", " ".join(missing))
    return universe


async def btc_is_safe(client: AsyncClient) -> bool:
    """حماية: البيتكوين ما نزل أكثر من 1% خلال آخر 60 دقيقة."""
    k = await client.get_klines(symbol="BTCUSDT", interval=AsyncClient.KLINE_INTERVAL_1MINUTE, limit=61)
    change = (float(k[-1][4]) / float(k[0][4]) - 1) * 100
    if change < -BTC_MAX_DROP_PCT:
        log.info("البيتكوين نازل %.2f%% خلال ساعة: لا إشارات", change)
        return False
    return True


async def scan_once(client: AsyncClient, universe: dict, state: State, tg: Telegram):
    """فحص واحد: يرشّح بالسعر والسيولة، ثم يفحص الشموع، ويرسل أفضل إشارة."""
    tickers = await client.get_ticker()  # ملخص 24 ساعة لكل العملات بطلب واحد
    candidates = {}
    for t in tickers:
        sym = t["symbol"]
        if sym not in universe:
            continue
        price = float(t["lastPrice"])
        if not (PRICE_MIN <= price <= PRICE_MAX):
            continue
        if float(t["quoteVolume"]) < MIN_QUOTE_VOLUME_24H:
            continue
        if universe[sym] / price * 100 > MAX_TICK_PCT:
            continue
        if sym in state.data["symbols_today"]:
            continue
        candidates[sym] = price
    if not candidates:
        return
    if not await btc_is_safe(client):
        return

    sem = asyncio.Semaphore(CONCURRENCY)
    now_ms = int(time.time() * 1000)

    async def check(sym):
        async with sem:
            try:
                kl = await client.get_klines(
                    symbol=sym, interval=AsyncClient.KLINE_INTERVAL_1MINUTE,
                    limit=SLEEP_MINUTES + SPIKE_MINUTES + 5)
            except (BinanceAPIException, BinanceRequestException) as exc:
                log.warning("%s: فشل جلب الشموع (%s)", sym, getattr(exc, "message", exc))
                return None
        res, reason = analyze(kl, now_ms)
        return (sym, res) if res else None

    found = [r for r in await asyncio.gather(*(check(s) for s in candidates)) if r]
    if not found:
        return
    sym, res = max(found, key=lambda r: r[1]["spike"])   # الأقوى حجماً
    price = candidates[sym]
    target, stop = price * (1 + TARGET_PCT / 100), price * (1 - STOP_PCT / 100)
    coin = sym[:-4]
    msg = (
        f"🚨 عبد القادر - فرصة: عملة {coin}\n"
        f"السعر: {fmt_price(price)}$\n"
        f"الحجم صاعد: {res['spike']:.0f}%\n"
        f"صعد من النوم: {res['rise']:.1f}% | ضغط الشراء: {res['buy_ratio'] * 100:.0f}%\n"
        f"ادخل: {TARGET_PCT:g}% هدف ({fmt_price(target)}$)، {STOP_PCT:g}% ستوب ({fmt_price(stop)}$)\n\n"
        "إشارة مراقبة وليست نصيحة مالية. الدخول يدوي وعلى مسؤوليتك."
    )
    if await tg.send(msg):
        now_s = int(time.time())
        state.data["count"] += 1
        state.data["session_count"] += 1
        state.data["symbols_today"].append(sym)
        state.data["last_signal_ts"] = now_s
        state.data["open"].append({
            "symbol": sym, "entry": price, "target": target, "stop": stop,
            "created": now_s, "last_checked_ms": now_s * 1000,
        })
        state.save()
        log.info("إشارة: %s سعر=%s حجم=+%.0f%%", sym, fmt_price(price), res["spike"])


async def track_open(client: AsyncClient, state: State, tg: Telegram):
    """يتابع الإشارات المفتوحة ويخبرك عند الهدف أو الستوب أو انتهاء المدة."""
    still_open = []
    for sig in state.data["open"]:
        try:
            kl = await client.get_klines(
                symbol=sig["symbol"], interval=AsyncClient.KLINE_INTERVAL_1MINUTE,
                startTime=sig["last_checked_ms"], limit=1000)
            status, exit_price, fresh = check_outcome(sig, kl)
            if not status and time.time() - sig["created"] > TRACK_TIMEOUT_HOURS * 3600:
                status, exit_price = "expired", float(kl[-1][4]) if kl else sig["entry"]
            if not status:
                if fresh:
                    sig["last_checked_ms"] = int(fresh[-1][0])
                still_open.append(sig)
                continue
        except Exception as exc:
            log.warning("%s: فشل التتبع (%s)", sig["symbol"], exc)
            still_open.append(sig)
            continue

        state.data["stats"][status] += 1
        pnl = (exit_price / sig["entry"] - 1) * 100
        title = {"win": "✅ تحقق الهدف", "loss": "❌ ضرب الستوب", "expired": "⏱ انتهت المدة"}[status]
        s = state.data["stats"]
        total = s["win"] + s["loss"] + s["expired"]
        rate = s["win"] / total * 100 if total else 0
        await tg.send(
            f"{title}: {sig['symbol'][:-4]}\n"
            f"دخول {fmt_price(sig['entry'])} ← خروج {fmt_price(exit_price)} ({pnl:+.1f}%)\n\n"
            f"الإحصائيات: {s['win']} ربح / {s['loss']} خسارة / {s['expired']} منتهية\n"
            f"نسبة النجاح الفعلية: {rate:.0f}% من {total} إشارة")
    state.data["open"] = still_open
    state.save()


# ============================== الحلقة الرئيسية ==============================
async def run(args):
    load_dotenv(BASE_DIR / ".env")
    token = os.getenv("TELEGRAM_TOKEN", "").strip()
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()
    api_key = os.getenv("BINANCE_API_KEY", "").strip() or None
    api_secret = os.getenv("BINANCE_API_SECRET", "").strip() or None

    state = State(BASE_DIR / ("state_test.json" if args.anytime else "state.json"))
    tg = Telegram(token, chat_id, state)

    if args.test:
        ok = await tg.send("✅ بوت عبد القادر: رسالة تجربة. الاتصال بتليجرام يعمل.")
        print("تم الإرسال" if ok else "فشل الإرسال: راجع TELEGRAM_TOKEN وأرسل /start لبوتك")
        await tg.close()
        return

    # مفاتيح Binance اختيارية: بيانات السوق عامة. إذا وضعتها، اجعلها قراءة فقط بدون تداول.
    client = await AsyncClient.create(api_key, api_secret)
    try:
        universe = await load_universe(client)
        if not universe:
            log.error("لا توجد عملات صالحة. تحقق من الاتصال بـ Binance")
            return
        await tg.send(f"🟢 بوت عبد القادر اشتغل. يراقب {len(universe)} عملة "
                      f"من {window_text()} (بتوقيت بغداد).")
        log.info("البوت يعمل. Ctrl+C للإيقاف")

        while True:
            now = datetime.now(TZ)
            state.roll_day(trading_day(now))
            try:
                await track_open(client, state, tg)
                active = args.anytime or in_window(now)
                if active:
                    if not state.data["session_open"]:
                        state.data.update(session_open=True, session_count=0)
                        state.save()
                        await tg.send("🔔 بدأت جلسة المراقبة. إذا ما وصلتك رسالة، يعني ما كو فرصة قوية.")
                    since_last = time.time() - state.data["last_signal_ts"]
                    if (state.data["count"] < MAX_SIGNALS_PER_DAY
                            and since_last >= MIN_GAP_BETWEEN_SIGNALS_MIN * 60):
                        await scan_once(client, universe, state, tg)
                elif state.data["session_open"]:
                    state.data["session_open"] = False
                    state.save()
                    await tg.send(f"🔕 انتهت جلسة اليوم. عدد الإشارات: {state.data['session_count']}")
            except BinanceAPIException as exc:
                log.error("خطأ Binance %s: %s", exc.status_code, exc.message)
                if exc.status_code in (418, 429):
                    await asyncio.sleep(60)   # تخفيف الضغط عند حد الطلبات
                elif exc.status_code == 451:
                    log.error("Binance تحجب منطقتك. استخدم سيرفر/VPN في بلد مسموح")
                    await asyncio.sleep(300)
            except Exception as exc:
                log.exception("خطأ غير متوقع: %s", exc)
            await asyncio.sleep(SCAN_INTERVAL)
    finally:
        await client.close_connection()
        await tg.close()


def main():
    if hasattr(sys.stdout, "reconfigure"):   # ليدعم ويندوز العربي والإيموجي
        sys.stdout.reconfigure(encoding="utf-8")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logging.getLogger("aiohttp").setLevel(logging.WARNING)
    parser = argparse.ArgumentParser(description="بوت عبد القادر 10%")
    parser.add_argument("--test", action="store_true", help="إرسال رسالة تجربة لتليجرام")
    parser.add_argument("--anytime", action="store_true", help="تجاهل النافذة الزمنية (للتجربة)")
    args = parser.parse_args()
    try:
        asyncio.run(run(args))
    except KeyboardInterrupt:
        print("تم الإيقاف")


if __name__ == "__main__":
    main()
