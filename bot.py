import asyncio
import html
import json
import logging
import os
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional, Set

import aiohttp
from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandObject, CommandStart
from aiogram.types import KeyboardButton, Message, ReplyKeyboardMarkup
from dotenv import load_dotenv

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()


def parse_id_set(raw: str, allow_negative: bool = False) -> Set[int]:
    result: Set[int] = set()
    for item in raw.replace(" ", "").split(","):
        if not item:
            continue
        if allow_negative and item.startswith("-") and item[1:].isdigit():
            result.add(int(item))
        elif item.isdigit():
            result.add(int(item))
    return result


ADMIN_IDS = parse_id_set(os.getenv("ADMIN_IDS", ""))
SIGNAL_CHAT_IDS = parse_id_set(os.getenv("SIGNAL_CHAT_IDS", ""), allow_negative=True)

MIN_SIGNAL_PROBABILITY = int(os.getenv("MIN_SIGNAL_PROBABILITY", "80"))
AUTO_SIGNALS_ENABLED = os.getenv("AUTO_SIGNALS_ENABLED", "true").strip().lower() in {"1", "true", "yes", "on"}
SYMBOLS = [s.strip().upper() for s in os.getenv(
    "SYMBOLS",
    "BTCUSDT,ETHUSDT,BNBUSDT,SOLUSDT,XRPUSDT,ADAUSDT,DOGEUSDT,TRXUSDT,LINKUSDT,AVAXUSDT,TONUSDT,ONTUSDT",
).split(",") if s.strip()]
SIGNAL_TIMEFRAME = os.getenv("SIGNAL_TIMEFRAME", "1h").strip()
SCAN_INTERVAL_SECONDS = int(os.getenv("SCAN_INTERVAL_SECONDS", "600"))
SIGNAL_COOLDOWN_MINUTES = int(os.getenv("SIGNAL_COOLDOWN_MINUTES", "360"))
MAX_SIGNALS_PER_SCAN = int(os.getenv("MAX_SIGNALS_PER_SCAN", "3"))
KLINES_LIMIT = int(os.getenv("KLINES_LIMIT", "160"))

# auto = Bybit first, Binance fallback. Binance is sometimes unavailable from Railway regions.
MARKET_DATA_PROVIDER = os.getenv("MARKET_DATA_PROVIDER", "auto").strip().lower()
BYBIT_API_BASE = os.getenv("BYBIT_API_BASE", "https://api.bybit.com").rstrip("/")
BINANCE_API_BASE = os.getenv("BINANCE_API_BASE", "https://api.binance.com").rstrip("/")

STOP_ATR_MULTIPLIER = float(os.getenv("STOP_ATR_MULTIPLIER", "1.2"))
MIN_RISK_PCT = float(os.getenv("MIN_RISK_PCT", "0.8"))

# This helps you see that the automatic worker is alive. It sends reports to admins only, not subscribers.
AUTO_SCAN_REPORTS_TO_ADMINS = os.getenv("AUTO_SCAN_REPORTS_TO_ADMINS", "true").strip().lower() in {"1", "true", "yes", "on"}
AUTO_SCAN_REPORT_EVERY_N_SCANS = max(1, int(os.getenv("AUTO_SCAN_REPORT_EVERY_N_SCANS", "1")))
TOP_PREVIEW_COUNT = max(1, int(os.getenv("TOP_PREVIEW_COUNT", "5")))

DATA_DIR = Path(__file__).parent / "data"
DATA_DIR.mkdir(exist_ok=True)
SUBSCRIBERS_FILE = DATA_DIR / "subscribers.json"
SENT_SIGNALS_FILE = DATA_DIR / "sent_signals.json"

keyboard = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="📊 Статус"), KeyboardButton(text="🧪 Скан сейчас")],
        [KeyboardButton(text="🆔 Мой ID"), KeyboardButton(text="❓ Помощь")],
        [KeyboardButton(text="🔕 Отписаться")],
    ],
    resize_keyboard=True,
)


@dataclass
class SignalCandidate:
    symbol: str
    side: str
    probability: int
    entry: float
    stop: float
    take_profits: list[float]
    reasons: list[str]
    timeframe: str


@dataclass
class ScanResult:
    candidates: list[SignalCandidate] = field(default_factory=list)
    sendable: list[SignalCandidate] = field(default_factory=list)
    successful_symbols: int = 0
    failed_symbols: int = 0
    skipped_symbols: list[str] = field(default_factory=list)
    data_provider: str = MARKET_DATA_PROVIDER
    scanned_at: float = field(default_factory=time.time)


# ---------- storage ----------

def load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        logging.exception("Не удалось прочитать %s", path)
        return default


def save_json(path: Path, data: Any) -> None:
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def load_subscribers() -> Set[int]:
    data = load_json(SUBSCRIBERS_FILE, [])
    return {int(x) for x in data if str(x).lstrip("-").isdigit()}


def save_subscribers(subscribers: Set[int]) -> None:
    save_json(SUBSCRIBERS_FILE, sorted(subscribers))


def load_sent_signals() -> dict[str, float]:
    data = load_json(SENT_SIGNALS_FILE, {})
    return {str(k): float(v) for k, v in data.items()}


def save_sent_signals(data: dict[str, float]) -> None:
    now = time.time()
    max_age = max(24 * 3600, SIGNAL_COOLDOWN_MINUTES * 60 * 4)
    cleaned = {k: v for k, v in data.items() if now - v <= max_age}
    save_json(SENT_SIGNALS_FILE, cleaned)


def get_recipients() -> Set[int]:
    return load_subscribers() | SIGNAL_CHAT_IDS


# ---------- formatting ----------

def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS


def parse_price(value: str) -> Optional[float]:
    cleaned = value.strip().replace(",", ".")
    try:
        price = float(cleaned)
    except ValueError:
        return None
    if price <= 0:
        return None
    return price


def fmt_price(value: float) -> str:
    if value >= 1000:
        return f"{value:,.2f}".replace(",", " ")
    if value >= 100:
        return f"{value:.2f}".rstrip("0").rstrip(".")
    if value >= 1:
        return f"{value:.4f}".rstrip("0").rstrip(".")
    return f"{value:.8f}".rstrip("0").rstrip(".")


def fmt_pct(value: float) -> str:
    sign = "+" if value > 0 else ""
    return f"{sign}{value:.2f}%"


def pct_from_entry(level: float, entry: float) -> float:
    return (level - entry) / entry * 100


def risk_reward(entry: float, stop: float, target: float, side: str) -> Optional[float]:
    if side == "LONG":
        risk = entry - stop
        reward = target - entry
    else:
        risk = stop - entry
        reward = entry - target
    if risk <= 0 or reward <= 0:
        return None
    return reward / risk


def structured_signal_text(
    symbol: str,
    side: str,
    probability: int,
    entry: float,
    stop: float,
    take_profits: list[float],
    comment: str = "",
    timeframe: str = "",
    auto: bool = False,
) -> str:
    side_clean = side.upper()
    emoji = "🟢" if side_clean == "LONG" else "🔴"
    title_prefix = "🤖 Авто-сигнал" if auto else "Сигнал"

    stop_pct = pct_from_entry(stop, entry)
    tp_lines = []
    for index, tp in enumerate(take_profits, start=1):
        tp_pct = pct_from_entry(tp, entry)
        rr = risk_reward(entry, stop, tp, side_clean)
        rr_text = f" · RR {rr:.2f}" if rr else ""
        tp_lines.append(
            f"TP{index}: <b>{html.escape(fmt_price(tp))}</b> "
            f"(<b>{html.escape(fmt_pct(tp_pct))}</b>{rr_text})"
        )

    comment_block = f"\n\n🧠 Основания:\n{html.escape(comment)}" if comment else ""
    timeframe_block = f"\nТаймфрейм: <b>{html.escape(timeframe)}</b>" if timeframe else ""

    return (
        f"{emoji} <b>{title_prefix}: {html.escape(symbol.upper())} / {html.escape(side_clean)}</b>\n"
        f"Проходимость: <b>{probability}%</b>{timeframe_block}\n\n"
        f"🎯 Вход: <b>{html.escape(fmt_price(entry))}</b>\n"
        f"🛑 Стоп-лосс: <b>{html.escape(fmt_price(stop))}</b> "
        f"(<b>{html.escape(fmt_pct(stop_pct))}</b>)\n"
        f"✅ Тейки:\n" + "\n".join(tp_lines) +
        f"{comment_block}\n\n"
        "⚠️ Не финсовет. Авто-сигнал — техническая оценка, не гарантия. Соблюдай риск-менеджмент."
    )


def scan_summary_text(scan: ScanResult, title: str = "🧪 Отчёт авто-скана") -> str:
    lines = [
        f"<b>{title}</b>",
        f"Порог отправки: <b>{MIN_SIGNAL_PROBABILITY}%</b>",
        f"Таймфрейм: <b>{html.escape(SIGNAL_TIMEFRAME)}</b>",
        f"Источник данных: <b>{html.escape(MARKET_DATA_PROVIDER)}</b>",
        f"Данные получены: <b>{scan.successful_symbols}</b> / {len(SYMBOLS)}",
        f"Ошибки/нет пары: <b>{scan.failed_symbols}</b>",
    ]
    if scan.sendable:
        lines.append("\n<b>Найдены сигналы  выше порога:</b>")
        for c in scan.sendable[:TOP_PREVIEW_COUNT]:
            lines.append(f"• {html.escape(c.symbol)} {c.side} <b>{c.probability}%</b> · вход {html.escape(fmt_price(c.entry))}")
    elif scan.candidates:
        lines.append("\n<b>Лучшие сетапы, но ниже порога:</b>")
        for c in scan.candidates[:TOP_PREVIEW_COUNT]:
            lines.append(f"• {html.escape(c.symbol)} {c.side} <b>{c.probability}%</b> · вход {html.escape(fmt_price(c.entry))}")
    else:
        lines.append("\nСетапов не найдено. Если Данные получены = 0, проблема в источнике данных или SYMBOLS.")
    if scan.skipped_symbols:
        preview = ",".join(scan.skipped_symbols[:10])
        lines.append(f"\nПропущены первые пары: <code>{html.escape(preview)}</code>")
    return "\n".join(lines)


# ---------- indicators ----------

def ema(values: list[float], period: int) -> list[float]:
    if not values:
        return []
    alpha = 2 / (period + 1)
    out = [values[0]]
    for value in values[1:]:
        out.append(value * alpha + out[-1] * (1 - alpha))
    return out


def calculate_rsi(closes: list[float], period: int = 14) -> list[Optional[float]]:
    if len(closes) <= period:
        return [None] * len(closes)
    rsis: list[Optional[float]] = [None] * len(closes)
    gains = []
    losses = []
    for i in range(1, period + 1):
        change = closes[i] - closes[i - 1]
        gains.append(max(change, 0))
        losses.append(abs(min(change, 0)))
    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period

    def rsi_from_avgs(gain: float, loss: float) -> float:
        if loss == 0:
            return 100.0
        rs = gain / loss
        return 100 - (100 / (1 + rs))

    rsis[period] = rsi_from_avgs(avg_gain, avg_loss)
    for i in range(period + 1, len(closes)):
        change = closes[i] - closes[i - 1]
        gain = max(change, 0)
        loss = abs(min(change, 0))
        avg_gain = (avg_gain * (period - 1) + gain) / period
        avg_loss = (avg_loss * (period - 1) + loss) / period
        rsis[i] = rsi_from_avgs(avg_gain, avg_loss)
    return rsis


def calculate_atr(highs: list[float], lows: list[float], closes: list[float], period: int = 14) -> list[Optional[float]]:
    if len(closes) <= period:
        return [None] * len(closes)
    true_ranges = [0.0]
    for i in range(1, len(closes)):
        tr = max(
            highs[i] - lows[i],
            abs(highs[i] - closes[i - 1]),
            abs(lows[i] - closes[i - 1]),
        )
        true_ranges.append(tr)
    atrs: list[Optional[float]] = [None] * len(closes)
    atr = sum(true_ranges[1:period + 1]) / period
    atrs[period] = atr
    for i in range(period + 1, len(closes)):
        atr = (atr * (period - 1) + true_ranges[i]) / period
        atrs[i] = atr
    return atrs


def macd_values(closes: list[float]) -> tuple[list[float], list[float], list[float]]:
    ema12 = ema(closes, 12)
    ema26 = ema(closes, 26)
    macd_line = [a - b for a, b in zip(ema12, ema26)]
    signal_line = ema(macd_line, 9)
    histogram = [m - s for m, s in zip(macd_line, signal_line)]
    return macd_line, signal_line, histogram


# ---------- exchange data ----------

def bybit_interval(interval: str) -> str:
    mapping = {
        "1m": "1", "3m": "3", "5m": "5", "15m": "15", "30m": "30",
        "1h": "60", "2h": "120", "4h": "240", "6h": "360", "12h": "720",
        "1d": "D", "1w": "W", "1M": "M",
    }
    return mapping.get(interval, interval)


async def fetch_binance_klines(session: aiohttp.ClientSession, symbol: str, interval: str, limit: int) -> Optional[list[dict[str, float]]]:
    url = f"{BINANCE_API_BASE}/api/v3/klines"
    params = {"symbol": symbol.upper(), "interval": interval, "limit": str(limit)}
    async with session.get(url, params=params, timeout=12) as response:
        if response.status != 200:
            text = await response.text()
            logging.warning("Binance klines error %s %s: %s", symbol, response.status, text[:160])
            return None
        raw = await response.json()
    candles: list[dict[str, float]] = []
    for item in raw:
        try:
            candles.append({
                "open_time": float(item[0]),
                "open": float(item[1]),
                "high": float(item[2]),
                "low": float(item[3]),
                "close": float(item[4]),
                "volume": float(item[5]),
                "close_time": float(item[6]),
            })
        except Exception:
            continue
    return candles or None


async def fetch_bybit_klines(session: aiohttp.ClientSession, symbol: str, interval: str, limit: int) -> Optional[list[dict[str, float]]]:
    url = f"{BYBIT_API_BASE}/v5/market/kline"
    params = {"category": "spot", "symbol": symbol.upper(), "interval": bybit_interval(interval), "limit": str(limit)}
    async with session.get(url, params=params, timeout=12) as response:
        if response.status != 200:
            text = await response.text()
            logging.warning("Bybit HTTP error %s %s: %s", symbol, response.status, text[:160])
            return None
        raw = await response.json()
    if str(raw.get("retCode")) != "0":
        logging.warning("Bybit API error %s: %s", symbol, str(raw)[:180])
        return None
    items = raw.get("result", {}).get("list", [])
    candles: list[dict[str, float]] = []
    for item in items:
        try:
            start = float(item[0])
            candles.append({
                "open_time": start,
                "open": float(item[1]),
                "high": float(item[2]),
                "low": float(item[3]),
                "close": float(item[4]),
                "volume": float(item[5]),
                "close_time": start,
            })
        except Exception:
            continue
    candles.sort(key=lambda c: c["open_time"])
    return candles or None


async def fetch_klines(session: aiohttp.ClientSession, symbol: str, interval: str, limit: int) -> Optional[list[dict[str, float]]]:
    try:
        if MARKET_DATA_PROVIDER == "bybit":
            return await fetch_bybit_klines(session, symbol, interval, limit)
        if MARKET_DATA_PROVIDER == "binance":
            return await fetch_binance_klines(session, symbol, interval, limit)
        # auto: Bybit first, Binance fallback.
        data = await fetch_bybit_klines(session, symbol, interval, limit)
        if data:
            return data
        return await fetch_binance_klines(session, symbol, interval, limit)
    except Exception:
        logging.exception("Ошибка запроса свечей для %s", symbol)
        return None


def build_stop_and_tps(side: str, entry: float, atr_value: float) -> tuple[float, list[float]]:
    min_risk = entry * (MIN_RISK_PCT / 100)
    risk = max(atr_value * STOP_ATR_MULTIPLIER, min_risk)
    multipliers = [1.0, 1.7, 2.5]
    if side == "LONG":
        stop = max(entry - risk, entry * 0.0001)
        tps = [entry + risk * m for m in multipliers]
    else:
        stop = entry + risk
        tps = [max(entry - risk * m, entry * 0.0001) for m in multipliers]
    return stop, tps


def analyze_candles(symbol: str, candles: list[dict[str, float]]) -> Optional[SignalCandidate]:
    if len(candles) < 80:
        return None
    closes = [c["close"] for c in candles]
    opens = [c["open"] for c in candles]
    highs = [c["high"] for c in candles]
    lows = [c["low"] for c in candles]
    volumes = [c["volume"] for c in candles]
    entry = closes[-1]
    if entry <= 0:
        return None

    ema9 = ema(closes, 9)
    ema21 = ema(closes, 21)
    ema50 = ema(closes, 50)
    rsis = calculate_rsi(closes, 14)
    atrs = calculate_atr(highs, lows, closes, 14)
    _, _, hist = macd_values(closes)

    rsi_now = rsis[-1]
    rsi_prev = rsis[-2]
    atr_now = atrs[-1]
    if rsi_now is None or rsi_prev is None or atr_now is None or atr_now <= 0:
        return None

    avg_volume = sum(volumes[-21:-1]) / 20 if len(volumes) >= 21 else max(volumes[-1], 1)
    volume_ratio = volumes[-1] / avg_volume if avg_volume > 0 else 1
    previous_high_20 = max(highs[-21:-1])
    previous_low_20 = min(lows[-21:-1])
    candle_green = closes[-1] > opens[-1]
    candle_red = closes[-1] < opens[-1]

    long_score = 0
    short_score = 0
    long_reasons: list[str] = []
    short_reasons: list[str] = []

    if entry > ema9[-1] > ema21[-1] > ema50[-1]:
        long_score += 25
        long_reasons.append("EMA 9/21/50 выстроены вверх")
    elif entry < ema9[-1] < ema21[-1] < ema50[-1]:
        short_score += 25
        short_reasons.append("EMA 9/21/50 выстроены вниз")

    if entry > ema21[-1]:
        long_score += 8
    else:
        short_score += 8

    if 52 <= rsi_now <= 68:
        long_score += 15
        long_reasons.append(f"RSI {rsi_now:.1f}: импульс без сильной перекупленности")
    elif 68 < rsi_now <= 75:
        long_score += 6
        long_reasons.append(f"RSI {rsi_now:.1f}: сильный импульс")
    elif rsi_now < 28:
        long_score += 8
        long_reasons.append(f"RSI {rsi_now:.1f}: возможный отскок")

    if 32 <= rsi_now <= 48:
        short_score += 15
        short_reasons.append(f"RSI {rsi_now:.1f}: слабый импульс")
    elif 25 <= rsi_now < 32:
        short_score += 6
        short_reasons.append(f"RSI {rsi_now:.1f}: сильная слабость")
    elif rsi_now > 72:
        short_score += 8
        short_reasons.append(f"RSI {rsi_now:.1f}: риск отката")

    if rsi_now > rsi_prev:
        long_score += 8
    elif rsi_now < rsi_prev:
        short_score += 8

    if len(hist) >= 3:
        if hist[-1] > 0 and hist[-1] > hist[-2]:
            long_score += 17
            long_reasons.append("MACD histogram растёт выше нуля")
        elif hist[-1] < 0 and hist[-1] < hist[-2]:
            short_score += 17
            short_reasons.append("MACD histogram падает ниже нуля")

    if volume_ratio >= 1.2 and candle_green:
        long_score += 10
        long_reasons.append(f"Объём выше среднего x{volume_ratio:.2f}")
    elif volume_ratio >= 1.2 and candle_red:
        short_score += 10
        short_reasons.append(f"Объём выше среднего x{volume_ratio:.2f}")

    if entry >= previous_high_20 * 0.998:
        long_score += 12
        long_reasons.append("Цена рядом с пробоем 20-свечного high")
    if entry <= previous_low_20 * 1.002:
        short_score += 12
        short_reasons.append("Цена рядом с пробоем 20-свечного low")

    if candle_green:
        long_score += 5
    if candle_red:
        short_score += 5

    # Flat/noise filter. Keep weaker candidates visible for debug, but not if both sides are nearly equal.
    if abs(long_score - short_score) < 6:
        return None

    if long_score > short_score:
        side = "LONG"
        probability = min(95, int(round(long_score)))
        reasons = long_reasons or ["лонг-скоринг выше шорт-скоринга"]
    else:
        side = "SHORT"
        probability = min(95, int(round(short_score)))
        reasons = short_reasons or ["шорт-скоринг выше лонг-скоринга"]

    stop, tps = build_stop_and_tps(side, entry, atr_now)
    return SignalCandidate(symbol=symbol, side=side, probability=probability, entry=entry, stop=stop, take_profits=tps, reasons=reasons[:5], timeframe=SIGNAL_TIMEFRAME)


async def scan_market_detailed() -> ScanResult:
    result = ScanResult(data_provider=MARKET_DATA_PROVIDER)
    candidates: list[SignalCandidate] = []
    connector = aiohttp.TCPConnector(limit=10)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [fetch_klines(session, symbol, SIGNAL_TIMEFRAME, KLINES_LIMIT) for symbol in SYMBOLS]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    for symbol, response in zip(SYMBOLS, results):
        if isinstance(response, Exception) or response is None:
            result.failed_symbols += 1
            result.skipped_symbols.append(symbol)
            continue
        result.successful_symbols += 1
        candidate = analyze_candles(symbol, response)
        if candidate:
            candidates.append(candidate)

    candidates.sort(key=lambda x: x.probability, reverse=True)
    result.candidates = candidates
    result.sendable = [c for c in candidates if c.probability >= MIN_SIGNAL_PROBABILITY][:MAX_SIGNALS_PER_SCAN]
    return result


async def scan_market() -> list[SignalCandidate]:
    scan = await scan_market_detailed()
    return scan.sendable


def signal_key(candidate: SignalCandidate) -> str:
    return f"{candidate.symbol}:{candidate.side}:{candidate.timeframe}"


def is_on_cooldown(candidate: SignalCandidate) -> bool:
    sent = load_sent_signals()
    key = signal_key(candidate)
    last_sent = sent.get(key)
    if not last_sent:
        return False
    return time.time() - last_sent < SIGNAL_COOLDOWN_MINUTES * 60


def mark_sent(candidate: SignalCandidate) -> None:
    sent = load_sent_signals()
    sent[signal_key(candidate)] = time.time()
    save_sent_signals(sent)


async def broadcast_to_admins(bot: Bot, text: str) -> None:
    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(admin_id, text)
            await asyncio.sleep(0.05)
        except Exception:
            logging.exception("Не удалось отправить сообщение админу %s", admin_id)


async def broadcast_signal(bot: Bot, candidate: SignalCandidate) -> tuple[int, int]:
    recipients = get_recipients()
    if not recipients:
        return 0, 0
    reasons_text = "\n".join(f"• {reason}" for reason in candidate.reasons) if candidate.reasons else "• технический скоринг выше порога"
    text = structured_signal_text(
        symbol=candidate.symbol.replace("USDT", "/USDT"),
        side=candidate.side,
        probability=candidate.probability,
        entry=candidate.entry,
        stop=candidate.stop,
        take_profits=candidate.take_profits,
        comment=reasons_text,
        timeframe=candidate.timeframe,
        auto=True,
    )
    sent_count = 0
    failed_count = 0
    for chat_id in recipients:
        try:
            await bot.send_message(chat_id, text)
            sent_count += 1
            await asyncio.sleep(0.05)
        except Exception:
            failed_count += 1
            logging.exception("Не удалось отправить авто-сигнал chat_id=%s", chat_id)
    return sent_count, failed_count


async def run_auto_scan_once(bot: Bot, ignore_cooldown: bool = False) -> tuple[ScanResult, list[SignalCandidate], list[SignalCandidate]]:
    scan = await scan_market_detailed()
    sent_candidates: list[SignalCandidate] = []
    skipped_by_cooldown: list[SignalCandidate] = []
    for candidate in scan.sendable:
        if not ignore_cooldown and is_on_cooldown(candidate):
            skipped_by_cooldown.append(candidate)
            continue
        sent_count, failed_count = await broadcast_signal(bot, candidate)
        if sent_count > 0:
            mark_sent(candidate)
            sent_candidates.append(candidate)
            logging.info("Авто-сигнал отправлен %s %s %s%% sent=%s failed=%s", candidate.symbol, candidate.side, candidate.probability, sent_count, failed_count)
    return scan, sent_candidates, skipped_by_cooldown


async def auto_signal_worker(bot: Bot) -> None:
    if not AUTO_SIGNALS_ENABLED:
        logging.info("AUTO_SIGNALS_ENABLED=false, авто-сканер выключен")
        return
    logging.info("Авто-сканер включён: symbols=%s timeframe=%s interval=%ss threshold=%s%% provider=%s", ",".join(SYMBOLS), SIGNAL_TIMEFRAME, SCAN_INTERVAL_SECONDS, MIN_SIGNAL_PROBABILITY, MARKET_DATA_PROVIDER)
    await asyncio.sleep(8)
    scan_number = 0
    while True:
        try:
            scan_number += 1
            scan, sent, skipped = await run_auto_scan_once(bot)
            if AUTO_SCAN_REPORTS_TO_ADMINS and scan_number % AUTO_SCAN_REPORT_EVERY_N_SCANS == 0:
                title = "🤖 Авто-скан прошёл"
                extra = ""
                if sent:
                    extra = "\n\nОтправлены подписчикам: " + ", ".join(f"{c.symbol} {c.side} {c.probability}%" for c in sent)
                elif skipped:
                    extra = "\n\nСигналы есть, но пропущены по cooldown: " + ", ".join(f"{c.symbol} {c.side} {c.probability}%" for c in skipped)
                await broadcast_to_admins(bot, scan_summary_text(scan, title) + extra)
        except Exception:
            logging.exception("Ошибка авто-сканера")
            if AUTO_SCAN_REPORTS_TO_ADMINS:
                await broadcast_to_admins(bot, "⚠️ Ошибка авто-сканера. Проверь Railway Logs.")
        await asyncio.sleep(SCAN_INTERVAL_SECONDS)


# ---------- Telegram handlers ----------

dp = Dispatcher()


@dp.message(CommandStart())
async def cmd_start(message: Message) -> None:
    subscribers = load_subscribers()
    subscribers.add(message.from_user.id)
    save_subscribers(subscribers)
    await message.answer(
        "Привет! Я Telegram-бот для автоматических торговых сигналов.\n\n"
        "Ты подписан на сигналы. Бот сам сканирует рынок и отправляет сетапы "
        f"с проходимостью от {MIN_SIGNAL_PROBABILITY}% и выше.\n\n"
        "Команды: /help, /status, /scan, /id, /stop",
        reply_markup=keyboard,
    )


@dp.message(Command("help"))
async def cmd_help(message: Message) -> None:
    admin_help = ""
    if is_admin(message.from_user.id):
        admin_help = (
            "\n\n<b>Команды админа:</b>\n"
            "• /scan — запустить авто-скан сейчас\n"
            "• /status — настройки авто-сканера\n"
            "• /signal — ручной сигнал\n\n"
            "Ручной формат:\n"
            "<code>/signal TRX LONG 82 0.3235 0.3195 0.3265 0.3290 0.3320 Комментарий</code>"
        )
    await message.answer(
        "<b>Что я умею:</b>\n"
        "• автоматически сканировать монеты\n"
        f"• отправлять только сигналы от {MIN_SIGNAL_PROBABILITY}%\n"
        "• считать стоп и 3 тейка\n"
        "• /start — подписаться\n"
        "• /stop — отписаться\n"
        "• /status — статус бота\n"
        "• /id — показать Telegram ID"
        f"{admin_help}",
        reply_markup=keyboard,
    )


@dp.message(Command("id"))
async def cmd_id(message: Message) -> None:
    await message.answer(f"Твой Telegram ID: <code>{message.from_user.id}</code>")


@dp.message(Command("stop"))
async def cmd_stop(message: Message) -> None:
    subscribers = load_subscribers()
    subscribers.discard(message.from_user.id)
    save_subscribers(subscribers)
    await message.answer("Готово, ты отписан от сигналов. Напиши /start, чтобы подписаться снова.")


@dp.message(Command("status"))
async def cmd_status(message: Message) -> None:
    subscribers = load_subscribers()
    status = "включён" if AUTO_SIGNALS_ENABLED else "выключен"
    await message.answer(
        "<b>Статус авто-бота</b>\n"
        f"Авто-сигналы: <b>{status}</b>\n"
        f"Порог: <b>{MIN_SIGNAL_PROBABILITY}%</b>\n"
        f"Таймфрейм: <b>{html.escape(SIGNAL_TIMEFRAME)}</b>\n"
        f"Интервал скана: <b>{SCAN_INTERVAL_SECONDS} сек.</b>\n"
        f"Cooldown: <b>{SIGNAL_COOLDOWN_MINUTES} мин.</b>\n"
        f"Источник данных: <b>{html.escape(MARKET_DATA_PROVIDER)}</b>\n"
        f"Отчёты админу: <b>{'включены' if AUTO_SCAN_REPORTS_TO_ADMINS else 'выключены'}</b>\n"
        f"Монет: <b>{len(SYMBOLS)}</b>\n"
        f"Монеты: <code>{html.escape(','.join(SYMBOLS[:60]))}{'...' if len(SYMBOLS) > 60 else ''}</code>\n"
        f"Подписчиков: <b>{len(subscribers)}</b>\n"
        f"Доп. чаты из SIGNAL_CHAT_IDS: <b>{len(SIGNAL_CHAT_IDS)}</b>",
        reply_markup=keyboard,
    )


@dp.message(Command("scan"))
async def cmd_scan(message: Message, bot: Bot) -> None:
    if not is_admin(message.from_user.id):
        await message.answer("Эта команда доступна только админу.")
        return
    progress = await message.answer("🧪 Запускаю ручной авто-скан рынка...")
    try:
        scan, sent_candidates, skipped = await run_auto_scan_once(bot, ignore_cooldown=True)
    except Exception as exc:
        logging.exception("Ошибка ручного скана")
        await progress.edit_text(f"Ошибка скана: <code>{html.escape(str(exc))}</code>")
        return
    report = scan_summary_text(scan, "🧪 Ручной авто-скан готов")
    if sent_candidates:
        report += "\n\n<b>Отправлены подписчикам:</b>\n" + "\n".join(f"• {c.symbol} {c.side} {c.probability}%" for c in sent_candidates)
    elif scan.sendable:
        report += "\n\nСигналы выше порога найдены, но не отправлены: нет подписчиков или ошибка отправки."
    await progress.edit_text(report)


@dp.message(Command("signal"))
async def cmd_signal(message: Message, command: CommandObject, bot: Bot) -> None:
    if not is_admin(message.from_user.id):
        await message.answer("Эта команда доступна только админу.")
        return
    if not command.args:
        await message.answer("Пример:\n<code>/signal TRX LONG 82 0.3235 0.3195 0.3265 0.3290 0.3320 Лонг от поддержки</code>")
        return
    parts = command.args.split()
    if len(parts) < 6:
        await message.answer("Неверный формат. Нужно так:\n<code>/signal TRX LONG 82 0.3235 0.3195 0.3265 0.3290 0.3320 Комментарий</code>")
        return
    symbol, side, probability_raw = parts[0], parts[1].upper(), parts[2]
    if side not in {"LONG", "SHORT"}:
        await message.answer("Сторона должна быть LONG или SHORT.")
        return
    try:
        probability = int(probability_raw.strip().replace("%", ""))
        if not 1 <= probability <= 100:
            raise ValueError
    except ValueError:
        await message.answer("Проходимость должна быть числом от 1 до 100. Пример: <code>82</code> или <code>82%</code>")
        return
    if probability < MIN_SIGNAL_PROBABILITY:
        await message.answer(f"⛔️ Сигнал не отправлен: проходимость <b>{probability}%</b>, минимум — <b>{MIN_SIGNAL_PROBABILITY}%</b>.")
        return
    entry = parse_price(parts[3])
    stop = parse_price(parts[4])
    if entry is None or stop is None:
        await message.answer("Вход и стоп должны быть положительными числами. Пример: <code>0.3235 0.3195</code>")
        return
    take_profits: list[float] = []
    comment_start_index = None
    for i, raw in enumerate(parts[5:], start=5):
        price = parse_price(raw)
        if price is None:
            comment_start_index = i
            break
        take_profits.append(price)
        if len(take_profits) == 5:
            comment_start_index = i + 1
            break
    if not take_profits:
        await message.answer("Нужен минимум один тейк-профит после стопа.")
        return
    comment = ""
    if comment_start_index is not None and comment_start_index < len(parts):
        comment = " ".join(parts[comment_start_index:])
    if side == "LONG":
        if stop >= entry:
            await message.answer("Для LONG стоп должен быть ниже входа.")
            return
        if any(tp <= entry for tp in take_profits):
            await message.answer("Для LONG тейки должны быть выше входа.")
            return
    else:
        if stop <= entry:
            await message.answer("Для SHORT стоп должен быть выше входа.")
            return
        if any(tp >= entry for tp in take_profits):
            await message.answer("Для SHORT тейки должны быть ниже входа.")
            return
    recipients = get_recipients()
    if not recipients:
        await message.answer("Пока нет подписчиков и SIGNAL_CHAT_IDS пустой.")
        return
    text = structured_signal_text(symbol, side, probability, entry, stop, take_profits, comment)
    sent = 0
    failed = 0
    for chat_id in recipients:
        try:
            await bot.send_message(chat_id, text)
            sent += 1
            await asyncio.sleep(0.05)
        except Exception:
            failed += 1
            logging.exception("Не удалось отправить сигнал chat_id=%s", chat_id)
    await message.answer(f"Сигнал отправлен. Успешно: {sent}, ошибок: {failed}.")


@dp.message(F.text == "📊 Статус")
async def button_status(message: Message) -> None:
    await cmd_status(message)


@dp.message(F.text == "🧪 Скан сейчас")
async def button_scan(message: Message, bot: Bot) -> None:
    await cmd_scan(message, bot)


@dp.message(F.text == "🆔 Мой ID")
async def button_id(message: Message) -> None:
    await cmd_id(message)


@dp.message(F.text == "❓ Помощь")
async def button_help(message: Message) -> None:
    await cmd_help(message)


@dp.message(F.text == "🔕 Отписаться")
async def button_stop(message: Message) -> None:
    await cmd_stop(message)


@dp.message()
async def fallback(message: Message) -> None:
    await message.answer("Я понял сообщение, но команды такой нет. Нажми /help.", reply_markup=keyboard)


async def main() -> None:
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN не найден. Добавь переменную BOT_TOKEN в Railway")
    logging.basicConfig(level=logging.INFO)
    bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    await bot.delete_webhook(drop_pending_updates=True)
    worker_task = asyncio.create_task(auto_signal_worker(bot))
    try:
        await dp.start_polling(bot)
    finally:
        worker_task.cancel()
        try:
            await worker_task
        except asyncio.CancelledError:
            pass
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
