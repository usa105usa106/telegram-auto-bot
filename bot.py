import asyncio
import html
import json
import logging
import os
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional, Set

import aiohttp
import ccxt.async_support as ccxt
from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandObject, CommandStart
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, KeyboardButton, Message, ReplyKeyboardMarkup
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

# Биржи: MEXC Futures и BingX Futures. По умолчанию — MEXC,
# но переключение теперь есть прямо в Telegram через /settings.
MARKET_DATA_PROVIDER = os.getenv("MARKET_DATA_PROVIDER", "mexc").strip().lower()
if MARKET_DATA_PROVIDER not in {"mexc", "bingx"}:
    MARKET_DATA_PROVIDER = "mexc"

MEXC_API_BASE = os.getenv("MEXC_API_BASE", "https://api.mexc.com").rstrip("/")
BINGX_API_BASE = os.getenv("BINGX_API_BASE", "https://open-api.bingx.com").rstrip("/")

MEXC_DYNAMIC_TOP_SYMBOLS = os.getenv("MEXC_DYNAMIC_TOP_SYMBOLS", "true").strip().lower() in {"1", "true", "yes", "on"}
BINGX_DYNAMIC_TOP_SYMBOLS = os.getenv("BINGX_DYNAMIC_TOP_SYMBOLS", "true").strip().lower() in {"1", "true", "yes", "on"}
MEXC_SYMBOLS_LIMIT = max(1, min(150, int(os.getenv("MEXC_SYMBOLS_LIMIT", "100"))))
BINGX_SYMBOLS_LIMIT = max(1, min(150, int(os.getenv("BINGX_SYMBOLS_LIMIT", "100"))))
USE_ENV_SYMBOLS = os.getenv("USE_ENV_SYMBOLS", "false").strip().lower() in {"1", "true", "yes", "on"}

DEFAULT_MEXC_FUTURES_SYMBOLS = [
    "BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "BNBUSDT", "DOGEUSDT", "ADAUSDT", "TRXUSDT",
    "LINKUSDT", "AVAXUSDT", "SUIUSDT", "TONUSDT", "BCHUSDT", "LTCUSDT", "DOTUSDT", "NEARUSDT",
    "UNIUSDT", "AAVEUSDT", "APTUSDT", "ICPUSDT", "FILUSDT", "ETCUSDT", "ATOMUSDT", "HBARUSDT",
    "OPUSDT", "ARBUSDT", "INJUSDT", "SEIUSDT", "TIAUSDT", "JUPUSDT", "PYTHUSDT", "WIFUSDT",
    "PEPEUSDT", "SHIBUSDT", "FLOKIUSDT", "BONKUSDT", "ORDIUSDT", "WLDUSDT", "GALAUSDT", "RUNEUSDT",
    "FTMUSDT", "RENDERUSDT", "ARUSDT", "STXUSDT", "MKRUSDT", "COMPUSDT", "CRVUSDT", "DYDXUSDT",
    "SANDUSDT", "MANAUSDT", "AXSUSDT", "APEUSDT", "LDOUSDT", "ENSUSDT", "ETHFIUSDT", "STRKUSDT",
    "PENDLEUSDT", "NOTUSDT", "ZKUSDT", "ZROUSDT", "JTOUSDT", "JASMYUSDT", "GRTUSDT", "CHZUSDT",
    "ALGOUSDT", "IOTAUSDT", "XLMUSDT", "XMRUSDT", "ZECUSDT", "DASHUSDT", "KASUSDT", "CFXUSDT",
    "MINAUSDT", "EGLDUSDT", "FLOWUSDT", "ROSEUSDT", "KAVAUSDT", "GMTUSDT", "MASKUSDT", "SNXUSDT",
    "1INCHUSDT", "YFIUSDT", "SUSHIUSDT", "ZRXUSDT", "BATUSDT", "RVNUSDT", "LPTUSDT", "ANKRUSDT",
    "WOOUSDT", "BLURUSDT", "CKBUSDT", "CELOUSDT", "QTUMUSDT", "KSMUSDT", "ONTUSDT", "WAVESUSDT",
    "1000PEPEUSDT", "1000SHIBUSDT", "1000BONKUSDT", "1000FLOKIUSDT", "1000RATSUSDT", "1000SATSUSDT"
]
_ENV_SYMBOLS = [s.strip().upper() for s in os.getenv("SYMBOLS", "").split(",") if s.strip()]
SYMBOLS = _ENV_SYMBOLS if USE_ENV_SYMBOLS and _ENV_SYMBOLS else DEFAULT_MEXC_FUTURES_SYMBOLS

SIGNAL_TIMEFRAME = os.getenv("SIGNAL_TIMEFRAME", "15m").strip()
SCAN_INTERVAL_SECONDS = int(os.getenv("SCAN_INTERVAL_SECONDS", "600"))
SIGNAL_COOLDOWN_MINUTES = int(os.getenv("SIGNAL_COOLDOWN_MINUTES", "360"))
MAX_SIGNALS_PER_SCAN = int(os.getenv("MAX_SIGNALS_PER_SCAN", "3"))
KLINES_LIMIT = int(os.getenv("KLINES_LIMIT", "160"))
FETCH_CONCURRENCY = max(1, int(os.getenv("FETCH_CONCURRENCY", "1")))
REQUEST_DELAY_SECONDS = float(os.getenv("REQUEST_DELAY_SECONDS", "0.12"))

BYBIT_API_BASE = os.getenv("BYBIT_API_BASE", "https://api.bybit.com").rstrip("/")
BINANCE_API_BASE = os.getenv("BINANCE_API_BASE", "https://api.binance.com").rstrip("/")
OKX_API_BASE = os.getenv("OKX_API_BASE", "https://www.okx.com").rstrip("/")

STOP_ATR_MULTIPLIER = float(os.getenv("STOP_ATR_MULTIPLIER", "1.2"))
MIN_RISK_PCT = float(os.getenv("MIN_RISK_PCT", "0.8"))

# This helps you see that the automatic worker is alive. It sends reports to admins only, not subscribers.
AUTO_SCAN_REPORTS_TO_ADMINS = os.getenv("AUTO_SCAN_REPORTS_TO_ADMINS", "true").strip().lower() in {"1", "true", "yes", "on"}
AUTO_SCAN_REPORT_EVERY_N_SCANS = max(1, int(os.getenv("AUTO_SCAN_REPORT_EVERY_N_SCANS", "1")))
TOP_PREVIEW_COUNT = max(1, int(os.getenv("TOP_PREVIEW_COUNT", "5")))

# ---- Автоторговля ----
# По умолчанию выключена. Включается кнопками в /settings.
# mode:
#   off   — бот только шлёт сигналы
#   paper — тестовая торговля без реальных ордеров
#   live  — реальные рыночные ордера через API
AUTO_TRADE_MODE = os.getenv("AUTO_TRADE_MODE", "off").strip().lower()
if AUTO_TRADE_MODE not in {"off", "paper", "live"}:
    AUTO_TRADE_MODE = "off"

TRADE_MARGIN_USDT = float(os.getenv("TRADE_MARGIN_USDT", "5"))
TRADE_MARGIN_USDT = max(1.0, min(10000.0, TRADE_MARGIN_USDT))
AUTO_CLOSE_TP_INDEX = int(os.getenv("AUTO_CLOSE_TP_INDEX", "1"))
AUTO_CLOSE_TP_INDEX = max(1, min(3, AUTO_CLOSE_TP_INDEX))

# ---- Умный алгоритм ----
# OFF по умолчанию. Включается в /settings кнопкой "🧠 Умный алгоритм".
# Это не ИИ-прогноз и не гарантия прибыли: бот анализирует историю закрытых авто-сделок,
# штрафует убыточные связки символ/сторона и становится строже после серии минусов.
SMART_ALGORITHM_ENABLED = os.getenv("SMART_ALGORITHM_ENABLED", "false").strip().lower() in {"1", "true", "yes", "on"}
SMART_LOOKBACK_TRADES = max(5, min(200, int(os.getenv("SMART_LOOKBACK_TRADES", "30"))))
SMART_MIN_HISTORY_TRADES = max(3, min(50, int(os.getenv("SMART_MIN_HISTORY_TRADES", "5"))))
SMART_LOSS_STREAK_TRIGGER = max(2, min(10, int(os.getenv("SMART_LOSS_STREAK_TRIGGER", "3"))))
SMART_ADJUSTMENT_CAP = max(3, min(30, int(os.getenv("SMART_ADJUSTMENT_CAP", "15"))))

MAX_ACTIVE_TRADES = int(os.getenv("MAX_ACTIVE_TRADES", "1"))
MAX_ACTIVE_TRADES = max(1, min(20, MAX_ACTIVE_TRADES))
TRADE_MONITOR_INTERVAL_SECONDS = int(os.getenv("TRADE_MONITOR_INTERVAL_SECONDS", "20"))

DATA_DIR = Path(__file__).parent / "data"
DATA_DIR.mkdir(exist_ok=True)
SUBSCRIBERS_FILE = DATA_DIR / "subscribers.json"
SENT_SIGNALS_FILE = DATA_DIR / "sent_signals.json"
SETTINGS_FILE = DATA_DIR / "settings.json"
API_KEYS_FILE = DATA_DIR / "api_keys.json"
TRADES_FILE = DATA_DIR / "trades.json"

TIMEFRAME_OPTIONS = ["5m", "15m", "30m", "1h", "4h"]
PROBABILITY_OPTIONS = [60, 70, 75, 80, 85, 90]
SCAN_INTERVAL_OPTIONS = [120, 300, 600, 900, 1800, 3600]
EXCHANGE_OPTIONS = ["mexc", "bingx"]
AUTO_TRADE_MODE_OPTIONS = ["off", "paper", "live"]
TRADE_MARGIN_OPTIONS = [5, 10, 20, 50, 100, 250]
AUTO_CLOSE_TP_OPTIONS = [1, 2, 3]


def load_runtime_settings() -> dict[str, Any]:
    return load_json(SETTINGS_FILE, {})


def save_runtime_settings() -> None:
    save_json(SETTINGS_FILE, {
        "MIN_SIGNAL_PROBABILITY": MIN_SIGNAL_PROBABILITY,
        "SIGNAL_TIMEFRAME": SIGNAL_TIMEFRAME,
        "SCAN_INTERVAL_SECONDS": SCAN_INTERVAL_SECONDS,
        "MARKET_DATA_PROVIDER": MARKET_DATA_PROVIDER,
        "AUTO_TRADE_MODE": AUTO_TRADE_MODE,
        "TRADE_MARGIN_USDT": TRADE_MARGIN_USDT,
        "AUTO_CLOSE_TP_INDEX": AUTO_CLOSE_TP_INDEX,
        "SMART_ALGORITHM_ENABLED": SMART_ALGORITHM_ENABLED,
    })


def apply_runtime_settings(settings: dict[str, Any]) -> None:
    global MIN_SIGNAL_PROBABILITY, SIGNAL_TIMEFRAME, SCAN_INTERVAL_SECONDS, MARKET_DATA_PROVIDER
    global AUTO_TRADE_MODE, TRADE_MARGIN_USDT, AUTO_CLOSE_TP_INDEX, SMART_ALGORITHM_ENABLED
    try:
        probability = int(settings.get("MIN_SIGNAL_PROBABILITY", MIN_SIGNAL_PROBABILITY))
        MIN_SIGNAL_PROBABILITY = max(1, min(100, probability))
    except Exception:
        pass

    timeframe = str(settings.get("SIGNAL_TIMEFRAME", SIGNAL_TIMEFRAME)).strip()
    if timeframe in TIMEFRAME_OPTIONS:
        SIGNAL_TIMEFRAME = timeframe

    try:
        interval = int(settings.get("SCAN_INTERVAL_SECONDS", SCAN_INTERVAL_SECONDS))
        SCAN_INTERVAL_SECONDS = max(30, min(86400, interval))
    except Exception:
        pass

    exchange = str(settings.get("MARKET_DATA_PROVIDER", MARKET_DATA_PROVIDER)).strip().lower()
    if exchange in EXCHANGE_OPTIONS:
        MARKET_DATA_PROVIDER = exchange

    mode = str(settings.get("AUTO_TRADE_MODE", AUTO_TRADE_MODE)).strip().lower()
    if mode in AUTO_TRADE_MODE_OPTIONS:
        AUTO_TRADE_MODE = mode

    try:
        margin = float(settings.get("TRADE_MARGIN_USDT", TRADE_MARGIN_USDT))
        TRADE_MARGIN_USDT = max(1.0, min(10000.0, margin))
    except Exception:
        pass

    try:
        tp_index = int(settings.get("AUTO_CLOSE_TP_INDEX", AUTO_CLOSE_TP_INDEX))
        AUTO_CLOSE_TP_INDEX = max(1, min(3, tp_index))
    except Exception:
        pass

    smart_raw = settings.get("SMART_ALGORITHM_ENABLED", SMART_ALGORITHM_ENABLED)
    if isinstance(smart_raw, bool):
        SMART_ALGORITHM_ENABLED = smart_raw
    else:
        SMART_ALGORITHM_ENABLED = str(smart_raw).strip().lower() in {"1", "true", "yes", "on"}


def human_interval(seconds: int) -> str:
    if seconds % 3600 == 0:
        hours = seconds // 3600
        return f"{hours} ч"
    if seconds % 60 == 0:
        minutes = seconds // 60
        return f"{minutes} мин"
    return f"{seconds} сек"


def exchange_label(exchange: Optional[str] = None) -> str:
    value = (exchange or MARKET_DATA_PROVIDER).lower()
    if value == "bingx":
        return "BingX Futures"
    return "MEXC Futures"


def symbols_mode_text() -> str:
    if MARKET_DATA_PROVIDER == "mexc" and MEXC_DYNAMIC_TOP_SYMBOLS and not USE_ENV_SYMBOLS:
        return f"топ {MEXC_SYMBOLS_LIMIT} MEXC Futures по 24h обороту"
    if MARKET_DATA_PROVIDER == "bingx" and BINGX_DYNAMIC_TOP_SYMBOLS and not USE_ENV_SYMBOLS:
        return f"топ {BINGX_SYMBOLS_LIMIT} BingX Futures по 24h обороту"
    return "фиксированный список"


def autotrade_label() -> str:
    if AUTO_TRADE_MODE == "live":
        return "LIVE — реальные ордера"
    if AUTO_TRADE_MODE == "paper":
        return "PAPER — тест без ордеров"
    return "OFF — выключена"


def smart_algorithm_label() -> str:
    if SMART_ALGORITHM_ENABLED:
        return "ON — самообучение по закрытым сделкам"
    return "OFF — обычный скоринг"


def settings_menu_text() -> str:
    return (
        "<b>⚙️ Настройки авто-бота</b>\n\n"
        f"Биржа: <b>{html.escape(exchange_label())}</b>\n"
        f"Таймфрейм: <b>{html.escape(SIGNAL_TIMEFRAME)}</b>\n"
        f"Проходимость: <b>{MIN_SIGNAL_PROBABILITY}%</b>\n"
        f"Интервал скана: <b>{human_interval(SCAN_INTERVAL_SECONDS)}</b>\n"
        f"Умный алгоритм: <b>{html.escape(smart_algorithm_label())}</b>\n"
        f"История smart: <b>{html.escape(smart_learning_stats_text())}</b>\n"
        f"Автоторговля: <b>{html.escape(autotrade_label())}</b>\n"
        f"Маржа/объём сделки: <b>${TRADE_MARGIN_USDT:g}</b>\n"
        f"Авто-закрытие: <b>SL или TP{AUTO_CLOSE_TP_INDEX}</b>\n\n"
        "Нажми кнопку ниже, чтобы изменить настройку. Изменения применяются сразу."
    )


def settings_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🏦 Биржа", callback_data="settings:exchange")],
        [
            InlineKeyboardButton(text="⏱ Таймфрейм", callback_data="settings:timeframe"),
            InlineKeyboardButton(text="🎯 Проходимость", callback_data="settings:probability"),
        ],
        [InlineKeyboardButton(text="🔁 Интервал скана", callback_data="settings:interval")],
        [InlineKeyboardButton(text="🧠 Умный алгоритм", callback_data="settings:smart")],
        [InlineKeyboardButton(text="💰 Автоторговля", callback_data="settings:autotrade")],
        [InlineKeyboardButton(text="🔑 API ключи", callback_data="settings:api")],
        [InlineKeyboardButton(text="❌ Закрыть", callback_data="settings:close")],
    ])


def timeframe_keyboard() -> InlineKeyboardMarkup:
    rows = []
    for i in range(0, len(TIMEFRAME_OPTIONS), 3):
        rows.append([
            InlineKeyboardButton(
                text=("✅ " if value == SIGNAL_TIMEFRAME else "") + value,
                callback_data=f"settings:set_timeframe:{value}",
            )
            for value in TIMEFRAME_OPTIONS[i:i + 3]
        ])
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="settings:menu")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def probability_keyboard() -> InlineKeyboardMarkup:
    rows = []
    for i in range(0, len(PROBABILITY_OPTIONS), 3):
        rows.append([
            InlineKeyboardButton(
                text=("✅ " if value == MIN_SIGNAL_PROBABILITY else "") + f"{value}%",
                callback_data=f"settings:set_probability:{value}",
            )
            for value in PROBABILITY_OPTIONS[i:i + 3]
        ])
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="settings:menu")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def interval_keyboard() -> InlineKeyboardMarkup:
    rows = []
    for i in range(0, len(SCAN_INTERVAL_OPTIONS), 2):
        rows.append([
            InlineKeyboardButton(
                text=("✅ " if value == SCAN_INTERVAL_SECONDS else "") + human_interval(value),
                callback_data=f"settings:set_interval:{value}",
            )
            for value in SCAN_INTERVAL_OPTIONS[i:i + 2]
        ])
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="settings:menu")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def exchange_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(
                text=("✅ " if MARKET_DATA_PROVIDER == "mexc" else "") + "MEXC Futures",
                callback_data="settings:set_exchange:mexc",
            )
        ],
        [
            InlineKeyboardButton(
                text=("✅ " if MARKET_DATA_PROVIDER == "bingx" else "") + "BingX Futures",
                callback_data="settings:set_exchange:bingx",
            )
        ],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="settings:menu")],
    ])


def smart_algorithm_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(
                text=("✅ " if not SMART_ALGORITHM_ENABLED else "") + "OFF",
                callback_data="settings:set_smart:off",
            ),
            InlineKeyboardButton(
                text=("✅ " if SMART_ALGORITHM_ENABLED else "") + "ON",
                callback_data="settings:set_smart:on",
            ),
        ],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="settings:menu")],
    ])


def autotrade_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(
                text=("✅ " if AUTO_TRADE_MODE == "off" else "") + "OFF",
                callback_data="settings:set_autotrade_mode:off",
            ),
            InlineKeyboardButton(
                text=("✅ " if AUTO_TRADE_MODE == "paper" else "") + "PAPER",
                callback_data="settings:set_autotrade_mode:paper",
            ),
            InlineKeyboardButton(
                text=("✅ " if AUTO_TRADE_MODE == "live" else "") + "LIVE",
                callback_data="settings:set_autotrade_mode:live",
            ),
        ],
        [InlineKeyboardButton(text="💵 Маржа/объём сделки", callback_data="settings:trade_margin")],
        [InlineKeyboardButton(text="🎯 Авто-закрытие TP", callback_data="settings:close_tp")],
        [InlineKeyboardButton(text="📂 Активные сделки", callback_data="settings:trades")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="settings:menu")],
    ])


def trade_margin_keyboard() -> InlineKeyboardMarkup:
    rows = []
    for i in range(0, len(TRADE_MARGIN_OPTIONS), 3):
        rows.append([
            InlineKeyboardButton(
                text=("✅ " if abs(value - TRADE_MARGIN_USDT) < 1e-9 else "") + f"${value}",
                callback_data=f"settings:set_trade_margin:{value}",
            )
            for value in TRADE_MARGIN_OPTIONS[i:i + 3]
        ])
    rows.append([InlineKeyboardButton(text="✍️ Ввести вручную", callback_data="settings:trade_margin_custom")])
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="settings:autotrade")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def close_tp_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(
                text=("✅ " if AUTO_CLOSE_TP_INDEX == value else "") + f"TP{value}",
                callback_data=f"settings:set_close_tp:{value}",
            )
            for value in AUTO_CLOSE_TP_OPTIONS
        ],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="settings:autotrade")],
    ])


def api_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📌 Как добавить ключи", callback_data="settings:api_help")],
        [InlineKeyboardButton(text="🧹 Очистить ключи текущей биржи", callback_data="settings:api_clear_current")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="settings:menu")],
    ])



keyboard = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="📊 Статус"), KeyboardButton(text="🧪 Скан сейчас")],
        [KeyboardButton(text="🆔 Мой ID"), KeyboardButton(text="❓ Помощь")],
        [KeyboardButton(text="⚙️ Настройки"), KeyboardButton(text="🧠 Умный алгоритм")],
        [KeyboardButton(text="💰 Автоторговля"), KeyboardButton(text="🔑 API")],
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
    total_symbols: int = 0
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


apply_runtime_settings(load_runtime_settings())


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


def load_api_keys() -> dict[str, dict[str, str]]:
    data = load_json(API_KEYS_FILE, {})
    if not isinstance(data, dict):
        data = {}
    # Railway/env ключи тоже поддерживаются, но не обязательны.
    for exchange in ("mexc", "bingx"):
        key = os.getenv(f"{exchange.upper()}_API_KEY", "").strip()
        secret = os.getenv(f"{exchange.upper()}_API_SECRET", "").strip()
        if key and secret and exchange not in data:
            data[exchange] = {"api_key": key, "api_secret": secret}
    return data


def save_api_keys(data: dict[str, dict[str, str]]) -> None:
    save_json(API_KEYS_FILE, data)


def has_api_keys(exchange: Optional[str] = None) -> bool:
    keys = load_api_keys().get((exchange or MARKET_DATA_PROVIDER).lower(), {})
    return bool(keys.get("api_key") and keys.get("api_secret"))


def mask_secret(value: str) -> str:
    if not value:
        return "нет"
    if len(value) <= 8:
        return value[:2] + "***"
    return value[:4] + "..." + value[-4:]


def load_trades() -> list[dict[str, Any]]:
    data = load_json(TRADES_FILE, [])
    return data if isinstance(data, list) else []


def save_trades(trades: list[dict[str, Any]]) -> None:
    save_json(TRADES_FILE, trades)


def trade_pnl_pct_value(trade: dict[str, Any]) -> Optional[float]:
    try:
        if trade.get("pnl_pct") is not None:
            return float(trade.get("pnl_pct"))
        entry = float(trade.get("entry", 0))
        close = float(trade.get("close_price", 0))
        if entry <= 0 or close <= 0:
            return None
        value = pct_from_entry(close, entry)
        if str(trade.get("side", "")).upper() == "SHORT":
            value = -value
        return value
    except Exception:
        return None


def get_closed_trades_for_learning(lookback: Optional[int] = None) -> list[dict[str, Any]]:
    try:
        trades = [t for t in load_trades() if t.get("status") == "closed" and trade_pnl_pct_value(t) is not None]
    except Exception:
        return []
    trades.sort(key=lambda t: float(t.get("closed_at") or t.get("opened_at") or 0), reverse=True)
    if lookback is None:
        lookback = SMART_LOOKBACK_TRADES
    return trades[:max(1, int(lookback))]


def loss_streak_from_trades(trades: list[dict[str, Any]]) -> int:
    streak = 0
    for trade in trades:
        pnl = trade_pnl_pct_value(trade)
        if pnl is not None and pnl < 0:
            streak += 1
        else:
            break
    return streak


def win_rate_from_trades(trades: list[dict[str, Any]]) -> Optional[float]:
    values = [trade_pnl_pct_value(t) for t in trades]
    values = [v for v in values if v is not None]
    if not values:
        return None
    return sum(1 for v in values if v > 0) / len(values)


def smart_learning_stats_text() -> str:
    closed = get_closed_trades_for_learning(SMART_LOOKBACK_TRADES)
    if not closed:
        return "истории пока нет"
    win_rate = win_rate_from_trades(closed)
    loss_streak = loss_streak_from_trades(closed)
    avg_pnl = sum(trade_pnl_pct_value(t) or 0 for t in closed) / len(closed)
    winrate_text = f"{win_rate * 100:.0f}%" if win_rate is not None else "нет"
    return f"сделок {len(closed)}, winrate {winrate_text}, серия минусов {loss_streak}, ср. PnL {avg_pnl:+.2f}%"


def clone_candidate(candidate: SignalCandidate, probability: int, reasons: Optional[list[str]] = None) -> SignalCandidate:
    return SignalCandidate(
        symbol=candidate.symbol,
        side=candidate.side,
        probability=max(1, min(95, int(probability))),
        entry=candidate.entry,
        stop=candidate.stop,
        take_profits=list(candidate.take_profits),
        reasons=list(candidate.reasons if reasons is None else reasons),
        timeframe=candidate.timeframe,
    )


def apply_smart_algorithm(candidate: Optional[SignalCandidate]) -> Optional[SignalCandidate]:
    """Адаптивный фильтр по истории закрытых авто-сделок.

    Логика специально консервативная: после серии минусов бот не пытается "отыграться",
    а режет слабые сетапы и штрафует убыточные символы/стороны.
    """
    if candidate is None or not SMART_ALGORITHM_ENABLED:
        return candidate

    closed = get_closed_trades_for_learning(SMART_LOOKBACK_TRADES)
    if len(closed) < SMART_MIN_HISTORY_TRADES:
        return clone_candidate(
            candidate,
            candidate.probability,
            candidate.reasons + [f"🧠 smart: мало истории ({len(closed)}/{SMART_MIN_HISTORY_TRADES}), базовый скоринг"],
        )

    compact = compact_symbol(candidate.symbol)
    side = candidate.side.upper()
    same_symbol = [t for t in closed if compact_symbol(str(t.get("symbol", ""))) == compact]
    same_symbol_side = [t for t in same_symbol if str(t.get("side", "")).upper() == side]
    same_side = [t for t in closed if str(t.get("side", "")).upper() == side]

    adjustment = 0
    smart_reasons: list[str] = []
    recent_loss_streak = loss_streak_from_trades(closed)
    overall_wr = win_rate_from_trades(closed) or 0

    # Если последние сделки закрывались в минус — включаем защитный режим.
    if recent_loss_streak >= SMART_LOSS_STREAK_TRIGGER:
        adjustment -= 5
        smart_reasons.append(f"🧠 серия минусов {recent_loss_streak}: фильтр строже")
        # В серии минусов слабые сигналы вообще отсекаем.
        if candidate.probability < MIN_SIGNAL_PROBABILITY + 5:
            return None

    # Если конкретная монета+сторона дала два минуса подряд — временно не берём её.
    if len(same_symbol_side) >= 2:
        last_two = same_symbol_side[:2]
        if all((trade_pnl_pct_value(t) or 0) < 0 for t in last_two):
            return None

    # Адаптация по стороне LONG/SHORT.
    if len(same_side) >= 5:
        side_wr = win_rate_from_trades(same_side) or 0
        if side_wr < 0.40:
            adjustment -= 5
            smart_reasons.append(f"🧠 {side} winrate {side_wr * 100:.0f}%: штраф")
        elif side_wr >= 0.65:
            adjustment += 4
            smart_reasons.append(f"🧠 {side} winrate {side_wr * 100:.0f}%: бонус")

    # Адаптация по конкретной монете.
    if len(same_symbol) >= 3:
        symbol_wr = win_rate_from_trades(same_symbol) or 0
        if symbol_wr < 0.34:
            adjustment -= 8
            smart_reasons.append(f"🧠 {display_symbol(candidate.symbol)} winrate {symbol_wr * 100:.0f}%: штраф")
        elif symbol_wr >= 0.67:
            adjustment += 4
            smart_reasons.append(f"🧠 {display_symbol(candidate.symbol)} winrate {symbol_wr * 100:.0f}%: бонус")

    # Если общая история слабая — не разгоняем количество сигналов.
    if overall_wr < 0.45:
        adjustment -= 3
        smart_reasons.append(f"🧠 общий winrate {overall_wr * 100:.0f}%: осторожнее")

    # Слабые однофакторные сетапы уменьшаем.
    if len(candidate.reasons) < 3:
        adjustment -= 3

    adjustment = max(-SMART_ADJUSTMENT_CAP, min(SMART_ADJUSTMENT_CAP, adjustment))
    new_probability = max(1, min(95, candidate.probability + adjustment))
    reasons = candidate.reasons + smart_reasons[:3]
    return clone_candidate(candidate, new_probability, reasons[:7])


def get_open_trades() -> list[dict[str, Any]]:
    return [t for t in load_trades() if t.get("status") == "open"]


def active_trade_for_symbol(symbol: str, exchange: Optional[str] = None) -> Optional[dict[str, Any]]:
    compact = compact_symbol(symbol)
    exchange_value = (exchange or MARKET_DATA_PROVIDER).lower()
    for trade in get_open_trades():
        if compact_symbol(str(trade.get("symbol", ""))) == compact and str(trade.get("exchange", "")).lower() == exchange_value:
            return trade
    return None


def api_status_text() -> str:
    keys = load_api_keys()
    lines = [
        "<b>🔑 API ключи</b>",
        "",
        f"MEXC: <b>{'добавлены' if has_api_keys('mexc') else 'нет'}</b>",
        f"BingX: <b>{'добавлены' if has_api_keys('bingx') else 'нет'}</b>",
        "",
        "Для LIVE-торговли нужны права <b>Read + Trade</b>. <b>Withdraw/вывод средств не включай.</b>",
        "",
        "Добавление ключей командой:",
        "<code>/api_set MEXC API_KEY API_SECRET</code>",
        "<code>/api_set BINGX API_KEY API_SECRET</code>",
        "",
        "После отправки команды бот попробует удалить сообщение с ключами. Но безопаснее использовать отдельный API-ключ без вывода средств и с минимальными правами.",
    ]
    return "\n".join(lines)


def trades_status_text() -> str:
    trades = get_open_trades()
    if not trades:
        return "📂 Открытых авто-сделок нет."
    lines = ["<b>📂 Открытые авто-сделки</b>"]
    for t in trades:
        tp_index = int(t.get("tp_index", 1))
        tps = t.get("take_profits", [])
        tp = tps[tp_index - 1] if len(tps) >= tp_index else None
        lines.append(
            "\n"
            f"• <b>{html.escape(str(t.get('symbol')))} {html.escape(str(t.get('side')))}</b> "
            f"({html.escape(str(t.get('mode')))}, {html.escape(str(t.get('exchange')))}):\n"
            f"  вход {html.escape(fmt_price(float(t.get('entry', 0))))}, "
            f"SL {html.escape(fmt_price(float(t.get('stop', 0))))}, "
            f"TP{tp_index} {html.escape(fmt_price(float(tp))) if tp else 'n/a'}\n"
            f"  объём ≈ ${float(t.get('notional_usdt', 0)):g}, amount {html.escape(str(t.get('amount')))}"
        )
    return "\n".join(lines)


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
        f"Источник данных: <b>{html.escape(exchange_label())}</b>",
        f"Умный алгоритм: <b>{'ON' if SMART_ALGORITHM_ENABLED else 'OFF'}</b>",
        f"Smart-история: <b>{html.escape(smart_learning_stats_text())}</b>",
        f"Данные получены: <b>{scan.successful_symbols}</b> / {scan.total_symbols or len(SYMBOLS)}",
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

def mexc_symbol(symbol: str) -> str:
    clean = symbol.upper().strip().replace("-", "_")
    if "_" in clean:
        return clean
    if clean.endswith("USDT"):
        return clean[:-4] + "_USDT"
    if clean.endswith("USDC"):
        return clean[:-4] + "_USDC"
    return clean


def compact_symbol(symbol: str) -> str:
    return symbol.upper().replace("_", "").replace("-", "")


def display_symbol(symbol: str) -> str:
    if MARKET_DATA_PROVIDER == "mexc":
        return mexc_symbol(symbol)
    if MARKET_DATA_PROVIDER == "bingx":
        return bingx_symbol(symbol)
    return symbol.upper().replace("USDT", "/USDT")


def mexc_interval(interval: str) -> str:
    mapping = {
        "1m": "Min1", "5m": "Min5", "15m": "Min15", "30m": "Min30",
        "1h": "Min60", "4h": "Hour4", "8h": "Hour8",
        "1d": "Day1", "1w": "Week1", "1M": "Month1",
    }
    return mapping.get(interval, "Min15")


def interval_seconds(interval: str) -> int:
    mapping = {
        "1m": 60, "3m": 180, "5m": 300, "15m": 900, "30m": 1800,
        "1h": 3600, "2h": 7200, "4h": 14400, "6h": 21600, "8h": 28800, "12h": 43200,
        "1d": 86400, "1w": 604800, "1M": 2592000,
    }
    return mapping.get(interval, 900)


async def fetch_mexc_top_symbols(session: aiohttp.ClientSession, limit: int) -> Optional[list[str]]:
    url = f"{MEXC_API_BASE}/api/v1/contract/ticker"
    try:
        async with session.get(url, timeout=15) as response:
            if response.status != 200:
                text = await response.text()
                logging.warning("MEXC ticker HTTP error %s: %s", response.status, text[:160])
                return None
            raw = await response.json()
    except Exception:
        logging.exception("Не удалось получить MEXC ticker")
        return None

    if not raw.get("success"):
        logging.warning("MEXC ticker API error: %s", str(raw)[:180])
        return None

    data = raw.get("data", [])
    if isinstance(data, dict):
        data = [data]

    rows: list[tuple[float, str]] = []
    for item in data:
        try:
            raw_symbol = str(item.get("symbol", "")).upper()
            if not raw_symbol.endswith("_USDT"):
                continue
            last_price = float(item.get("lastPrice") or item.get("fairPrice") or 0)
            amount24 = float(item.get("amount24") or 0)
            if last_price <= 0 or amount24 <= 0:
                continue
            rows.append((amount24, compact_symbol(raw_symbol)))
        except Exception:
            continue

    rows.sort(reverse=True, key=lambda x: x[0])
    symbols = []
    seen = set()
    for _, symbol in rows:
        if symbol in seen:
            continue
        seen.add(symbol)
        symbols.append(symbol)
        if len(symbols) >= limit:
            break
    return symbols or None


async def get_symbols_for_scan(session: aiohttp.ClientSession) -> list[str]:
    if MARKET_DATA_PROVIDER == "mexc":
        if MEXC_DYNAMIC_TOP_SYMBOLS and not USE_ENV_SYMBOLS:
            symbols = await fetch_mexc_top_symbols(session, MEXC_SYMBOLS_LIMIT)
            if symbols:
                return symbols
            logging.warning("MEXC dynamic top symbols не получены, использую DEFAULT_MEXC_FUTURES_SYMBOLS")
        return SYMBOLS[:MEXC_SYMBOLS_LIMIT]

    if MARKET_DATA_PROVIDER == "bingx":
        if BINGX_DYNAMIC_TOP_SYMBOLS and not USE_ENV_SYMBOLS:
            symbols = await fetch_bingx_top_symbols(session, BINGX_SYMBOLS_LIMIT)
            if symbols:
                return symbols
            logging.warning("BingX dynamic top symbols не получены, использую DEFAULT_MEXC_FUTURES_SYMBOLS")
        return SYMBOLS[:BINGX_SYMBOLS_LIMIT]

    return SYMBOLS


async def fetch_mexc_klines(session: aiohttp.ClientSession, symbol: str, interval: str, limit: int) -> Optional[list[dict[str, float]]]:
    contract_symbol = mexc_symbol(symbol)
    url = f"{MEXC_API_BASE}/api/v1/contract/kline/{contract_symbol}"
    end_ts = int(time.time())
    start_ts = end_ts - interval_seconds(interval) * max(limit, 100)
    params = {"interval": mexc_interval(interval), "start": str(start_ts), "end": str(end_ts)}
    async with session.get(url, params=params, timeout=15) as response:
        if response.status != 200:
            text = await response.text()
            logging.warning("MEXC kline HTTP error %s %s: %s", contract_symbol, response.status, text[:160])
            return None
        raw = await response.json()
    if not raw.get("success"):
        logging.warning("MEXC kline API error %s: %s", contract_symbol, str(raw)[:180])
        return None

    data = raw.get("data", {})
    times = data.get("time") or []
    opens = data.get("open") or []
    highs = data.get("high") or []
    lows = data.get("low") or []
    closes = data.get("close") or []
    volumes = data.get("vol") or []
    count = min(len(times), len(opens), len(highs), len(lows), len(closes))
    if count < 80:
        return None

    candles: list[dict[str, float]] = []
    for i in range(max(0, count - limit), count):
        try:
            ts = float(times[i]) * 1000
            candles.append({
                "open_time": ts,
                "open": float(opens[i]),
                "high": float(highs[i]),
                "low": float(lows[i]),
                "close": float(closes[i]),
                "volume": float(volumes[i]) if i < len(volumes) else 0.0,
                "close_time": ts,
            })
        except Exception:
            continue
    candles.sort(key=lambda c: c["open_time"])
    return candles or None


def bingx_symbol(symbol: str) -> str:
    clean = symbol.upper().strip().replace("_", "-").replace("/", "-")
    if "-" in clean:
        base, quote = clean.split("-", 1)
        if quote in {"USD", "USDT", "USDC"}:
            return f"{base}-{quote}"
        return clean
    if clean.endswith("USDT"):
        return clean[:-4] + "-USDT"
    if clean.endswith("USDC"):
        return clean[:-4] + "-USDC"
    return clean + "-USDT"


def normalize_user_symbol(raw: str) -> Optional[str]:
    text = raw.strip().upper().replace(" ", "")
    if not text or len(text) > 30:
        return None
    text = text.replace("/", "").replace("_", "").replace("-", "")
    if not all(ch.isalnum() for ch in text):
        return None
    if text in {"LONG", "SHORT", "HELP", "START", "STATUS", "SETTINGS", "SCAN"}:
        return None
    if not text.endswith("USDT") and not text.endswith("USDC"):
        text += "USDT"
    return text


def is_symbol_query(text: str) -> bool:
    normalized = normalize_user_symbol(text)
    if not normalized:
        return False
    # Чтобы обычные фразы не воспринимались как монеты.
    compact = text.strip().replace("/", "").replace("_", "").replace("-", "")
    return 2 <= len(compact) <= 20 and " " not in text.strip()


async def fetch_bingx_top_symbols(session: aiohttp.ClientSession, limit: int) -> Optional[list[str]]:
    url = f"{BINGX_API_BASE}/openApi/swap/v2/quote/ticker"
    try:
        async with session.get(url, timeout=15) as response:
            if response.status != 200:
                text = await response.text()
                logging.warning("BingX ticker HTTP error %s: %s", response.status, text[:160])
                return None
            raw = await response.json()
    except Exception:
        logging.exception("Не удалось получить BingX ticker")
        return None

    if str(raw.get("code")) not in {"0", "200"}:
        logging.warning("BingX ticker API error: %s", str(raw)[:180])
        return None

    data = raw.get("data", [])
    if isinstance(data, dict):
        data = [data]

    rows: list[tuple[float, str]] = []
    for item in data:
        try:
            raw_symbol = str(item.get("symbol", "")).upper().replace("_", "-")
            if not raw_symbol.endswith("-USDT"):
                continue
            last_price = float(item.get("lastPrice") or item.get("price") or item.get("close") or 0)
            amount = float(
                item.get("quoteVolume")
                or item.get("quoteVol")
                or item.get("amount")
                or item.get("turnover")
                or item.get("volume")
                or 0
            )
            if last_price <= 0 or amount <= 0:
                continue
            rows.append((amount, compact_symbol(raw_symbol)))
        except Exception:
            continue

    rows.sort(reverse=True, key=lambda x: x[0])
    symbols = []
    seen = set()
    for _, symbol in rows:
        if symbol in seen:
            continue
        seen.add(symbol)
        symbols.append(symbol)
        if len(symbols) >= limit:
            break
    if symbols:
        return symbols
    return await fetch_bingx_contract_symbols(session, limit)


async def fetch_bingx_contract_symbols(session: aiohttp.ClientSession, limit: int) -> Optional[list[str]]:
    url = f"{BINGX_API_BASE}/openApi/swap/v2/quote/contracts"
    try:
        async with session.get(url, timeout=15) as response:
            if response.status != 200:
                text = await response.text()
                logging.warning("BingX contracts HTTP error %s: %s", response.status, text[:160])
                return None
            raw = await response.json()
    except Exception:
        logging.exception("Не удалось получить BingX contracts")
        return None

    if str(raw.get("code")) not in {"0", "200"}:
        logging.warning("BingX contracts API error: %s", str(raw)[:180])
        return None
    data = raw.get("data", [])
    if isinstance(data, dict):
        data = [data]
    symbols = []
    seen = set()
    for item in data:
        raw_symbol = str(item.get("symbol", "")).upper().replace("_", "-")
        if not raw_symbol.endswith("-USDT"):
            continue
        symbol = compact_symbol(raw_symbol)
        if symbol in seen:
            continue
        seen.add(symbol)
        symbols.append(symbol)
        if len(symbols) >= limit:
            break
    return symbols or None


async def fetch_bingx_klines(session: aiohttp.ClientSession, symbol: str, interval: str, limit: int) -> Optional[list[dict[str, float]]]:
    contract_symbol = bingx_symbol(symbol)
    # Основной актуальный endpoint BingX для USDT-M Perpetual Futures klines.
    urls = [
        f"{BINGX_API_BASE}/openApi/swap/v3/quote/klines",
        f"{BINGX_API_BASE}/openApi/swap/v2/quote/klines",
    ]
    last_raw = None
    for url in urls:
        params = {"symbol": contract_symbol, "interval": interval, "limit": str(min(limit, 1000))}
        try:
            async with session.get(url, params=params, timeout=15) as response:
                if response.status != 200:
                    text = await response.text()
                    logging.warning("BingX kline HTTP error %s %s: %s", contract_symbol, response.status, text[:160])
                    continue
                raw = await response.json()
                last_raw = raw
        except Exception:
            logging.exception("BingX kline request error %s", contract_symbol)
            continue

        if str(raw.get("code")) not in {"0", "200"}:
            logging.warning("BingX kline API error %s: %s", contract_symbol, str(raw)[:180])
            continue

        items = raw.get("data", [])
        if isinstance(items, dict):
            # Некоторые версии API могут вернуть data со списком candles внутри.
            items = items.get("list") or items.get("klines") or items.get("candles") or []
        candles: list[dict[str, float]] = []
        for item in items:
            try:
                if isinstance(item, dict):
                    ts = float(item.get("time") or item.get("openTime") or item.get("timestamp") or 0)
                    candles.append({
                        "open_time": ts,
                        "open": float(item.get("open")),
                        "high": float(item.get("high")),
                        "low": float(item.get("low")),
                        "close": float(item.get("close")),
                        "volume": float(item.get("volume") or item.get("vol") or 0),
                        "close_time": ts,
                    })
                elif isinstance(item, (list, tuple)) and len(item) >= 6:
                    ts = float(item[0])
                    candles.append({
                        "open_time": ts,
                        "open": float(item[1]),
                        "high": float(item[2]),
                        "low": float(item[3]),
                        "close": float(item[4]),
                        "volume": float(item[5]) if len(item) > 5 else 0.0,
                        "close_time": ts,
                    })
            except Exception:
                continue
        candles = [c for c in candles if c["open"] > 0 and c["high"] > 0 and c["low"] > 0 and c["close"] > 0]
        candles.sort(key=lambda c: c["open_time"])
        if len(candles) >= 80:
            return candles[-limit:]
    if last_raw is not None:
        logging.warning("BingX no usable candles %s: %s", contract_symbol, str(last_raw)[:180])
    return None


def bybit_interval(interval: str) -> str:
    mapping = {
        "1m": "1", "3m": "3", "5m": "5", "15m": "15", "30m": "30",
        "1h": "60", "2h": "120", "4h": "240", "6h": "360", "12h": "720",
        "1d": "D", "1w": "W", "1M": "M",
    }
    return mapping.get(interval, interval)


def okx_symbol(symbol: str) -> str:
    clean = symbol.upper().strip()
    if "-" in clean:
        return clean
    if clean.endswith("USDT"):
        return clean[:-4] + "-USDT"
    if clean.endswith("USDC"):
        return clean[:-4] + "-USDC"
    return clean


def okx_interval(interval: str) -> str:
    mapping = {
        "1m": "1m", "3m": "3m", "5m": "5m", "15m": "15m", "30m": "30m",
        "1h": "1H", "2h": "2H", "4h": "4H", "6h": "6H", "12h": "12H",
        "1d": "1D", "1w": "1W", "1M": "1M",
    }
    return mapping.get(interval, interval)


async def fetch_okx_klines(session: aiohttp.ClientSession, symbol: str, interval: str, limit: int) -> Optional[list[dict[str, float]]]:
    url = f"{OKX_API_BASE}/api/v5/market/candles"
    params = {"instId": okx_symbol(symbol), "bar": okx_interval(interval), "limit": str(min(limit, 300))}
    async with session.get(url, params=params, timeout=12) as response:
        if response.status != 200:
            text = await response.text()
            logging.warning("OKX candles HTTP error %s %s: %s", symbol, response.status, text[:160])
            return None
        raw = await response.json()
    if str(raw.get("code")) != "0":
        logging.warning("OKX API error %s: %s", symbol, str(raw)[:180])
        return None
    items = raw.get("data", [])
    candles: list[dict[str, float]] = []
    for item in items:
        try:
            ts = float(item[0])
            candles.append({
                "open_time": ts,
                "open": float(item[1]),
                "high": float(item[2]),
                "low": float(item[3]),
                "close": float(item[4]),
                "volume": float(item[5]) if len(item) > 5 else 0.0,
                "close_time": ts,
            })
        except Exception:
            continue
    candles.sort(key=lambda c: c["open_time"])
    return candles or None


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
        if MARKET_DATA_PROVIDER == "mexc":
            return await fetch_mexc_klines(session, symbol, interval, limit)
        if MARKET_DATA_PROVIDER == "bingx":
            return await fetch_bingx_klines(session, symbol, interval, limit)
        if MARKET_DATA_PROVIDER == "okx":
            return await fetch_okx_klines(session, symbol, interval, limit)
        if MARKET_DATA_PROVIDER == "bybit":
            return await fetch_bybit_klines(session, symbol, interval, limit)
        if MARKET_DATA_PROVIDER == "binance":
            return await fetch_binance_klines(session, symbol, interval, limit)
        # auto: MEXC first, then BingX, then OKX, then Bybit, then Binance.
        for fetcher in (fetch_mexc_klines, fetch_bingx_klines, fetch_okx_klines, fetch_bybit_klines, fetch_binance_klines):
            data = await fetcher(session, symbol, interval, limit)
            if data:
                return data
        return None
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
    connector = aiohttp.TCPConnector(limit=max(1, FETCH_CONCURRENCY))
    async with aiohttp.ClientSession(connector=connector) as session:
        symbols_to_scan = await get_symbols_for_scan(session)
        result.total_symbols = len(symbols_to_scan)
        semaphore = asyncio.Semaphore(max(1, FETCH_CONCURRENCY))

        async def limited_fetch(symbol: str):
            async with semaphore:
                data = await fetch_klines(session, symbol, SIGNAL_TIMEFRAME, KLINES_LIMIT)
                if REQUEST_DELAY_SECONDS > 0:
                    await asyncio.sleep(REQUEST_DELAY_SECONDS)
                return data

        tasks = [limited_fetch(symbol) for symbol in symbols_to_scan]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    for symbol, response in zip(symbols_to_scan, results):
        if isinstance(response, Exception) or response is None:
            result.failed_symbols += 1
            result.skipped_symbols.append(symbol)
            continue
        result.successful_symbols += 1
        candidate = apply_smart_algorithm(analyze_candles(symbol, response))
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
        symbol=display_symbol(candidate.symbol),
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


def autotrading_is_enabled() -> bool:
    return AUTO_TRADE_MODE in {"paper", "live"}


async def fetch_klines_for_exchange(session: aiohttp.ClientSession, exchange: str, symbol: str, interval: str, limit: int) -> Optional[list[dict[str, float]]]:
    exchange_value = exchange.lower()
    try:
        if exchange_value == "mexc":
            return await fetch_mexc_klines(session, symbol, interval, limit)
        if exchange_value == "bingx":
            return await fetch_bingx_klines(session, symbol, interval, limit)
    except Exception:
        logging.exception("Ошибка запроса цены для активной сделки %s %s", exchange, symbol)
    return None


def trade_target_price(trade: dict[str, Any]) -> Optional[float]:
    tps = trade.get("take_profits") or []
    try:
        tp_index = int(trade.get("tp_index", AUTO_CLOSE_TP_INDEX))
        if 1 <= tp_index <= len(tps):
            return float(tps[tp_index - 1])
    except Exception:
        return None
    return None


def trade_exit_reason(trade: dict[str, Any], last_price: float) -> Optional[str]:
    side = str(trade.get("side", "")).upper()
    stop = float(trade.get("stop", 0))
    target = trade_target_price(trade)
    if side == "LONG":
        if last_price <= stop:
            return "SL"
        if target is not None and last_price >= target:
            return f"TP{int(trade.get('tp_index', AUTO_CLOSE_TP_INDEX))}"
    if side == "SHORT":
        if last_price >= stop:
            return "SL"
        if target is not None and last_price <= target:
            return f"TP{int(trade.get('tp_index', AUTO_CLOSE_TP_INDEX))}"
    return None


def create_ccxt_exchange(exchange_name: str):
    exchange_value = exchange_name.lower()
    keys = load_api_keys().get(exchange_value, {})
    api_key = keys.get("api_key", "")
    api_secret = keys.get("api_secret", "")
    config = {
        "apiKey": api_key,
        "secret": api_secret,
        "enableRateLimit": True,
        "options": {"defaultType": "swap"},
    }
    if exchange_value == "mexc":
        return ccxt.mexc(config)
    if exchange_value == "bingx":
        return ccxt.bingx(config)
    raise ValueError(f"Биржа {exchange_name} не поддерживается для автоторговли")


def ccxt_symbol_candidates(symbol: str) -> tuple[str, str]:
    clean = compact_symbol(symbol)
    quote = "USDT" if clean.endswith("USDT") else "USDC" if clean.endswith("USDC") else "USDT"
    base = clean[:-len(quote)] if clean.endswith(quote) else clean
    return base, quote


async def find_ccxt_market_symbol(exchange, symbol: str) -> str:
    await exchange.load_markets()
    base, quote = ccxt_symbol_candidates(symbol)
    preferred = [
        f"{base}/{quote}:{quote}",
        f"{base}/{quote}",
        f"{base}_{quote}",
        f"{base}-{quote}",
        f"{base}{quote}",
    ]
    for candidate in preferred:
        if candidate in exchange.markets:
            return candidate
    for market_symbol, market in exchange.markets.items():
        try:
            if str(market.get("base", "")).upper() == base and str(market.get("quote", "")).upper() == quote:
                if market.get("swap") or market.get("future") or market.get("linear"):
                    return market_symbol
        except Exception:
            continue
    raise ValueError(f"Пара {symbol} не найдена в CCXT markets для {exchange.id}")


async def execute_live_order(exchange_name: str, symbol: str, side: str, amount: float, reduce_only: bool = False) -> dict[str, Any]:
    exchange = create_ccxt_exchange(exchange_name)
    try:
        market_symbol = await find_ccxt_market_symbol(exchange, symbol)
        amount_precise = float(exchange.amount_to_precision(market_symbol, amount))
        if amount_precise <= 0:
            raise ValueError("Размер ордера получился 0 после округления биржи")
        order_side = side.lower()
        params = {"reduceOnly": reduce_only}
        order = await exchange.create_order(market_symbol, "market", order_side, amount_precise, None, params)
        return {
            "market_symbol": market_symbol,
            "amount": amount_precise,
            "order": order,
        }
    finally:
        try:
            await exchange.close()
        except Exception:
            pass


async def open_autotrade_for_signal(bot: Bot, candidate: SignalCandidate) -> Optional[dict[str, Any]]:
    if not autotrading_is_enabled():
        return None

    exchange_value = MARKET_DATA_PROVIDER
    symbol = candidate.symbol

    if active_trade_for_symbol(symbol, exchange_value):
        await broadcast_to_admins(bot, f"ℹ️ Автоторговля: по {html.escape(display_symbol(symbol))} уже есть открытая сделка, новую не открываю.")
        return None

    open_trades = get_open_trades()
    if len(open_trades) >= MAX_ACTIVE_TRADES:
        await broadcast_to_admins(bot, f"⛔️ Автоторговля: лимит открытых сделок {MAX_ACTIVE_TRADES}, новую не открываю.")
        return None

    if AUTO_TRADE_MODE == "live" and not has_api_keys(exchange_value):
        await broadcast_to_admins(
            bot,
            "⛔️ LIVE-автоторговля не открыла сделку: нет API ключей для текущей биржи.\n"
            "Добавь ключи командой /api_set или переключи режим в PAPER."
        )
        return None

    # В целях безопасности TRADE_MARGIN_USDT трактуется как максимальный USDT-объём позиции.
    # При плече на бирже фактическая маржа может отличаться, поэтому начинай с минимальных сумм.
    notional_usdt = float(TRADE_MARGIN_USDT)
    amount = notional_usdt / candidate.entry
    trade_id = uuid.uuid4().hex[:12]
    trade = {
        "id": trade_id,
        "status": "open",
        "mode": AUTO_TRADE_MODE,
        "exchange": exchange_value,
        "symbol": symbol,
        "display_symbol": display_symbol(symbol),
        "side": candidate.side,
        "probability": candidate.probability,
        "timeframe": candidate.timeframe,
        "entry": candidate.entry,
        "stop": candidate.stop,
        "take_profits": candidate.take_profits,
        "tp_index": AUTO_CLOSE_TP_INDEX,
        "notional_usdt": notional_usdt,
        "amount": amount,
        "opened_at": time.time(),
        "open_order_id": "paper",
    }

    try:
        if AUTO_TRADE_MODE == "live":
            order_side = "buy" if candidate.side == "LONG" else "sell"
            result = await execute_live_order(exchange_value, symbol, order_side, amount, reduce_only=False)
            trade["amount"] = result["amount"]
            trade["ccxt_symbol"] = result["market_symbol"]
            trade["open_order_id"] = str((result.get("order") or {}).get("id") or "live")
        trades = load_trades()
        trades.append(trade)
        save_trades(trades)
        await broadcast_to_admins(
            bot,
            "💰 <b>Авто-сделка открыта</b>\n"
            f"Режим: <b>{html.escape(AUTO_TRADE_MODE.upper())}</b>\n"
            f"Биржа: <b>{html.escape(exchange_label(exchange_value))}</b>\n"
            f"Пара: <b>{html.escape(display_symbol(symbol))}</b>\n"
            f"Сторона: <b>{candidate.side}</b>\n"
            f"Объём/маржа лимит: <b>${notional_usdt:g}</b>\n"
            f"Amount: <b>{html.escape(str(trade['amount']))}</b>\n"
            f"Закрытие: <b>SL или TP{AUTO_CLOSE_TP_INDEX}</b>"
        )
        return trade
    except Exception as exc:
        logging.exception("Не удалось открыть авто-сделку")
        await broadcast_to_admins(bot, f"⚠️ Не удалось открыть авто-сделку: <code>{html.escape(str(exc))}</code>")
        return None


async def close_autotrade(bot: Bot, trade: dict[str, Any], reason: str, last_price: float) -> bool:
    trades = load_trades()
    found = False
    for item in trades:
        if item.get("id") == trade.get("id") and item.get("status") == "open":
            found = True
            break
    if not found:
        return False

    try:
        if str(trade.get("mode")) == "live":
            close_side = "sell" if str(trade.get("side")).upper() == "LONG" else "buy"
            await execute_live_order(
                str(trade.get("exchange")),
                str(trade.get("symbol")),
                close_side,
                float(trade.get("amount", 0)),
                reduce_only=True,
            )

        pnl_pct = pct_from_entry(last_price, float(trade.get("entry", last_price)))
        if str(trade.get("side")).upper() == "SHORT":
            pnl_pct = -pnl_pct

        now = time.time()
        for item in trades:
            if item.get("id") == trade.get("id") and item.get("status") == "open":
                item["status"] = "closed"
                item["closed_at"] = now
                item["close_reason"] = reason
                item["close_price"] = last_price
                item["pnl_pct"] = pnl_pct
                break
        save_trades(trades)
        await broadcast_to_admins(
            bot,
            "✅ <b>Авто-сделка закрыта</b>\n"
            f"Пара: <b>{html.escape(str(trade.get('display_symbol') or trade.get('symbol')))}</b>\n"
            f"Сторона: <b>{html.escape(str(trade.get('side')))}</b>\n"
            f"Причина: <b>{html.escape(reason)}</b>\n"
            f"Цена закрытия: <b>{html.escape(fmt_price(last_price))}</b>\n"
            f"PnL примерно: <b>{html.escape(fmt_pct(pnl_pct))}</b>"
        )
        return True
    except Exception as exc:
        logging.exception("Не удалось закрыть авто-сделку")
        await broadcast_to_admins(bot, f"⚠️ Не удалось закрыть авто-сделку {html.escape(str(trade.get('symbol')))}: <code>{html.escape(str(exc))}</code>")
        return False


async def trade_monitor_worker(bot: Bot) -> None:
    await asyncio.sleep(15)
    while True:
        try:
            open_trades = get_open_trades()
            if open_trades:
                async with aiohttp.ClientSession() as session:
                    for trade in open_trades:
                        candles = await fetch_klines_for_exchange(
                            session,
                            str(trade.get("exchange", MARKET_DATA_PROVIDER)),
                            str(trade.get("symbol")),
                            SIGNAL_TIMEFRAME,
                            100,
                        )
                        if not candles:
                            continue
                        last_price = float(candles[-1]["close"])
                        reason = trade_exit_reason(trade, last_price)
                        if reason:
                            await close_autotrade(bot, trade, reason, last_price)
                        await asyncio.sleep(0.15)
        except Exception:
            logging.exception("Ошибка мониторинга авто-сделок")
            await broadcast_to_admins(bot, "⚠️ Ошибка мониторинга авто-сделок. Проверь Railway Logs.")
        await asyncio.sleep(max(5, TRADE_MONITOR_INTERVAL_SECONDS))


async def run_auto_scan_once(
    bot: Bot,
    ignore_cooldown: bool = False,
    allow_trading: bool = True,
) -> tuple[ScanResult, list[SignalCandidate], list[SignalCandidate]]:
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
            if allow_trading:
                await open_autotrade_for_signal(bot, candidate)
    return scan, sent_candidates, skipped_by_cooldown


async def auto_signal_worker(bot: Bot) -> None:
    if not AUTO_SIGNALS_ENABLED:
        logging.info("AUTO_SIGNALS_ENABLED=false, авто-сканер выключен")
        return
    logging.info("Авто-сканер включён: provider=%s symbols_mode=%s timeframe=%s interval=%ss threshold=%s%%", MARKET_DATA_PROVIDER, symbols_mode_text(), SIGNAL_TIMEFRAME, SCAN_INTERVAL_SECONDS, MIN_SIGNAL_PROBABILITY)
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


async def scan_single_symbol(symbol: str) -> tuple[Optional[SignalCandidate], bool, int]:
    """Скан одной монеты для ручного ввода.

    Возвращает: candidate, has_data, candles_count.
    Таймаут нужен, чтобы бот не зависал на сообщении «Сканирую...»,
    если API биржи медленно отвечает или подвисает.
    """
    normalized = normalize_user_symbol(symbol)
    if not normalized:
        return None, False, 0
    timeout = aiohttp.ClientTimeout(total=25)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        candles = await asyncio.wait_for(fetch_klines(session, normalized, SIGNAL_TIMEFRAME, KLINES_LIMIT), timeout=30)
    if not candles:
        return None, False, 0
    return apply_smart_algorithm(analyze_candles(normalized, candles)), True, len(candles)


async def safe_edit(message_to_edit: Message, text: str) -> None:
    try:
        await message_to_edit.edit_text(text)
    except Exception:
        logging.exception("Не удалось отредактировать progress-сообщение")


async def answer_single_symbol_scan(message: Message, symbol_text: str) -> None:
    normalized = normalize_user_symbol(symbol_text)
    if not normalized:
        await message.answer("Не понял монету. Напиши, например: <code>BTC</code>, <code>XMR</code> или <code>BTCUSDT</code>.")
        return

    progress = await message.answer(f"🔎 Сканирую <b>{html.escape(display_symbol(normalized))}</b> на {html.escape(exchange_label())}...")
    try:
        candidate, has_data, candles_count = await scan_single_symbol(normalized)
    except asyncio.TimeoutError:
        logging.exception("Таймаут ручного скана одной монеты")
        await safe_edit(progress, f"⏳ Биржа долго не отвечает по <b>{html.escape(display_symbol(normalized))}</b>. Попробуй ещё раз или переключи биржу в /settings.")
        return
    except Exception as exc:
        logging.exception("Ошибка скана одной монеты")
        await safe_edit(progress, "⚠️ Ошибка ручного скана. Подробности смотри в Railway Logs.")
        await message.answer(f"Монета: <b>{html.escape(display_symbol(normalized))}</b>\nОшибка: <code>{html.escape(str(exc))}</code>")
        return

    if not has_data:
        await safe_edit(progress, "✅ Ручной скан завершён")
        await message.answer(
            f"Не получил данные по <b>{html.escape(display_symbol(normalized))}</b> на <b>{html.escape(exchange_label())}</b>.\n\n"
            "Проверь, есть ли эта пара на выбранной бирже, или переключи биржу в /settings."
        )
        return

    if not candidate:
        await safe_edit(progress, "✅ Ручной скан завершён")
        await message.answer(
            f"🔎 <b>{html.escape(display_symbol(normalized))}</b> просканирована.\n\n"
            f"Биржа: <b>{html.escape(exchange_label())}</b>\n"
            f"Таймфрейм: <b>{html.escape(SIGNAL_TIMEFRAME)}</b>\n"
            f"Свечей получено: <b>{candles_count}</b>\n\n"
            "Сильного LONG/SHORT сетапа по текущей логике нет. Лучше подождать."
        )
        return

    reasons_text = "\n".join(f"• {reason}" for reason in candidate.reasons) if candidate.reasons else "• технический скоринг"
    text = structured_signal_text(
        symbol=display_symbol(candidate.symbol),
        side=candidate.side,
        probability=candidate.probability,
        entry=candidate.entry,
        stop=candidate.stop,
        take_profits=candidate.take_profits,
        comment=reasons_text,
        timeframe=candidate.timeframe,
        auto=False,
    )
    if candidate.probability < MIN_SIGNAL_PROBABILITY:
        text += f"\n\nℹ️ Ниже порога автоотправки: {candidate.probability}% < {MIN_SIGNAL_PROBABILITY}%."

    await safe_edit(progress, "✅ Ручной скан завершён")
    await message.answer(text)


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
        "Команды: /help, /status, /settings, /scan, /id, /stop",
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
            "• /settings — кнопки настроек авто-сканера и автоторговли\n"
            "• /smart — статус умного алгоритма\n"
            "• /api — API ключи для LIVE-торговли\n"
            "• /margin 10 — маржа/объём на сделку в USDT\n"
            "• /trades — активные авто-сделки\n"
            "• отправь BTC, XMR или BTCUSDT — скан одной монеты\n"
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
        "• /settings — настройки бота через кнопки\n"
        "• /smart — статистика умного алгоритма\n"
        "• /api — API ключи для автоторговли\n"
        "• /trades — активные авто-сделки\n"
        "• отправь название монеты, например BTC или XMR, — я просканирую её отдельно\n"
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
        f"Источник данных: <b>{html.escape(exchange_label())}</b>\n"
        f"Умный алгоритм: <b>{html.escape(smart_algorithm_label())}</b>\n"
        f"Smart-история: <b>{html.escape(smart_learning_stats_text())}</b>\n"
        f"Автоторговля: <b>{html.escape(autotrade_label())}</b>\n"
        f"API текущей биржи: <b>{'есть' if has_api_keys(MARKET_DATA_PROVIDER) else 'нет'}</b>\n"
        f"Маржа/объём сделки: <b>${TRADE_MARGIN_USDT:g}</b>\n"
        f"Авто-закрытие: <b>SL или TP{AUTO_CLOSE_TP_INDEX}</b>\n"
        f"Открытых авто-сделок: <b>{len(get_open_trades())}</b> / {MAX_ACTIVE_TRADES}\n"
        f"Отчёты админу: <b>{'включены' if AUTO_SCAN_REPORTS_TO_ADMINS else 'выключены'}</b>\n"
        f"Режим монет: <b>{html.escape(symbols_mode_text())}</b>\n"
        f"Фолбэк-монет: <b>{len(SYMBOLS)}</b>\n"
        f"Фолбэк-список: <code>{html.escape(','.join(SYMBOLS[:40]))}{'...' if len(SYMBOLS) > 40 else ''}</code>\n"
        f"Подписчиков: <b>{len(subscribers)}</b>\n"
        f"Доп. чаты из SIGNAL_CHAT_IDS: <b>{len(SIGNAL_CHAT_IDS)}</b>",
        reply_markup=keyboard,
    )


@dp.message(Command("settings"))
async def cmd_settings(message: Message) -> None:
    if not is_admin(message.from_user.id):
        await message.answer("Настройки доступны только админу.")
        return
    await message.answer(settings_menu_text(), reply_markup=settings_keyboard())


@dp.message(Command("smart"))
async def cmd_smart(message: Message) -> None:
    if not is_admin(message.from_user.id):
        await message.answer("Настройка доступна только админу.")
        return
    await message.answer(
        "<b>🧠 Умный алгоритм</b>\n\n"
        f"Статус: <b>{html.escape(smart_algorithm_label())}</b>\n"
        f"История: <b>{html.escape(smart_learning_stats_text())}</b>\n"
        f"Lookback: <b>{SMART_LOOKBACK_TRADES}</b> закрытых сделок\n"
        f"Старт обучения: <b>от {SMART_MIN_HISTORY_TRADES}</b> закрытых сделок\n"
        f"Триггер серии минусов: <b>{SMART_LOSS_STREAK_TRIGGER}</b> подряд\n\n"
        "Включается/выключается в /settings → 🧠 Умный алгоритм.",
        reply_markup=smart_algorithm_keyboard(),
    )


@dp.message(Command("api"))
async def cmd_api(message: Message) -> None:
    if not is_admin(message.from_user.id):
        await message.answer("API настройки доступны только админу.")
        return
    await message.answer(api_status_text(), reply_markup=api_keyboard())


@dp.message(Command("api_set"))
async def cmd_api_set(message: Message, command: CommandObject) -> None:
    if not is_admin(message.from_user.id):
        await message.answer("API настройки доступны только админу.")
        return
    if not command.args:
        await message.answer(
            "Формат:\n"
            "<code>/api_set MEXC API_KEY API_SECRET</code>\n"
            "<code>/api_set BINGX API_KEY API_SECRET</code>\n\n"
            "Нужны права Read + Trade. Withdraw/вывод средств не включай."
        )
        return

    parts = command.args.split()
    if len(parts) < 3:
        await message.answer("Недостаточно данных. Нужно: биржа, API key, API secret.")
        return
    exchange = parts[0].strip().lower()
    if exchange not in EXCHANGE_OPTIONS:
        await message.answer("Биржа должна быть MEXC или BINGX.")
        return
    api_key = parts[1].strip()
    api_secret = parts[2].strip()
    if len(api_key) < 6 or len(api_secret) < 6:
        await message.answer("Ключи выглядят слишком короткими. Проверь API key и API secret.")
        return

    keys = load_api_keys()
    keys[exchange] = {"api_key": api_key, "api_secret": api_secret}
    save_api_keys(keys)

    try:
        await message.delete()
    except Exception:
        pass

    await message.answer(
        f"✅ API ключи для <b>{html.escape(exchange_label(exchange))}</b> сохранены.\n"
        "Для запуска реальных ордеров включи режим LIVE в /settings → Автоторговля.\n\n"
        "Важно: ключ должен быть без права вывода средств."
    )


@dp.message(Command("api_clear"))
async def cmd_api_clear(message: Message, command: CommandObject) -> None:
    if not is_admin(message.from_user.id):
        await message.answer("API настройки доступны только админу.")
        return
    exchange = (command.args or MARKET_DATA_PROVIDER).strip().lower()
    if exchange == "all":
        save_api_keys({})
        await message.answer("🧹 Все API ключи очищены.")
        return
    if exchange not in EXCHANGE_OPTIONS:
        await message.answer("Формат: <code>/api_clear MEXC</code>, <code>/api_clear BINGX</code> или <code>/api_clear all</code>.")
        return
    keys = load_api_keys()
    keys.pop(exchange, None)
    save_api_keys(keys)
    await message.answer(f"🧹 API ключи для {html.escape(exchange_label(exchange))} очищены.")


@dp.message(Command("margin"))
async def cmd_margin(message: Message, command: CommandObject) -> None:
    global TRADE_MARGIN_USDT
    if not is_admin(message.from_user.id):
        await message.answer("Настройка доступна только админу.")
        return
    if not command.args:
        await message.answer(f"Текущая маржа/объём на сделку: <b>${TRADE_MARGIN_USDT:g}</b>\nПример: <code>/margin 10</code>")
        return
    try:
        value = float(command.args.strip().replace(",", "."))
    except ValueError:
        await message.answer("Сумма должна быть числом. Пример: <code>/margin 10</code>")
        return
    TRADE_MARGIN_USDT = max(1.0, min(10000.0, value))
    save_runtime_settings()
    await message.answer(f"✅ Маржа/объём на сделку установлен: <b>${TRADE_MARGIN_USDT:g}</b>")


@dp.message(Command("trades"))
async def cmd_trades(message: Message) -> None:
    if not is_admin(message.from_user.id):
        await message.answer("Список сделок доступен только админу.")
        return
    await message.answer(trades_status_text())


@dp.callback_query(F.data.startswith("settings:"))
async def settings_callback(callback: CallbackQuery) -> None:
    global SIGNAL_TIMEFRAME, MIN_SIGNAL_PROBABILITY, SCAN_INTERVAL_SECONDS, MARKET_DATA_PROVIDER
    global AUTO_TRADE_MODE, TRADE_MARGIN_USDT, AUTO_CLOSE_TP_INDEX, SMART_ALGORITHM_ENABLED

    if callback.from_user is None or not is_admin(callback.from_user.id):
        await callback.answer("Только админ может менять настройки", show_alert=True)
        return

    data = callback.data or ""
    message = callback.message
    if message is None:
        await callback.answer()
        return

    if data == "settings:menu":
        await message.edit_text(settings_menu_text(), reply_markup=settings_keyboard())
        await callback.answer()
        return

    if data == "settings:exchange":
        await message.edit_text(
            f"<b>🏦 Выбери биржу</b>\n\nСейчас: <b>{html.escape(exchange_label())}</b>",
            reply_markup=exchange_keyboard(),
        )
        await callback.answer()
        return

    if data == "settings:timeframe":
        await message.edit_text(
            f"<b>⏱ Выбери таймфрейм</b>\n\nСейчас: <b>{html.escape(SIGNAL_TIMEFRAME)}</b>",
            reply_markup=timeframe_keyboard(),
        )
        await callback.answer()
        return

    if data == "settings:probability":
        await message.edit_text(
            f"<b>🎯 Выбери минимальную проходимость</b>\n\nСейчас: <b>{MIN_SIGNAL_PROBABILITY}%</b>",
            reply_markup=probability_keyboard(),
        )
        await callback.answer()
        return

    if data == "settings:interval":
        await message.edit_text(
            f"<b>🔁 Выбери интервал скана</b>\n\nСейчас: <b>{human_interval(SCAN_INTERVAL_SECONDS)}</b>",
            reply_markup=interval_keyboard(),
        )
        await callback.answer()
        return

    if data == "settings:smart":
        await message.edit_text(
            "<b>🧠 Умный алгоритм</b>\n\n"
            f"Сейчас: <b>{html.escape(smart_algorithm_label())}</b>\n"
            f"История: <b>{html.escape(smart_learning_stats_text())}</b>\n\n"
            "Когда включено, бот смотрит историю закрытых авто-сделок: после серии минусов становится строже, "
            "штрафует убыточные пары/стороны и может отсеивать слабые сетапы. Это не гарантия прибыли.",
            reply_markup=smart_algorithm_keyboard(),
        )
        await callback.answer()
        return

    if data == "settings:autotrade":
        await message.edit_text(
            "<b>💰 Автоторговля</b>\n\n"
            f"Режим: <b>{html.escape(autotrade_label())}</b>\n"
            f"Маржа/объём сделки: <b>${TRADE_MARGIN_USDT:g}</b>\n"
            f"Авто-закрытие: <b>SL или TP{AUTO_CLOSE_TP_INDEX}</b>\n"
            f"Открытых сделок: <b>{len(get_open_trades())}</b>\n\n"
            "OFF — только сигналы. PAPER — тест без ордеров. LIVE — реальные рыночные ордера по API.",
            reply_markup=autotrade_keyboard(),
        )
        await callback.answer()
        return

    if data == "settings:trade_margin":
        await message.edit_text(
            f"<b>💵 Выбери маржу/объём на сделку</b>\n\nСейчас: <b>${TRADE_MARGIN_USDT:g}</b>\n\n"
            "Бот не будет открывать позицию больше этой суммы в USDT по своей логике.",
            reply_markup=trade_margin_keyboard(),
        )
        await callback.answer()
        return

    if data == "settings:trade_margin_custom":
        await message.edit_text(
            "✍️ Чтобы задать свою сумму, отправь команду:\n"
            "<code>/margin 15</code>\n\n"
            "Пример выше установит $15 на сделку.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data="settings:autotrade")]]),
        )
        await callback.answer()
        return

    if data == "settings:close_tp":
        await message.edit_text(
            f"<b>🎯 Выбери тейк для авто-закрытия</b>\n\nСейчас: <b>TP{AUTO_CLOSE_TP_INDEX}</b>\n\n"
            "Если цена дойдёт до выбранного TP или до SL, бот закроет всю авто-сделку.",
            reply_markup=close_tp_keyboard(),
        )
        await callback.answer()
        return

    if data == "settings:trades":
        await message.edit_text(
            trades_status_text(),
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data="settings:autotrade")]]),
        )
        await callback.answer()
        return

    if data == "settings:api":
        await message.edit_text(api_status_text(), reply_markup=api_keyboard())
        await callback.answer()
        return

    if data == "settings:api_help":
        await message.edit_text(api_status_text(), reply_markup=api_keyboard())
        await callback.answer()
        return

    if data == "settings:api_clear_current":
        keys = load_api_keys()
        keys.pop(MARKET_DATA_PROVIDER, None)
        save_api_keys(keys)
        await message.edit_text(api_status_text(), reply_markup=api_keyboard())
        await callback.answer("Ключи текущей биржи очищены")
        return

    if data.startswith("settings:set_exchange:"):
        value = data.split(":", 2)[2].lower()
        if value in EXCHANGE_OPTIONS:
            MARKET_DATA_PROVIDER = value
            save_runtime_settings()
            await message.edit_text(settings_menu_text(), reply_markup=settings_keyboard())
            await callback.answer(f"Биржа: {exchange_label(value)}")
        else:
            await callback.answer("Неверная биржа", show_alert=True)
        return

    if data.startswith("settings:set_timeframe:"):
        value = data.split(":", 2)[2]
        if value in TIMEFRAME_OPTIONS:
            SIGNAL_TIMEFRAME = value
            save_runtime_settings()
            await message.edit_text(settings_menu_text(), reply_markup=settings_keyboard())
            await callback.answer(f"Таймфрейм: {value}")
        else:
            await callback.answer("Неверный таймфрейм", show_alert=True)
        return

    if data.startswith("settings:set_probability:"):
        try:
            value = int(data.split(":", 2)[2])
        except ValueError:
            await callback.answer("Неверное значение", show_alert=True)
            return
        if value in PROBABILITY_OPTIONS:
            MIN_SIGNAL_PROBABILITY = value
            save_runtime_settings()
            await message.edit_text(settings_menu_text(), reply_markup=settings_keyboard())
            await callback.answer(f"Проходимость: {value}%")
        else:
            await callback.answer("Неверная проходимость", show_alert=True)
        return

    if data.startswith("settings:set_interval:"):
        try:
            value = int(data.split(":", 2)[2])
        except ValueError:
            await callback.answer("Неверное значение", show_alert=True)
            return
        if value in SCAN_INTERVAL_OPTIONS:
            SCAN_INTERVAL_SECONDS = value
            save_runtime_settings()
            await message.edit_text(settings_menu_text(), reply_markup=settings_keyboard())
            await callback.answer(f"Интервал: {human_interval(value)}")
        else:
            await callback.answer("Неверный интервал", show_alert=True)
        return

    if data.startswith("settings:set_smart:"):
        value = data.split(":", 2)[2].lower()
        if value in {"on", "off"}:
            SMART_ALGORITHM_ENABLED = value == "on"
            save_runtime_settings()
            await message.edit_text(settings_menu_text(), reply_markup=settings_keyboard())
            await callback.answer("Умный алгоритм включён" if SMART_ALGORITHM_ENABLED else "Умный алгоритм выключен")
        else:
            await callback.answer("Неверное значение", show_alert=True)
        return

    if data.startswith("settings:set_autotrade_mode:"):
        value = data.split(":", 2)[2].lower()
        if value in AUTO_TRADE_MODE_OPTIONS:
            if value == "live" and not has_api_keys(MARKET_DATA_PROVIDER):
                await callback.answer("Сначала добавь API ключи для выбранной биржи", show_alert=True)
                return
            AUTO_TRADE_MODE = value
            save_runtime_settings()
            await message.edit_text(settings_menu_text(), reply_markup=settings_keyboard())
            await callback.answer(f"Автоторговля: {autotrade_label()}")
        else:
            await callback.answer("Неверный режим", show_alert=True)
        return

    if data.startswith("settings:set_trade_margin:"):
        try:
            value = float(data.split(":", 2)[2])
        except ValueError:
            await callback.answer("Неверная сумма", show_alert=True)
            return
        TRADE_MARGIN_USDT = max(1.0, min(10000.0, value))
        save_runtime_settings()
        await message.edit_text(settings_menu_text(), reply_markup=settings_keyboard())
        await callback.answer(f"Маржа/объём: ${TRADE_MARGIN_USDT:g}")
        return

    if data.startswith("settings:set_close_tp:"):
        try:
            value = int(data.split(":", 2)[2])
        except ValueError:
            await callback.answer("Неверный TP", show_alert=True)
            return
        if value in AUTO_CLOSE_TP_OPTIONS:
            AUTO_CLOSE_TP_INDEX = value
            save_runtime_settings()
            await message.edit_text(settings_menu_text(), reply_markup=settings_keyboard())
            await callback.answer(f"Закрытие по TP{value}")
        else:
            await callback.answer("Неверный TP", show_alert=True)
        return

    if data == "settings:close":
        await message.edit_text("Настройки закрыты.")
        await callback.answer()
        return

    await callback.answer()


@dp.message(Command("scan"))
async def cmd_scan(message: Message, bot: Bot) -> None:
    if not is_admin(message.from_user.id):
        await message.answer("Эта команда доступна только админу.")
        return
    progress = await message.answer("🧪 Запускаю ручной авто-скан рынка...")
    try:
        scan, sent_candidates, skipped = await run_auto_scan_once(bot, ignore_cooldown=True, allow_trading=False)
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


@dp.message(F.text == "⚙️ Настройки")
async def button_settings(message: Message) -> None:
    await cmd_settings(message)


@dp.message(F.text == "🧠 Умный алгоритм")
async def button_smart(message: Message) -> None:
    await cmd_smart(message)


@dp.message(F.text == "💰 Автоторговля")
async def button_autotrade(message: Message) -> None:
    if not is_admin(message.from_user.id):
        await message.answer("Настройки автоторговли доступны только админу.")
        return
    await message.answer(
        "<b>💰 Автоторговля</b>\n\n"
        f"Режим: <b>{html.escape(autotrade_label())}</b>\n"
        f"Маржа/объём сделки: <b>${TRADE_MARGIN_USDT:g}</b>\n"
        f"Авто-закрытие: <b>SL или TP{AUTO_CLOSE_TP_INDEX}</b>",
        reply_markup=autotrade_keyboard(),
    )


@dp.message(F.text == "🔑 API")
async def button_api(message: Message) -> None:
    await cmd_api(message)


@dp.message(F.text == "🔕 Отписаться")
async def button_stop(message: Message) -> None:
    await cmd_stop(message)


@dp.message()
async def fallback(message: Message) -> None:
    text = (message.text or "").strip()
    if text and is_symbol_query(text):
        await answer_single_symbol_scan(message, text)
        return
    await message.answer("Я понял сообщение, но команды такой нет. Чтобы просканировать монету, напиши, например: BTC или XMR. Для команд нажми /help.", reply_markup=keyboard)


async def main() -> None:
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN не найден. Добавь переменную BOT_TOKEN в Railway")
    logging.basicConfig(level=logging.INFO)
    bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    await bot.delete_webhook(drop_pending_updates=True)
    worker_task = asyncio.create_task(auto_signal_worker(bot))
    trade_monitor_task = asyncio.create_task(trade_monitor_worker(bot))
    try:
        await dp.start_polling(bot)
    finally:
        worker_task.cancel()
        trade_monitor_task.cancel()
        for task in (worker_task, trade_monitor_task):
            try:
                await task
            except asyncio.CancelledError:
                pass
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
