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

# ---- Фильтр основного тренда ----
# Когда включено, бот сначала находит сетап на рабочем таймфрейме,
# затем проверяет старший таймфрейм и пропускает только LONG по бычьему тренду
# или SHORT по медвежьему тренду. Фильтр применяется и к сигналам, и к автоторговле.
TREND_FILTER_ENABLED = os.getenv("TREND_FILTER_ENABLED", "true").strip().lower() in {"1", "true", "yes", "on"}
TREND_TIMEFRAME = os.getenv("TREND_TIMEFRAME", "4h").strip()
TREND_MIN_SCORE = max(2, min(6, int(os.getenv("TREND_MIN_SCORE", "3"))))
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

# LIVE safety settings. Protective orders are created on the exchange where CCXT supports them;
# the bot still keeps a fallback monitor as a second line of defence.
USE_EXCHANGE_PROTECTIVE_ORDERS = os.getenv("USE_EXCHANGE_PROTECTIVE_ORDERS", "true").strip().lower() in {"1", "true", "yes", "on"}
CANCEL_PROTECTIVE_ORDERS_ON_CLOSE = os.getenv("CANCEL_PROTECTIVE_ORDERS_ON_CLOSE", "true").strip().lower() in {"1", "true", "yes", "on"}
SYNC_POSITIONS_ON_START = os.getenv("SYNC_POSITIONS_ON_START", "true").strip().lower() in {"1", "true", "yes", "on"}
SYNC_POSITIONS_INTERVAL_SECONDS = max(30, int(os.getenv("SYNC_POSITIONS_INTERVAL_SECONDS", "120")))
ALLOW_API_KEYS_FILE = os.getenv("ALLOW_API_KEYS_FILE", "true").strip().lower() in {"1", "true", "yes", "on"}

# ---- Умный алгоритм ----
# OFF по умолчанию. Включается в /settings кнопкой "🧠 Умный алгоритм".
# Это не ИИ-прогноз и не гарантия прибыли: бот анализирует историю закрытых авто-сделок,
# штрафует убыточные связки символ/сторона и становится строже после серии минусов.
SMART_ALGORITHM_ENABLED = os.getenv("SMART_ALGORITHM_ENABLED", "false").strip().lower() in {"1", "true", "yes", "on"}
SMART_LOOKBACK_TRADES = max(5, min(200, int(os.getenv("SMART_LOOKBACK_TRADES", "30"))))
SMART_MIN_HISTORY_TRADES = max(3, min(50, int(os.getenv("SMART_MIN_HISTORY_TRADES", "5"))))
SMART_LOSS_STREAK_TRIGGER = max(2, min(10, int(os.getenv("SMART_LOSS_STREAK_TRIGGER", "3"))))
SMART_ADJUSTMENT_CAP = max(3, min(30, int(os.getenv("SMART_ADJUSTMENT_CAP", "15"))))

# ---- Нейро-оптимизатор алгоритмов ----
# Это не гарантия прибыли. Модуль перебирает несколько алгоритмических профилей
# на свежей истории свечей, выбирает лучший по winrate/profit factor/avg PnL
# и пропускает сигнал/сделку только если выбранный профиль показывает положительное преимущество.
NEURAL_OPTIMIZER_ENABLED = os.getenv(
    "NEURAL_OPTIMIZER_ENABLED",
    os.getenv("AI_OPTIMIZER_ENABLED", "false"),
).strip().lower() in {"1", "true", "yes", "on"}
NEURAL_OPTIMIZER_STRICT_MODE = os.getenv("NEURAL_OPTIMIZER_STRICT_MODE", "true").strip().lower() in {"1", "true", "yes", "on"}
NEURAL_OPTIMIZER_MIN_TRADES = max(3, min(50, int(os.getenv("NEURAL_OPTIMIZER_MIN_TRADES", "6"))))
NEURAL_OPTIMIZER_MIN_WIN_RATE = max(0.0, min(1.0, float(os.getenv("NEURAL_OPTIMIZER_MIN_WIN_RATE", "0.55"))))
NEURAL_OPTIMIZER_MIN_PROFIT_FACTOR = max(0.1, min(10.0, float(os.getenv("NEURAL_OPTIMIZER_MIN_PROFIT_FACTOR", "1.15"))))
NEURAL_OPTIMIZER_MIN_AVG_PNL = float(os.getenv("NEURAL_OPTIMIZER_MIN_AVG_PNL", "0.05"))
NEURAL_OPTIMIZER_HORIZON_CANDLES = max(3, min(80, int(os.getenv("NEURAL_OPTIMIZER_HORIZON_CANDLES", "24"))))
NEURAL_OPTIMIZER_PROBABILITY_BONUS = max(0, min(15, int(os.getenv("NEURAL_OPTIMIZER_PROBABILITY_BONUS", "5"))))
NEURAL_OPTIMIZER_MAX_PROFILES = max(3, min(20, int(os.getenv("NEURAL_OPTIMIZER_MAX_PROFILES", "10"))))

# ---- Супер сделка ----
# Когда включено, бот отправляет сигналы и открывает авто-сделки только при максимальных условиях:
# базовый сигнал почти максимальный, тренд строго +7 для LONG или -7 для SHORT, итоговая проходимость 97-99%.
# В Telegram нельзя реально покрасить текст сообщения в красный цвет, поэтому супер-сигналы выделяются красными эмодзи 🔴.
SUPER_DEAL_ENABLED = os.getenv("SUPER_DEAL_ENABLED", "false").strip().lower() in {"1", "true", "yes", "on"}
SUPER_DEAL_MIN_PROBABILITY = max(97, min(99, int(os.getenv("SUPER_DEAL_MIN_PROBABILITY", "97"))))
SUPER_DEAL_RAW_PROBABILITY_MIN = max(90, min(95, int(os.getenv("SUPER_DEAL_RAW_PROBABILITY_MIN", "95"))))
SUPER_DEAL_TREND_SCORE_ABS = max(3, min(7, int(os.getenv("SUPER_DEAL_TREND_SCORE_ABS", "7"))))

# ---- Улучшения торговли ----
# Master-переключатель. OFF = бот работает как раньше. ON = включаются дополнительные
# защитные фильтры, риск-движок, частичные тейки, breakeven, лимиты убытков, panic и расширенная статистика.
TRADING_IMPROVEMENTS_ENABLED = os.getenv("TRADING_IMPROVEMENTS_ENABLED", "false").strip().lower() in {"1", "true", "yes", "on"}
ACCOUNT_EQUITY_USDT = max(1.0, float(os.getenv("ACCOUNT_EQUITY_USDT", "100")))
RISK_PER_TRADE_PERCENT = max(0.05, min(10.0, float(os.getenv("RISK_PER_TRADE_PERCENT", "0.5"))))
MAX_POSITION_NOTIONAL_USDT = max(1.0, float(os.getenv("MAX_POSITION_NOTIONAL_USDT", str(TRADE_MARGIN_USDT))))
MAX_DAILY_LOSS_PERCENT = max(0.1, min(50.0, float(os.getenv("MAX_DAILY_LOSS_PERCENT", "2"))))
MAX_WEEKLY_LOSS_PERCENT = max(0.1, min(80.0, float(os.getenv("MAX_WEEKLY_LOSS_PERCENT", "5"))))
MAX_CONSECUTIVE_LOSSES = max(1, min(20, int(os.getenv("MAX_CONSECUTIVE_LOSSES", "3"))))
PAUSE_AFTER_LOSS_STREAK_HOURS = max(1, min(168, int(os.getenv("PAUSE_AFTER_LOSS_STREAK_HOURS", "12"))))
STRICT_PROTECTION_CHECK_ENABLED = os.getenv("STRICT_PROTECTION_CHECK_ENABLED", "true").strip().lower() in {"1", "true", "yes", "on"}
PARTIAL_TP_ENABLED = os.getenv("PARTIAL_TP_ENABLED", "true").strip().lower() in {"1", "true", "yes", "on"}
TP1_CLOSE_PERCENT = max(1, min(98, int(os.getenv("TP1_CLOSE_PERCENT", "40"))))
TP2_CLOSE_PERCENT = max(1, min(98, int(os.getenv("TP2_CLOSE_PERCENT", "30"))))
TP3_CLOSE_PERCENT = max(1, min(100, int(os.getenv("TP3_CLOSE_PERCENT", "30"))))
MOVE_SL_TO_BREAKEVEN_AFTER_TP1 = os.getenv("MOVE_SL_TO_BREAKEVEN_AFTER_TP1", "true").strip().lower() in {"1", "true", "yes", "on"}
BREAKEVEN_OFFSET_PCT = max(0.0, min(2.0, float(os.getenv("BREAKEVEN_OFFSET_PCT", "0.03"))))
LIQUIDITY_FILTER_ENABLED = os.getenv("LIQUIDITY_FILTER_ENABLED", "true").strip().lower() in {"1", "true", "yes", "on"}
MAX_SPREAD_PCT = max(0.01, min(5.0, float(os.getenv("MAX_SPREAD_PCT", "0.15"))))
MIN_LAST_CANDLE_VOLUME_USDT = max(0.0, float(os.getenv("MIN_LAST_CANDLE_VOLUME_USDT", "10000")))
MIN_AVG_CANDLE_VOLUME_USDT = max(0.0, float(os.getenv("MIN_AVG_CANDLE_VOLUME_USDT", "8000")))
MIN_24H_QUOTE_VOLUME_USDT = max(0.0, float(os.getenv("MIN_24H_QUOTE_VOLUME_USDT", "500000")))
MIN_ATR_PCT = max(0.0, float(os.getenv("MIN_ATR_PCT", "0.08")))
MAX_ATR_PCT = max(MIN_ATR_PCT + 0.01, float(os.getenv("MAX_ATR_PCT", "8")))
COIN_RATING_FILTER_ENABLED = os.getenv("COIN_RATING_FILTER_ENABLED", "true").strip().lower() in {"1", "true", "yes", "on"}
SYMBOL_RATING_MIN_TRADES = max(2, min(100, int(os.getenv("SYMBOL_RATING_MIN_TRADES", "5"))))
MIN_SYMBOL_WIN_RATE = max(0.0, min(1.0, float(os.getenv("MIN_SYMBOL_WIN_RATE", "0.55"))))
MIN_SYMBOL_PROFIT_FACTOR = max(0.1, min(10.0, float(os.getenv("MIN_SYMBOL_PROFIT_FACTOR", "1.1"))))
CORRELATION_FILTER_ENABLED = os.getenv("CORRELATION_FILTER_ENABLED", "true").strip().lower() in {"1", "true", "yes", "on"}
MAX_SAME_DIRECTION_TRADES = max(1, min(10, int(os.getenv("MAX_SAME_DIRECTION_TRADES", "1"))))
MAX_ALT_TRADES = max(1, min(20, int(os.getenv("MAX_ALT_TRADES", "2"))))
MARKET_REGIME_FILTER_ENABLED = os.getenv("MARKET_REGIME_FILTER_ENABLED", "true").strip().lower() in {"1", "true", "yes", "on"}
MAX_PUMP_CANDLE_PCT = max(0.5, min(50.0, float(os.getenv("MAX_PUMP_CANDLE_PCT", "6"))))
HIGH_VOL_POSITION_FACTOR = max(0.1, min(1.0, float(os.getenv("HIGH_VOL_POSITION_FACTOR", "0.5"))))
WALK_FORWARD_OPTIMIZER_ENABLED = os.getenv("WALK_FORWARD_OPTIMIZER_ENABLED", "true").strip().lower() in {"1", "true", "yes", "on"}
WALK_FORWARD_TRAIN_RATIO = max(0.5, min(0.85, float(os.getenv("WALK_FORWARD_TRAIN_RATIO", "0.7"))))
WALK_FORWARD_MIN_TEST_TRADES = max(1, min(30, int(os.getenv("WALK_FORWARD_MIN_TEST_TRADES", "2"))))

MAX_ACTIVE_TRADES = int(os.getenv("MAX_ACTIVE_TRADES", "1"))
MAX_ACTIVE_TRADES = max(1, min(20, MAX_ACTIVE_TRADES))
TRADE_MONITOR_INTERVAL_SECONDS = int(os.getenv("TRADE_MONITOR_INTERVAL_SECONDS", "20"))

DATA_DIR = Path(
    os.getenv("DATA_DIR")
    or os.getenv("RAILWAY_VOLUME_MOUNT_PATH")
    or (Path(__file__).parent / "data")
)
DATA_DIR.mkdir(parents=True, exist_ok=True)
SUBSCRIBERS_FILE = DATA_DIR / "subscribers.json"
SENT_SIGNALS_FILE = DATA_DIR / "sent_signals.json"
SETTINGS_FILE = DATA_DIR / "settings.json"
API_KEYS_FILE = DATA_DIR / "api_keys.json"
TRADES_FILE = DATA_DIR / "trades.json"
NEURAL_OPTIMIZER_FILE = DATA_DIR / "neural_optimizer.json"
IMPROVEMENTS_STATS_FILE = DATA_DIR / "trading_improvements_stats.json"
TRADES_LOCK = asyncio.Lock()

TIMEFRAME_OPTIONS = ["5m", "15m", "30m", "1h", "4h"]
TREND_TIMEFRAME_OPTIONS = ["15m", "30m", "1h", "4h", "8h", "1d"]
if TREND_TIMEFRAME not in TREND_TIMEFRAME_OPTIONS:
    TREND_TIMEFRAME = "4h"
PROBABILITY_OPTIONS = [60, 70, 75, 80, 85, 90, 95]
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
        "TREND_FILTER_ENABLED": TREND_FILTER_ENABLED,
        "TREND_TIMEFRAME": TREND_TIMEFRAME,
        "SCAN_INTERVAL_SECONDS": SCAN_INTERVAL_SECONDS,
        "MARKET_DATA_PROVIDER": MARKET_DATA_PROVIDER,
        "AUTO_TRADE_MODE": AUTO_TRADE_MODE,
        "TRADE_MARGIN_USDT": TRADE_MARGIN_USDT,
        "AUTO_CLOSE_TP_INDEX": AUTO_CLOSE_TP_INDEX,
        "SMART_ALGORITHM_ENABLED": SMART_ALGORITHM_ENABLED,
        "NEURAL_OPTIMIZER_ENABLED": NEURAL_OPTIMIZER_ENABLED,
        "SUPER_DEAL_ENABLED": SUPER_DEAL_ENABLED,
        "TRADING_IMPROVEMENTS_ENABLED": TRADING_IMPROVEMENTS_ENABLED,
    })


def apply_runtime_settings(settings: dict[str, Any]) -> None:
    global MIN_SIGNAL_PROBABILITY, SIGNAL_TIMEFRAME, SCAN_INTERVAL_SECONDS, MARKET_DATA_PROVIDER
    global AUTO_TRADE_MODE, TRADE_MARGIN_USDT, AUTO_CLOSE_TP_INDEX, SMART_ALGORITHM_ENABLED
    global NEURAL_OPTIMIZER_ENABLED, SUPER_DEAL_ENABLED, TRADING_IMPROVEMENTS_ENABLED
    global TREND_FILTER_ENABLED, TREND_TIMEFRAME
    try:
        probability = int(settings.get("MIN_SIGNAL_PROBABILITY", MIN_SIGNAL_PROBABILITY))
        MIN_SIGNAL_PROBABILITY = max(1, min(100, probability))
    except Exception:
        pass

    timeframe = str(settings.get("SIGNAL_TIMEFRAME", SIGNAL_TIMEFRAME)).strip()
    if timeframe in TIMEFRAME_OPTIONS:
        SIGNAL_TIMEFRAME = timeframe

    trend_enabled_raw = settings.get("TREND_FILTER_ENABLED", TREND_FILTER_ENABLED)
    if isinstance(trend_enabled_raw, bool):
        TREND_FILTER_ENABLED = trend_enabled_raw
    else:
        TREND_FILTER_ENABLED = str(trend_enabled_raw).strip().lower() in {"1", "true", "yes", "on"}

    trend_timeframe = str(settings.get("TREND_TIMEFRAME", TREND_TIMEFRAME)).strip()
    if trend_timeframe in TREND_TIMEFRAME_OPTIONS:
        TREND_TIMEFRAME = trend_timeframe

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

    neural_raw = settings.get("NEURAL_OPTIMIZER_ENABLED", NEURAL_OPTIMIZER_ENABLED)
    if isinstance(neural_raw, bool):
        NEURAL_OPTIMIZER_ENABLED = neural_raw
    else:
        NEURAL_OPTIMIZER_ENABLED = str(neural_raw).strip().lower() in {"1", "true", "yes", "on"}

    super_raw = settings.get("SUPER_DEAL_ENABLED", SUPER_DEAL_ENABLED)
    if isinstance(super_raw, bool):
        SUPER_DEAL_ENABLED = super_raw
    else:
        SUPER_DEAL_ENABLED = str(super_raw).strip().lower() in {"1", "true", "yes", "on"}

    improvements_raw = settings.get("TRADING_IMPROVEMENTS_ENABLED", TRADING_IMPROVEMENTS_ENABLED)
    if isinstance(improvements_raw, bool):
        TRADING_IMPROVEMENTS_ENABLED = improvements_raw
    else:
        TRADING_IMPROVEMENTS_ENABLED = str(improvements_raw).strip().lower() in {"1", "true", "yes", "on"}


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


def neural_optimizer_label() -> str:
    if NEURAL_OPTIMIZER_ENABLED:
        mode = "строгий" if NEURAL_OPTIMIZER_STRICT_MODE else "мягкий"
        return f"ON — перебор алгоритмов ({mode})"
    return "OFF — без нейро-оптимизации"


def trend_filter_label() -> str:
    if TREND_FILTER_ENABLED:
        return f"ON — только по тренду {TREND_TIMEFRAME}"
    return "OFF — сигналы без фильтра старшего ТФ"


def super_deal_label() -> str:
    if SUPER_DEAL_ENABLED:
        return (
            f"ON — только супер-сигналы {SUPER_DEAL_MIN_PROBABILITY}-99%, "
            f"trend score ±{SUPER_DEAL_TREND_SCORE_ABS}"
        )
    return "OFF — обычные сигналы по текущим фильтрам"


def trading_improvements_label() -> str:
    if TRADING_IMPROVEMENTS_ENABLED:
        return (
            f"ON — риск {RISK_PER_TRADE_PERCENT:g}%/сделка, лимит дня {MAX_DAILY_LOSS_PERCENT:g}%, "
            f"частичные TP + breakeven"
        )
    return "OFF — прежний режим без дополнительных улучшений"


def settings_menu_text() -> str:
    return (
        "<b>⚙️ Настройки авто-бота</b>\n\n"
        f"Биржа: <b>{html.escape(exchange_label())}</b>\n"
        f"Таймфрейм: <b>{html.escape(SIGNAL_TIMEFRAME)}</b>\n"
        f"Проходимость: <b>{MIN_SIGNAL_PROBABILITY}%</b>\n"
        f"Интервал скана: <b>{human_interval(SCAN_INTERVAL_SECONDS)}</b>\n"
        f"Умный алгоритм: <b>{html.escape(smart_algorithm_label())}</b>\n"
        f"Нейросети: <b>{html.escape(neural_optimizer_label())}</b>\n"
        f"AI-статус: <b>{html.escape(neural_optimizer_stats_text())}</b>\n"
        f"История smart: <b>{html.escape(smart_learning_stats_text())}</b>\n"
        f"Фильтр тренда: <b>{html.escape(trend_filter_label())}</b>\n"
        f"Супер сделка: <b>{html.escape(super_deal_label())}</b>\n"
        f"Улучшения торговли: <b>{html.escape(trading_improvements_label())}</b>\n"
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
        [InlineKeyboardButton(text="🤖 Нейросети", callback_data="settings:neural")],
        [InlineKeyboardButton(text="🧭 Фильтр тренда", callback_data="settings:trend")],
        [InlineKeyboardButton(text="🔴 Супер сделка", callback_data="settings:super_deal")],
        [InlineKeyboardButton(text="🚀 Улучшения торговли", callback_data="settings:improvements")],
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


def neural_optimizer_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(
                text=("✅ " if not NEURAL_OPTIMIZER_ENABLED else "") + "OFF",
                callback_data="settings:set_neural:off",
            ),
            InlineKeyboardButton(
                text=("✅ " if NEURAL_OPTIMIZER_ENABLED else "") + "ON",
                callback_data="settings:set_neural:on",
            ),
        ],
        [InlineKeyboardButton(text="📊 Лучший алгоритм", callback_data="settings:neural_stats")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="settings:menu")],
    ])


def trend_filter_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(
                text=("✅ " if not TREND_FILTER_ENABLED else "") + "OFF",
                callback_data="settings:set_trend:off",
            ),
            InlineKeyboardButton(
                text=("✅ " if TREND_FILTER_ENABLED else "") + "ON",
                callback_data="settings:set_trend:on",
            ),
        ],
        [InlineKeyboardButton(text=f"⏱ Старший ТФ: {TREND_TIMEFRAME}", callback_data="settings:trend_timeframe")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="settings:menu")],
    ])


def trend_timeframe_keyboard() -> InlineKeyboardMarkup:
    rows = []
    for i in range(0, len(TREND_TIMEFRAME_OPTIONS), 3):
        rows.append([
            InlineKeyboardButton(
                text=("✅ " if value == TREND_TIMEFRAME else "") + value,
                callback_data=f"settings:set_trend_timeframe:{value}",
            )
            for value in TREND_TIMEFRAME_OPTIONS[i:i + 3]
        ])
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="settings:trend")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def super_deal_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(
                text=("✅ " if not SUPER_DEAL_ENABLED else "") + "OFF",
                callback_data="settings:set_super_deal:off",
            ),
            InlineKeyboardButton(
                text=("✅ " if SUPER_DEAL_ENABLED else "") + "ON",
                callback_data="settings:set_super_deal:on",
            ),
        ],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="settings:menu")],
    ])


def trading_improvements_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(
                text=("✅ " if not TRADING_IMPROVEMENTS_ENABLED else "") + "OFF",
                callback_data="settings:set_improvements:off",
            ),
            InlineKeyboardButton(
                text=("✅ " if TRADING_IMPROVEMENTS_ENABLED else "") + "ON",
                callback_data="settings:set_improvements:on",
            ),
        ],
        [InlineKeyboardButton(text="📊 Статистика", callback_data="settings:improvements_stats")],
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
        [KeyboardButton(text="🤖 Нейросети"), KeyboardButton(text="🧭 Фильтр тренда")],
        [KeyboardButton(text="🔴 Супер сделка"), KeyboardButton(text="🚀 Улучшения торговли")],
        [KeyboardButton(text="💰 Автоторговля"), KeyboardButton(text="🔑 API")],
        [KeyboardButton(text="🔕 Отписаться")],
    ],
    resize_keyboard=True,
)


@dataclass
class TrendInfo:
    direction: str
    score: int
    confidence: int
    timeframe: str
    reasons: list[str]
    close: float = 0.0


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
    trend: Optional[TrendInfo] = None
    ai_optimizer: Optional[dict[str, Any]] = None
    is_super_deal: bool = False
    super_deal_score: int = 0


@dataclass
class NeuralBacktestResult:
    profile_id: str
    profile_name: str
    side: str
    trades: int
    win_rate: float
    profit_factor: float
    avg_pnl: float
    total_pnl: float
    fitness: float
    current_score: int
    accepted: bool
    reason: str


@dataclass
class ScanResult:
    candidates: list[SignalCandidate] = field(default_factory=list)
    sendable: list[SignalCandidate] = field(default_factory=list)
    successful_symbols: int = 0
    failed_symbols: int = 0
    total_symbols: int = 0
    skipped_symbols: list[str] = field(default_factory=list)
    trend_passed: int = 0
    trend_blocked: int = 0
    trend_unknown: int = 0
    neural_passed: int = 0
    neural_blocked: int = 0
    super_deal_passed: int = 0
    super_deal_blocked: int = 0
    improvements_passed: int = 0
    improvements_blocked: int = 0
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
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp_path, path)


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
    data: dict[str, dict[str, str]] = {}

    # Безопасный режим по умолчанию: LIVE-ключи читаются только из Railway Variables/.env.
    # Файловое хранение можно включить вручную через ALLOW_API_KEYS_FILE=true, если очень нужно.
    if ALLOW_API_KEYS_FILE:
        file_data = load_json(API_KEYS_FILE, {})
        if isinstance(file_data, dict):
            for exchange, values in file_data.items():
                if isinstance(values, dict):
                    data[str(exchange).lower()] = {
                        "api_key": str(values.get("api_key", "")).strip(),
                        "api_secret": str(values.get("api_secret", "")).strip(),
                    }

    # Railway/env ключи имеют приоритет над файлом.
    for exchange in ("mexc", "bingx"):
        key = os.getenv(f"{exchange.upper()}_API_KEY", "").strip()
        secret = os.getenv(f"{exchange.upper()}_API_SECRET", "").strip()
        if key and secret:
            data[exchange] = {"api_key": key, "api_secret": secret}
    return data


def save_api_keys(data: dict[str, dict[str, str]]) -> None:
    if not ALLOW_API_KEYS_FILE:
        # Не сохраняем секреты в filesystem по умолчанию.
        return
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


async def load_trades_locked() -> list[dict[str, Any]]:
    async with TRADES_LOCK:
        return load_trades()


async def save_trades_locked(trades: list[dict[str, Any]]) -> None:
    async with TRADES_LOCK:
        save_trades(trades)


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


def neural_strategy_profiles() -> list[dict[str, Any]]:
    """Набор алгоритмов, которые нейро-оптимизатор перебирает на свежей истории.

    Здесь нет обещания прибыли: это быстрый локальный walk-forward/backtest фильтр.
    Он нужен, чтобы текущий сигнал поддерживался не одним фиксированным скорингом,
    а лучшим из нескольких профилей на последнем участке рынка.
    """
    return [
        {
            "id": "trend_momentum",
            "name": "EMA + RSI + MACD momentum",
            "threshold": 7,
            "min_diff": 3,
            "volume_min": 1.0,
            "weights": {"ema_stack": 2, "price_ema21": 1, "rsi_zone": 2, "rsi_slope": 1, "macd": 2, "volume": 1, "breakout": 1, "candle": 1},
        },
        {
            "id": "breakout_volume",
            "name": "Пробой + объём",
            "threshold": 6,
            "min_diff": 2,
            "volume_min": 1.18,
            "weights": {"ema_stack": 1, "price_ema21": 1, "rsi_zone": 1, "rsi_slope": 1, "macd": 1, "volume": 3, "breakout": 3, "candle": 1},
        },
        {
            "id": "conservative_trend",
            "name": "Консервативный тренд",
            "threshold": 8,
            "min_diff": 4,
            "volume_min": 1.05,
            "weights": {"ema_stack": 3, "price_ema21": 2, "rsi_zone": 1, "rsi_slope": 1, "macd": 2, "volume": 1, "breakout": 1, "candle": 1},
        },
        {
            "id": "macd_rsi_drive",
            "name": "MACD + RSI drive",
            "threshold": 6,
            "min_diff": 2,
            "volume_min": 1.0,
            "weights": {"ema_stack": 1, "price_ema21": 1, "rsi_zone": 2, "rsi_slope": 2, "macd": 3, "volume": 1, "breakout": 0, "candle": 1},
        },
        {
            "id": "pullback_to_ema",
            "name": "Откат к EMA21",
            "threshold": 6,
            "min_diff": 2,
            "volume_min": 0.8,
            "pullback_atr": 0.65,
            "weights": {"ema_stack": 3, "price_ema21": 0, "rsi_zone": 1, "rsi_slope": 2, "macd": 1, "volume": 0, "breakout": 0, "candle": 1, "pullback": 3},
        },
        {
            "id": "range_reversal",
            "name": "RSI reversal",
            "threshold": 5,
            "min_diff": 2,
            "volume_min": 0.9,
            "allow_reversal": True,
            "weights": {"ema_stack": 0, "price_ema21": 0, "rsi_zone": 1, "rsi_slope": 2, "macd": 2, "volume": 1, "breakout": 0, "candle": 1, "reversal": 3},
        },
        {
            "id": "high_volume_trend",
            "name": "High-volume trend",
            "threshold": 7,
            "min_diff": 3,
            "volume_min": 1.35,
            "weights": {"ema_stack": 2, "price_ema21": 1, "rsi_zone": 1, "rsi_slope": 1, "macd": 1, "volume": 4, "breakout": 1, "candle": 1},
        },
        {
            "id": "ema_breakout_strict",
            "name": "EMA strict breakout",
            "threshold": 8,
            "min_diff": 4,
            "volume_min": 1.1,
            "weights": {"ema_stack": 4, "price_ema21": 1, "rsi_zone": 1, "rsi_slope": 1, "macd": 1, "volume": 1, "breakout": 3, "candle": 1},
        },
    ]


def neural_candle_arrays(candles: list[dict[str, float]]) -> dict[str, list[Any]]:
    closes = [c["close"] for c in candles]
    highs = [c["high"] for c in candles]
    lows = [c["low"] for c in candles]
    return {
        "opens": [c["open"] for c in candles],
        "closes": closes,
        "highs": highs,
        "lows": lows,
        "volumes": [c["volume"] for c in candles],
        "ema9": ema(closes, 9),
        "ema21": ema(closes, 21),
        "ema50": ema(closes, 50),
        "rsi": calculate_rsi(closes, 14),
        "atr": calculate_atr(highs, lows, closes, 14),
        "hist": macd_values(closes)[2],
    }


def neural_profile_signal_at(arrays: dict[str, list[Any]], idx: int, profile: dict[str, Any]) -> tuple[Optional[str], int]:
    if idx < 55:
        return None, 0

    opens = arrays["opens"]
    closes = arrays["closes"]
    highs = arrays["highs"]
    lows = arrays["lows"]
    volumes = arrays["volumes"]
    ema9_values = arrays["ema9"]
    ema21_values = arrays["ema21"]
    ema50_values = arrays["ema50"]
    rsis = arrays["rsi"]
    atrs = arrays["atr"]
    hist = arrays["hist"]

    entry = float(closes[idx])
    rsi_now = rsis[idx]
    rsi_prev = rsis[idx - 1] if idx > 0 else None
    atr_now = atrs[idx]
    if entry <= 0 or rsi_now is None or rsi_prev is None or atr_now is None or atr_now <= 0:
        return None, 0

    weights = profile.get("weights", {})
    long_score = 0
    short_score = 0

    ema_up = entry > ema9_values[idx] > ema21_values[idx] > ema50_values[idx]
    ema_down = entry < ema9_values[idx] < ema21_values[idx] < ema50_values[idx]
    if ema_up:
        long_score += int(weights.get("ema_stack", 0))
    if ema_down:
        short_score += int(weights.get("ema_stack", 0))

    if entry > ema21_values[idx]:
        long_score += int(weights.get("price_ema21", 0))
    elif entry < ema21_values[idx]:
        short_score += int(weights.get("price_ema21", 0))

    if 52 <= rsi_now <= 70:
        long_score += int(weights.get("rsi_zone", 0))
    elif 30 <= rsi_now <= 48:
        short_score += int(weights.get("rsi_zone", 0))

    if rsi_now > rsi_prev:
        long_score += int(weights.get("rsi_slope", 0))
    elif rsi_now < rsi_prev:
        short_score += int(weights.get("rsi_slope", 0))

    if len(hist) > idx and idx > 0:
        if hist[idx] > 0 and hist[idx] > hist[idx - 1]:
            long_score += int(weights.get("macd", 0))
        elif hist[idx] < 0 and hist[idx] < hist[idx - 1]:
            short_score += int(weights.get("macd", 0))

    avg_volume = sum(volumes[idx - 20:idx]) / 20 if idx >= 20 else max(float(volumes[idx]), 1.0)
    volume_ratio = float(volumes[idx]) / avg_volume if avg_volume > 0 else 1.0
    candle_green = closes[idx] > opens[idx]
    candle_red = closes[idx] < opens[idx]
    if volume_ratio >= float(profile.get("volume_min", 1.0)) and candle_green:
        long_score += int(weights.get("volume", 0))
    elif volume_ratio >= float(profile.get("volume_min", 1.0)) and candle_red:
        short_score += int(weights.get("volume", 0))

    previous_high = max(highs[idx - 20:idx])
    previous_low = min(lows[idx - 20:idx])
    if entry >= previous_high * 0.998:
        long_score += int(weights.get("breakout", 0))
    if entry <= previous_low * 1.002:
        short_score += int(weights.get("breakout", 0))

    if candle_green:
        long_score += int(weights.get("candle", 0))
    if candle_red:
        short_score += int(weights.get("candle", 0))

    pullback_atr = profile.get("pullback_atr")
    if pullback_atr is not None:
        near_ema21 = abs(entry - ema21_values[idx]) <= float(atr_now) * float(pullback_atr)
        if near_ema21 and ema_up and rsi_now > rsi_prev:
            long_score += int(weights.get("pullback", 0))
        if near_ema21 and ema_down and rsi_now < rsi_prev:
            short_score += int(weights.get("pullback", 0))

    if profile.get("allow_reversal"):
        # Reversal-профиль намеренно слабее и пропускается только если backtest подтверждает плюс.
        if rsi_now < 30 and rsi_now > rsi_prev and candle_green:
            long_score += int(weights.get("reversal", 0))
        if rsi_now > 70 and rsi_now < rsi_prev and candle_red:
            short_score += int(weights.get("reversal", 0))

    min_diff = int(profile.get("min_diff", 2))
    threshold = int(profile.get("threshold", 6))
    diff = long_score - short_score
    if long_score >= threshold and diff >= min_diff:
        return "LONG", long_score
    if short_score >= threshold and diff <= -min_diff:
        return "SHORT", short_score
    return None, max(long_score, short_score)


def neural_simulate_trade(
    candles: list[dict[str, float]],
    side: str,
    entry_index: int,
    stop: float,
    target: float,
    horizon: int,
) -> float:
    entry = float(candles[entry_index]["close"])
    end = min(len(candles) - 1, entry_index + horizon)

    for i in range(entry_index + 1, end + 1):
        high = float(candles[i]["high"])
        low = float(candles[i]["low"])
        if side == "LONG":
            # Консервативно: если SL и TP были в одной свече, считаем, что сначала сработал SL.
            if low <= stop:
                return pct_from_entry(stop, entry)
            if high >= target:
                return pct_from_entry(target, entry)
        else:
            if high >= stop:
                return -pct_from_entry(stop, entry)
            if low <= target:
                return -pct_from_entry(target, entry)

    exit_price = float(candles[end]["close"])
    pnl = pct_from_entry(exit_price, entry)
    return -pnl if side == "SHORT" else pnl


def neural_backtest_profile(
    candles: list[dict[str, float]],
    profile: dict[str, Any],
    candidate_side: str,
) -> NeuralBacktestResult:
    if len(candles) < 90:
        return NeuralBacktestResult(
            str(profile.get("id")), str(profile.get("name")), candidate_side, 0, 0.0, 0.0, 0.0, 0.0, -999.0, 0, False, "мало свечей"
        )

    arrays = neural_candle_arrays(candles)
    pnls: list[float] = []
    start = 70
    end = max(start, len(candles) - NEURAL_OPTIMIZER_HORIZON_CANDLES - 1)
    for idx in range(start, end):
        side, _score = neural_profile_signal_at(arrays, idx, profile)
        if side != candidate_side:
            continue
        atr_value = arrays["atr"][idx]
        if atr_value is None or atr_value <= 0:
            continue
        entry = float(candles[idx]["close"])
        stop, tps = build_stop_and_tps(side, entry, float(atr_value))
        tp_index = max(1, min(len(tps), AUTO_CLOSE_TP_INDEX)) - 1
        pnls.append(neural_simulate_trade(candles, side, idx, stop, tps[tp_index], NEURAL_OPTIMIZER_HORIZON_CANDLES))

    if not pnls:
        current_side, current_score = neural_profile_signal_at(arrays, len(candles) - 1, profile)
        return NeuralBacktestResult(
            str(profile.get("id")), str(profile.get("name")), candidate_side, 0, 0.0, 0.0, 0.0, 0.0, -100.0, current_score, False, "нет похожих сделок в истории"
        )

    wins = [p for p in pnls if p > 0]
    losses = [p for p in pnls if p <= 0]
    gross_profit = sum(wins)
    gross_loss = abs(sum(losses))
    profit_factor = 99.0 if gross_loss == 0 and gross_profit > 0 else (gross_profit / gross_loss if gross_loss > 0 else 0.0)
    win_rate = len(wins) / len(pnls)
    avg_pnl = sum(pnls) / len(pnls)
    total_pnl = sum(pnls)
    current_side, current_score = neural_profile_signal_at(arrays, len(candles) - 1, profile)
    fitness = (avg_pnl * 1.8) + ((win_rate - 0.5) * 1.2) + min(profit_factor, 3.0) * 0.25 + min(len(pnls), 20) * 0.015
    accepted = (
        current_side == candidate_side
        and len(pnls) >= NEURAL_OPTIMIZER_MIN_TRADES
        and win_rate >= NEURAL_OPTIMIZER_MIN_WIN_RATE
        and profit_factor >= NEURAL_OPTIMIZER_MIN_PROFIT_FACTOR
        and avg_pnl >= NEURAL_OPTIMIZER_MIN_AVG_PNL
    )
    reason = "преимущество подтверждено" if accepted else "метрики ниже порога"
    return NeuralBacktestResult(
        str(profile.get("id")),
        str(profile.get("name")),
        candidate_side,
        len(pnls),
        win_rate,
        profit_factor,
        avg_pnl,
        total_pnl,
        fitness,
        current_score,
        accepted,
        reason,
    )


def neural_backtest_profile_walk_forward(
    candles: list[dict[str, float]],
    profile: dict[str, Any],
    candidate_side: str,
) -> NeuralBacktestResult:
    """Walk-forward: 70% истории для подбора, 30% для проверки.

    Если профиль хорош только на train, но не подтверждается на test, он не проходит.
    """
    base = neural_backtest_profile(candles, profile, candidate_side)
    if len(candles) < 110:
        return base

    arrays = neural_candle_arrays(candles)
    split = int(len(candles) * WALK_FORWARD_TRAIN_RATIO)
    split = max(75, min(len(candles) - NEURAL_OPTIMIZER_HORIZON_CANDLES - 2, split))
    pnls_test: list[float] = []

    end = max(split, len(candles) - NEURAL_OPTIMIZER_HORIZON_CANDLES - 1)
    for idx in range(split, end):
        side, _score = neural_profile_signal_at(arrays, idx, profile)
        if side != candidate_side:
            continue
        atr_value = arrays["atr"][idx]
        if atr_value is None or atr_value <= 0:
            continue
        entry = float(candles[idx]["close"])
        stop, tps = build_stop_and_tps(side, entry, float(atr_value))
        tp_index = max(1, min(len(tps), AUTO_CLOSE_TP_INDEX)) - 1
        pnls_test.append(neural_simulate_trade(candles, side, idx, stop, tps[tp_index], NEURAL_OPTIMIZER_HORIZON_CANDLES))

    if len(pnls_test) < WALK_FORWARD_MIN_TEST_TRADES:
        base.accepted = False
        base.reason = f"walk-forward: мало test-сделок ({len(pnls_test)}/{WALK_FORWARD_MIN_TEST_TRADES})"
        base.fitness -= 1.0
        return base

    wins = [p for p in pnls_test if p > 0]
    losses = [p for p in pnls_test if p <= 0]
    gross_profit = sum(wins)
    gross_loss = abs(sum(losses))
    pf_test = 99.0 if gross_loss == 0 and gross_profit > 0 else (gross_profit / gross_loss if gross_loss > 0 else 0.0)
    wr_test = len(wins) / len(pnls_test)
    avg_test = sum(pnls_test) / len(pnls_test)

    if not (wr_test >= NEURAL_OPTIMIZER_MIN_WIN_RATE and pf_test >= NEURAL_OPTIMIZER_MIN_PROFIT_FACTOR and avg_test >= NEURAL_OPTIMIZER_MIN_AVG_PNL):
        base.accepted = False
        base.reason = f"walk-forward не подтвердил: test WR {wr_test * 100:.0f}%, PF {pf_test:.2f}, avg {avg_test:+.2f}%"
        base.fitness -= 0.75
        return base

    base.reason = f"walk-forward подтверждён: test {len(pnls_test)} сдел., WR {wr_test * 100:.0f}%, PF {pf_test:.2f}, avg {avg_test:+.2f}%"
    base.fitness += 0.35
    return base


def choose_neural_optimizer_result(candidate: SignalCandidate, candles: list[dict[str, float]]) -> Optional[NeuralBacktestResult]:
    if not candles or len(candles) < 90:
        return None
    arrays = neural_candle_arrays(candles)
    current_index = len(candles) - 1
    results: list[NeuralBacktestResult] = []
    for profile in neural_strategy_profiles()[:NEURAL_OPTIMIZER_MAX_PROFILES]:
        current_side, _score = neural_profile_signal_at(arrays, current_index, profile)
        if current_side != candidate.side:
            continue
        if TRADING_IMPROVEMENTS_ENABLED and WALK_FORWARD_OPTIMIZER_ENABLED:
            results.append(neural_backtest_profile_walk_forward(candles, profile, candidate.side))
        else:
            results.append(neural_backtest_profile(candles, profile, candidate.side))

    if not results:
        return NeuralBacktestResult(
            "no_profile",
            "нет совпавшего профиля",
            candidate.side,
            0,
            0.0,
            0.0,
            0.0,
            0.0,
            -999.0,
            0,
            False,
            "ни один профиль не подтвердил текущий сигнал",
        )

    results.sort(key=lambda r: r.fitness, reverse=True)
    best = results[0]
    if not best.accepted and not NEURAL_OPTIMIZER_STRICT_MODE:
        # Мягкий режим не блокирует сильный базовый сигнал, но не даёт ему AI-бонус.
        if candidate.probability >= MIN_SIGNAL_PROBABILITY + 8 and best.trades >= max(3, NEURAL_OPTIMIZER_MIN_TRADES // 2):
            best.accepted = True
            best.reason = "мягкий режим: базовый сигнал сильный, AI не возражает"
    return best


def neural_result_to_dict(result: Optional[NeuralBacktestResult]) -> Optional[dict[str, Any]]:
    if result is None:
        return None
    return {
        "profile_id": result.profile_id,
        "profile_name": result.profile_name,
        "side": result.side,
        "trades": result.trades,
        "win_rate": result.win_rate,
        "profit_factor": result.profit_factor,
        "avg_pnl": result.avg_pnl,
        "total_pnl": result.total_pnl,
        "fitness": result.fitness,
        "current_score": result.current_score,
        "accepted": result.accepted,
        "reason": result.reason,
        "updated_at": time.time(),
    }


def save_neural_optimizer_choice(symbol: str, result: NeuralBacktestResult) -> None:
    try:
        state = load_json(NEURAL_OPTIMIZER_FILE, {})
        if not isinstance(state, dict):
            state = {}
        last_best = state.get("last_best")
        if not isinstance(last_best, dict):
            last_best = {}
        last_best[compact_symbol(symbol)] = neural_result_to_dict(result)
        state["last_best"] = last_best
        state["updated_at"] = time.time()
        save_json(NEURAL_OPTIMIZER_FILE, state)
    except Exception:
        logging.exception("Не удалось сохранить состояние нейро-оптимизатора")


def neural_optimizer_stats_text() -> str:
    ai_trades = [
        t for t in get_closed_trades_for_learning(200)
        if isinstance(t.get("ai_optimizer"), dict)
    ]
    if ai_trades:
        grouped: dict[str, list[dict[str, Any]]] = {}
        for trade in ai_trades:
            info = trade.get("ai_optimizer") or {}
            profile = str(info.get("profile_name") or info.get("profile_id") or "unknown")
            grouped.setdefault(profile, []).append(trade)
        rows = []
        for profile, trades in grouped.items():
            values = [trade_pnl_pct_value(t) or 0 for t in trades]
            wr = sum(1 for v in values if v > 0) / len(values)
            avg = sum(values) / len(values)
            rows.append((len(trades), wr, avg, profile))
        rows.sort(key=lambda x: (x[2], x[1], x[0]), reverse=True)
        count, wr, avg, profile = rows[0]
        return f"AI-сделок {len(ai_trades)}, лучший: {profile} ({count} сдел., WR {wr * 100:.0f}%, avg {avg:+.2f}%)"

    state = load_json(NEURAL_OPTIMIZER_FILE, {})
    last_best = state.get("last_best") if isinstance(state, dict) else None
    if isinstance(last_best, dict) and last_best:
        latest_symbol, latest_info = max(
            last_best.items(),
            key=lambda kv: float((kv[1] or {}).get("updated_at") or 0),
        )
        if isinstance(latest_info, dict):
            return (
                f"последний выбор {latest_symbol}: {latest_info.get('profile_name', 'n/a')} "
                f"WR {float(latest_info.get('win_rate') or 0) * 100:.0f}%, "
                f"PF {float(latest_info.get('profit_factor') or 0):.2f}, "
                f"avg {float(latest_info.get('avg_pnl') or 0):+.2f}%"
            )
    return "истории пока нет"


def apply_neural_optimizer(candidate: Optional[SignalCandidate], candles: list[dict[str, float]]) -> Optional[SignalCandidate]:
    if candidate is None or not NEURAL_OPTIMIZER_ENABLED:
        return candidate

    result = choose_neural_optimizer_result(candidate, candles)
    if result is None:
        if NEURAL_OPTIMIZER_STRICT_MODE:
            return None
        return clone_candidate(
            candidate,
            candidate.probability,
            candidate.reasons + ["🤖 AI: мало истории для перебора алгоритмов, оставлен базовый сигнал"],
        )

    save_neural_optimizer_choice(candidate.symbol, result)
    if not result.accepted:
        return None

    edge_bonus = min(
        NEURAL_OPTIMIZER_PROBABILITY_BONUS,
        max(0, int(round((result.win_rate - 0.50) * 20 + min(result.profit_factor, 3.0) - 1))),
    )
    new_probability = min(95, candidate.probability + edge_bonus)
    ai_reason = (
        f"🤖 AI: выбран алгоритм «{result.profile_name}» — "
        f"{result.trades} сдел., WR {result.win_rate * 100:.0f}%, "
        f"PF {result.profit_factor:.2f}, avg {result.avg_pnl:+.2f}%"
    )
    return SignalCandidate(
        symbol=candidate.symbol,
        side=candidate.side,
        probability=new_probability,
        entry=candidate.entry,
        stop=candidate.stop,
        take_profits=list(candidate.take_profits),
        reasons=(candidate.reasons + [ai_reason])[:8],
        timeframe=candidate.timeframe,
        trend=candidate.trend,
        ai_optimizer=neural_result_to_dict(result),
        is_super_deal=candidate.is_super_deal,
        super_deal_score=candidate.super_deal_score,
    )


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
        trend=candidate.trend,
        ai_optimizer=candidate.ai_optimizer,
        is_super_deal=candidate.is_super_deal,
        super_deal_score=candidate.super_deal_score,
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


# ---------- trading improvements ----------

def trading_improvements_active() -> bool:
    return bool(TRADING_IMPROVEMENTS_ENABLED)


def load_improvements_stats() -> dict[str, Any]:
    data = load_json(IMPROVEMENTS_STATS_FILE, {})
    if not isinstance(data, dict):
        data = {}
    data.setdefault("counters", {})
    data.setdefault("updated_at", 0)
    return data


def save_improvements_stats(data: dict[str, Any]) -> None:
    data["updated_at"] = time.time()
    save_json(IMPROVEMENTS_STATS_FILE, data)


def increment_improvement_counter(name: str, value: int = 1) -> None:
    try:
        data = load_improvements_stats()
        counters = data.setdefault("counters", {})
        counters[name] = int(counters.get(name, 0)) + value
        save_improvements_stats(data)
    except Exception:
        logging.exception("Не удалось обновить статистику улучшений")


def profit_factor_from_values(values: list[float]) -> float:
    wins = [v for v in values if v > 0]
    losses = [v for v in values if v <= 0]
    gp = sum(wins)
    gl = abs(sum(losses))
    if gl == 0 and gp > 0:
        return 99.0
    return gp / gl if gl > 0 else 0.0


def trade_pnl_usdt_estimate(trade: dict[str, Any]) -> float:
    try:
        explicit = trade.get("realized_pnl_usdt")
        if explicit is not None:
            return float(explicit)
    except Exception:
        pass
    pnl_pct = trade_pnl_pct_value(trade)
    if pnl_pct is None:
        return 0.0
    notional = float(trade.get("notional_usdt") or trade.get("initial_notional_usdt") or TRADE_MARGIN_USDT or 0)
    return notional * pnl_pct / 100.0


def loss_limit_block_reason(equity: Optional[float] = None) -> Optional[str]:
    if not trading_improvements_active():
        return None
    equity_value = float(equity or ACCOUNT_EQUITY_USDT)
    now = time.time()
    closed = get_closed_trades_for_learning(500)
    day_start = now - 86400
    week_start = now - 7 * 86400
    day_pnl = sum(trade_pnl_usdt_estimate(t) for t in closed if float(t.get("closed_at") or 0) >= day_start)
    week_pnl = sum(trade_pnl_usdt_estimate(t) for t in closed if float(t.get("closed_at") or 0) >= week_start)
    if day_pnl <= -equity_value * MAX_DAILY_LOSS_PERCENT / 100.0:
        return f"дневной лимит убытка достигнут ({day_pnl:.2f} USDT, лимит {MAX_DAILY_LOSS_PERCENT:g}%)"
    if week_pnl <= -equity_value * MAX_WEEKLY_LOSS_PERCENT / 100.0:
        return f"недельный лимит убытка достигнут ({week_pnl:.2f} USDT, лимит {MAX_WEEKLY_LOSS_PERCENT:g}%)"

    streak = loss_streak_from_trades(closed)
    if streak >= MAX_CONSECUTIVE_LOSSES and closed:
        last_closed = float(closed[0].get("closed_at") or 0)
        pause_until = last_closed + PAUSE_AFTER_LOSS_STREAK_HOURS * 3600
        if now < pause_until:
            hours_left = max(1, int((pause_until - now) // 3600) + 1)
            return f"серия минусов {streak}, пауза ещё примерно {hours_left} ч"
    return None


def symbol_rating_block_reason(symbol: str) -> Optional[str]:
    if not (trading_improvements_active() and COIN_RATING_FILTER_ENABLED):
        return None
    compact = compact_symbol(symbol)
    closed = [
        t for t in get_closed_trades_for_learning(300)
        if compact_symbol(str(t.get("symbol", ""))) == compact
    ]
    if len(closed) < SYMBOL_RATING_MIN_TRADES:
        return None
    values = [trade_pnl_pct_value(t) or 0.0 for t in closed]
    wr = sum(1 for v in values if v > 0) / len(values)
    pf = profit_factor_from_values(values)
    if wr < MIN_SYMBOL_WIN_RATE or pf < MIN_SYMBOL_PROFIT_FACTOR:
        return f"рейтинг монеты слабый: WR {wr * 100:.0f}%, PF {pf:.2f}, сделок {len(values)}"
    return None


def symbol_bucket(symbol: str) -> str:
    compact = compact_symbol(symbol)
    if compact.startswith("BTC"):
        return "BTC"
    if compact.startswith("ETH"):
        return "ETH"
    return "ALT"


def exposure_block_reason(candidate: SignalCandidate) -> Optional[str]:
    if not (trading_improvements_active() and CORRELATION_FILTER_ENABLED):
        return None
    open_trades = get_open_trades()
    same_dir = [t for t in open_trades if str(t.get("side", "")).upper() == candidate.side.upper()]
    if len(same_dir) >= MAX_SAME_DIRECTION_TRADES:
        return f"уже есть {len(same_dir)} открытая(ых) сделка(ок) в сторону {candidate.side}"
    if symbol_bucket(candidate.symbol) == "ALT":
        alt_trades = [t for t in open_trades if symbol_bucket(str(t.get("symbol", ""))) == "ALT"]
        if len(alt_trades) >= MAX_ALT_TRADES:
            return f"лимит коррелированных ALT-сделок: {len(alt_trades)}/{MAX_ALT_TRADES}"
    return None


def candle_market_quality(candles: list[dict[str, float]]) -> tuple[Optional[str], dict[str, Any]]:
    if not candles or len(candles) < 30:
        return "мало свечей для проверки качества рынка", {}
    last = candles[-1]
    close = float(last.get("close") or 0)
    if close <= 0:
        return "некорректная цена", {}

    last_quote_volume = float(last.get("volume") or 0) * close
    avg_quote_volume = sum(float(c.get("volume") or 0) * float(c.get("close") or 0) for c in candles[-21:-1]) / 20
    highs = [float(c["high"]) for c in candles]
    lows = [float(c["low"]) for c in candles]
    closes = [float(c["close"]) for c in candles]
    atrs = calculate_atr(highs, lows, closes, 14)
    atr_now = atrs[-1] if atrs else None
    atr_pct = (float(atr_now) / close * 100.0) if atr_now else 0.0
    candle_pct = abs(float(last.get("close")) - float(last.get("open"))) / close * 100.0

    meta = {
        "last_quote_volume": last_quote_volume,
        "avg_quote_volume": avg_quote_volume,
        "atr_pct": atr_pct,
        "last_candle_pct": candle_pct,
    }
    if LIQUIDITY_FILTER_ENABLED:
        if last_quote_volume < MIN_LAST_CANDLE_VOLUME_USDT:
            return f"низкий объём последней свечи: {last_quote_volume:.0f} USDT", meta
        if avg_quote_volume < MIN_AVG_CANDLE_VOLUME_USDT:
            return f"низкий средний объём: {avg_quote_volume:.0f} USDT", meta
    if atr_pct < MIN_ATR_PCT:
        return f"слишком низкая волатильность ATR {atr_pct:.2f}%", meta
    if atr_pct > MAX_ATR_PCT:
        return f"слишком высокая волатильность ATR {atr_pct:.2f}%", meta
    if candle_pct > MAX_PUMP_CANDLE_PCT:
        return f"последняя свеча слишком большая {candle_pct:.2f}% — риск входа после пампа/дампа", meta
    return None, meta


def market_regime(candles: list[dict[str, float]]) -> tuple[str, str]:
    if not candles or len(candles) < 80:
        return "UNKNOWN", "мало свечей"
    closes = [c["close"] for c in candles]
    highs = [c["high"] for c in candles]
    lows = [c["low"] for c in candles]
    ema21_values = ema(closes, 21)
    ema50_values = ema(closes, 50)
    atrs = calculate_atr(highs, lows, closes, 14)
    close = closes[-1]
    atr_pct = (atrs[-1] / close * 100.0) if atrs and atrs[-1] and close else 0.0
    slope = (ema50_values[-1] - ema50_values[-8]) / close * 100.0 if len(ema50_values) >= 8 and close else 0.0
    distance = abs(closes[-1] - ema21_values[-1]) / close * 100.0 if close else 0.0
    if atr_pct > MAX_ATR_PCT * 0.75:
        return "HIGH_VOL", f"высокая волатильность ATR {atr_pct:.2f}%"
    if abs(slope) < 0.05 and distance < max(0.15, atr_pct * 0.3):
        return "FLAT", f"флэт: EMA50 почти без наклона ({slope:+.2f}%)"
    if atr_pct < MIN_ATR_PCT:
        return "LOW_VOL", f"низкая волатильность ATR {atr_pct:.2f}%"
    return "TREND", f"трендовый режим, ATR {atr_pct:.2f}%, наклон EMA50 {slope:+.2f}%"


def apply_trading_improvements_filters(candidate: Optional[SignalCandidate], candles: list[dict[str, float]]) -> Optional[SignalCandidate]:
    if candidate is None or not trading_improvements_active():
        return candidate

    block_reason = loss_limit_block_reason()
    if block_reason:
        increment_improvement_counter("blocked_loss_limits")
        return None

    block_reason = symbol_rating_block_reason(candidate.symbol)
    if block_reason:
        increment_improvement_counter("blocked_symbol_rating")
        return None

    block_reason = exposure_block_reason(candidate)
    if block_reason:
        increment_improvement_counter("blocked_exposure")
        return None

    block_reason, meta = candle_market_quality(candles)
    if block_reason:
        increment_improvement_counter("blocked_market_quality")
        return None

    regime, regime_reason = market_regime(candles)
    if MARKET_REGIME_FILTER_ENABLED and regime in {"FLAT", "UNKNOWN", "LOW_VOL"}:
        # Во флэте этот бот с пробойной/трендовой логикой часто ловит ложные входы.
        increment_improvement_counter(f"blocked_regime_{regime.lower()}")
        return None

    reasons = candidate.reasons + [
        f"🚀 улучшения: рынок прошёл риск/ликвидность/режим",
        f"📈 режим рынка: {regime} — {regime_reason}",
    ]
    if meta:
        reasons.append(
            f"💧 ликвидность свечи: {meta.get('last_quote_volume', 0):.0f} USDT, ATR {meta.get('atr_pct', 0):.2f}%"
        )
    increment_improvement_counter("passed_signal_filters")
    return clone_candidate(candidate, candidate.probability, reasons[:10])


def improvements_stats_text() -> str:
    data = load_improvements_stats()
    counters = data.get("counters", {}) if isinstance(data, dict) else {}
    closed = get_closed_trades_for_learning(500)
    values = [trade_pnl_pct_value(t) or 0.0 for t in closed]
    wins = [v for v in values if v > 0]
    wr = (len(wins) / len(values) * 100.0) if values else 0.0
    pf = profit_factor_from_values(values)
    avg = (sum(values) / len(values)) if values else 0.0
    estimated_pnl = sum(trade_pnl_usdt_estimate(t) for t in closed)

    by_symbol: dict[str, list[float]] = {}
    for t in closed:
        by_symbol.setdefault(compact_symbol(str(t.get("symbol", "UNKNOWN"))), []).append(trade_pnl_pct_value(t) or 0.0)
    symbol_rows = []
    for symbol, vals in by_symbol.items():
        if len(vals) >= 2:
            symbol_rows.append((sum(vals) / len(vals), len(vals), symbol))
    symbol_rows.sort(reverse=True)
    best = f"{symbol_rows[0][2]} avg {symbol_rows[0][0]:+.2f}% ({symbol_rows[0][1]} сдел.)" if symbol_rows else "нет данных"
    worst = f"{symbol_rows[-1][2]} avg {symbol_rows[-1][0]:+.2f}% ({symbol_rows[-1][1]} сдел.)" if symbol_rows else "нет данных"

    counter_lines = []
    for key, value in sorted(counters.items(), key=lambda kv: str(kv[0])):
        counter_lines.append(f"• {html.escape(str(key))}: <b>{int(value)}</b>")
    counters_text = "\n".join(counter_lines) if counter_lines else "счётчики пока пустые"

    return (
        "<b>🚀 Улучшения торговли</b>\n\n"
        f"Статус: <b>{html.escape(trading_improvements_label())}</b>\n"
        f"Риск на сделку: <b>{RISK_PER_TRADE_PERCENT:g}%</b>\n"
        f"Дневной лимит убытка: <b>{MAX_DAILY_LOSS_PERCENT:g}%</b>, недельный: <b>{MAX_WEEKLY_LOSS_PERCENT:g}%</b>\n"
        f"Пауза после серии минусов: <b>{MAX_CONSECUTIVE_LOSSES}</b> минуса → <b>{PAUSE_AFTER_LOSS_STREAK_HOURS}</b> ч\n"
        f"Частичные TP: <b>{TP1_CLOSE_PERCENT}% / {TP2_CLOSE_PERCENT}% / {TP3_CLOSE_PERCENT}%</b>\n"
        f"Breakeven после TP1: <b>{'ON' if MOVE_SL_TO_BREAKEVEN_AFTER_TP1 else 'OFF'}</b>\n"
        f"Макс. спред LIVE: <b>{MAX_SPREAD_PCT:g}%</b>\n\n"
        "<b>Статистика закрытых сделок:</b>\n"
        f"Всего: <b>{len(values)}</b>, WR: <b>{wr:.0f}%</b>, PF: <b>{pf:.2f}</b>, avg: <b>{avg:+.2f}%</b>\n"
        f"Оценочный PnL: <b>{estimated_pnl:+.2f} USDT</b>\n"
        f"Лучшая монета: <b>{html.escape(best)}</b>\n"
        f"Худшая монета: <b>{html.escape(worst)}</b>\n\n"
        "<b>Счётчики фильтров:</b>\n"
        f"{counters_text}"
    )


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
    lines = [
        "<b>🔑 API ключи</b>",
        "",
        f"MEXC: <b>{'добавлены' if has_api_keys('mexc') else 'нет'}</b>",
        f"BingX: <b>{'добавлены' if has_api_keys('bingx') else 'нет'}</b>",
        "",
        "Для LIVE-торговли нужны права <b>Read + Trade</b>. <b>Withdraw/вывод средств не включай.</b>",
        "",
        "Безопасный способ: добавь ключи в Railway Variables:",
        "<code>MEXC_API_KEY</code>, <code>MEXC_API_SECRET</code>",
        "<code>BINGX_API_KEY</code>, <code>BINGX_API_SECRET</code>",
        "",
        "Команда /api_set включена по умолчанию, как в старой версии, и сохраняет ключи в data/api_keys.json.",
    ]
    if ALLOW_API_KEYS_FILE:
        lines += [
            "",
            "⚠️ Файловое хранение ключей включено. Чтобы отключить /api_set, установи ALLOW_API_KEYS_FILE=false.",
            "Команды:",
            "<code>/api_set MEXC API_KEY API_SECRET</code>",
            "<code>/api_clear MEXC</code>",
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
        protective = t.get("protective_orders") or {}
        protective_status = "есть" if protective.get("sl_order_id") or protective.get("tp_order_id") else "fallback"
        trend = t.get("trend") if isinstance(t.get("trend"), dict) else None
        trend_line = ""
        if trend:
            trend_line = (
                f"\n  тренд {html.escape(str(trend.get('timeframe', '')))}: "
                f"<b>{html.escape(trend_direction_label(str(trend.get('direction', ''))))}</b> "
                f"score {html.escape(str(trend.get('score', '')))}"
            )
        ai_info = t.get("ai_optimizer") if isinstance(t.get("ai_optimizer"), dict) else None
        ai_line = ""
        if ai_info:
            ai_line = (
                f"\n  AI: <b>{html.escape(str(ai_info.get('profile_name', '')))}</b> "
                f"WR {float(ai_info.get('win_rate') or 0) * 100:.0f}%, "
                f"PF {float(ai_info.get('profit_factor') or 0):.2f}, "
                f"avg {float(ai_info.get('avg_pnl') or 0):+.2f}%"
            )
        partial_line = ""
        if t.get("improvements_enabled"):
            hits_text = ",".join(map(str, t.get("partial_tp_hits") or [])) or "нет"
            partial_line = (
                f"\n  улучшения: <b>ON</b>; остаток amount {float(t.get('remaining_amount') or t.get('amount') or 0):g}; "
                f"текущий SL {html.escape(fmt_price(float(t.get('current_stop') or t.get('stop') or 0)))}; "
                f"TP hits: <b>{html.escape(hits_text)}</b>"
            )
        lines.append(
            "\n"
            f"• ID <code>{html.escape(str(t.get('id')))}</code> — "
            f"<b>{html.escape(str(t.get('symbol')))} {html.escape(str(t.get('side')))}</b> "
            f"({html.escape(str(t.get('mode')))}, {html.escape(str(t.get('exchange')))}):\n"
            f"  вход {html.escape(fmt_price(float(t.get('entry', 0))))}, "
            f"SL {html.escape(fmt_price(float(t.get('current_stop') or t.get('stop', 0))))}, "
            f"TP{tp_index} {html.escape(fmt_price(float(tp))) if tp else 'n/a'}\n"
            f"  объём ≈ ${float(t.get('notional_usdt', 0)):g}, amount {html.escape(str(t.get('amount')))}"
            f"{partial_line}"
            f"{trend_line}"
            f"{ai_line}\n"
            f"  защитные ордера: <b>{html.escape(protective_status)}</b>; ручное закрытие: <code>/close_trade {html.escape(str(t.get('id')))}</code>"
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
    super_deal: bool = False,
) -> str:
    side_clean = side.upper()
    emoji = "🟢" if side_clean == "LONG" else "🔴"
    title_prefix = "🤖 Авто-сигнал" if auto else "Сигнал"
    alert_prefix = ""
    if super_deal:
        emoji = "🔴"
        title_prefix = "🚨 Супер-сигнал"
        alert_prefix = "🔴🔴🔴 <b>Внимание, есть супер сделка!</b>\n"

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
        f"{alert_prefix}{emoji} <b>{title_prefix}: {html.escape(symbol.upper())} / {html.escape(side_clean)}</b>\n"
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
        f"Нейросети: <b>{'ON' if NEURAL_OPTIMIZER_ENABLED else 'OFF'}</b>",
        f"AI-статус: <b>{html.escape(neural_optimizer_stats_text())}</b>",
        f"Smart-история: <b>{html.escape(smart_learning_stats_text())}</b>",
        f"Фильтр тренда: <b>{html.escape(trend_filter_label())}</b>",
        f"Супер сделка: <b>{html.escape(super_deal_label())}</b>",
        f"Улучшения торговли: <b>{html.escape(trading_improvements_label())}</b>",
        f"Данные получены: <b>{scan.successful_symbols}</b> / {scan.total_symbols or len(SYMBOLS)}",
        f"Ошибки/нет пары: <b>{scan.failed_symbols}</b>",
    ]
    if TREND_FILTER_ENABLED:
        lines.append(
            f"Тренд-фильтр: пропущено <b>{scan.trend_passed}</b>, "
            f"отсечено <b>{scan.trend_blocked}</b>, неизвестно/флэт <b>{scan.trend_unknown}</b>"
        )
    if NEURAL_OPTIMIZER_ENABLED:
        lines.append(
            f"AI-оптимизатор: пропущено <b>{scan.neural_passed}</b>, "
            f"отсечено <b>{scan.neural_blocked}</b>"
        )
    if SUPER_DEAL_ENABLED:
        lines.append(
            f"Супер-сделки: найдено <b>{scan.super_deal_passed}</b>, "
            f"отсечено <b>{scan.super_deal_blocked}</b>"
        )
    if TRADING_IMPROVEMENTS_ENABLED:
        lines.append(
            f"Улучшения торговли: пропущено <b>{scan.improvements_passed}</b>, "
            f"отсечено <b>{scan.improvements_blocked}</b>"
        )
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


def trend_direction_label(direction: str) -> str:
    value = direction.upper()
    if value == "BULL":
        return "бычий"
    if value == "BEAR":
        return "медвежий"
    if value == "FLAT":
        return "флэт/нет явного тренда"
    return "неизвестен"


def analyze_primary_trend(candles: Optional[list[dict[str, float]]], timeframe: Optional[str] = None) -> Optional[TrendInfo]:
    """Определяет основной тренд на старшем таймфрейме.

    Это консервативный score-фильтр, а не отдельный сигнал: он разрешает сделки
    только в сторону старшего тренда и отсекает флэт/непонятный рынок.
    """
    tf = timeframe or TREND_TIMEFRAME
    if not candles or len(candles) < 80:
        return TrendInfo("UNKNOWN", 0, 0, tf, ["недостаточно свечей для тренда"], 0.0)

    closes = [c["close"] for c in candles]
    highs = [c["high"] for c in candles]
    lows = [c["low"] for c in candles]
    entry = closes[-1]
    if entry <= 0:
        return TrendInfo("UNKNOWN", 0, 0, tf, ["некорректная цена тренда"], 0.0)

    ema21 = ema(closes, 21)
    ema50 = ema(closes, 50)
    ema100 = ema(closes, 100) if len(closes) >= 100 else []
    rsis = calculate_rsi(closes, 14)
    _, _, hist = macd_values(closes)

    bull_score = 0
    bear_score = 0
    bull_reasons: list[str] = []
    bear_reasons: list[str] = []

    if entry > ema21[-1] > ema50[-1]:
        bull_score += 2
        bull_reasons.append("цена выше EMA21/EMA50")
    elif entry < ema21[-1] < ema50[-1]:
        bear_score += 2
        bear_reasons.append("цена ниже EMA21/EMA50")

    slope_index = -6 if len(ema50) >= 6 else 0
    if ema50[-1] > ema50[slope_index]:
        bull_score += 1
        bull_reasons.append("EMA50 растёт")
    elif ema50[-1] < ema50[slope_index]:
        bear_score += 1
        bear_reasons.append("EMA50 снижается")

    if ema100:
        if entry > ema100[-1] and ema50[-1] > ema100[-1]:
            bull_score += 1
            bull_reasons.append("цена и EMA50 выше EMA100")
        elif entry < ema100[-1] and ema50[-1] < ema100[-1]:
            bear_score += 1
            bear_reasons.append("цена и EMA50 ниже EMA100")

    rsi_now = rsis[-1]
    if rsi_now is not None:
        if rsi_now >= 52:
            bull_score += 1
            bull_reasons.append(f"RSI {rsi_now:.1f} выше нейтрали")
        elif rsi_now <= 48:
            bear_score += 1
            bear_reasons.append(f"RSI {rsi_now:.1f} ниже нейтрали")

    if len(hist) >= 3:
        if hist[-1] > 0 and hist[-1] >= hist[-2]:
            bull_score += 1
            bull_reasons.append("MACD histogram поддерживает рост")
        elif hist[-1] < 0 and hist[-1] <= hist[-2]:
            bear_score += 1
            bear_reasons.append("MACD histogram поддерживает снижение")

    if len(highs) >= 31 and len(lows) >= 31:
        previous_high = max(highs[-31:-1])
        previous_low = min(lows[-31:-1])
        if entry >= previous_high * 0.995:
            bull_score += 1
            bull_reasons.append("цена у верхней границы 30 свечей")
        elif entry <= previous_low * 1.005:
            bear_score += 1
            bear_reasons.append("цена у нижней границы 30 свечей")

    diff = bull_score - bear_score
    confidence = min(95, 50 + abs(diff) * 8)
    if diff >= TREND_MIN_SCORE:
        return TrendInfo("BULL", diff, confidence, tf, bull_reasons[:4], entry)
    if diff <= -TREND_MIN_SCORE:
        return TrendInfo("BEAR", diff, confidence, tf, bear_reasons[:4], entry)

    neutral_reasons = [
        f"нет сильного перевеса: bull {bull_score} / bear {bear_score}",
        "фильтр ждёт более ясный старший тренд",
    ]
    return TrendInfo("FLAT", diff, confidence, tf, neutral_reasons, entry)


def apply_trend_filter(candidate: Optional[SignalCandidate], trend: Optional[TrendInfo]) -> Optional[SignalCandidate]:
    if candidate is None or not (TREND_FILTER_ENABLED or SUPER_DEAL_ENABLED):
        return candidate
    if trend is None or trend.direction in {"UNKNOWN", "FLAT"}:
        return None

    direction = trend.direction.upper()
    side = candidate.side.upper()
    allowed = (side == "LONG" and direction == "BULL") or (side == "SHORT" and direction == "BEAR")
    if not allowed:
        return None

    reason = (
        f"🧭 Тренд {trend.timeframe}: {trend_direction_label(direction)} "
        f"({trend.confidence}%, score {trend.score:+d})"
    )
    if trend.reasons:
        reason += " — " + "; ".join(trend.reasons[:2])
    return SignalCandidate(
        symbol=candidate.symbol,
        side=candidate.side,
        probability=candidate.probability,
        entry=candidate.entry,
        stop=candidate.stop,
        take_profits=list(candidate.take_profits),
        reasons=(candidate.reasons + [reason])[:7],
        timeframe=candidate.timeframe,
        trend=trend,
        ai_optimizer=candidate.ai_optimizer,
        is_super_deal=candidate.is_super_deal,
        super_deal_score=candidate.super_deal_score,
    )


def trend_to_dict(trend: Optional[TrendInfo]) -> Optional[dict[str, Any]]:
    if trend is None:
        return None
    return {
        "direction": trend.direction,
        "score": trend.score,
        "confidence": trend.confidence,
        "timeframe": trend.timeframe,
        "reasons": list(trend.reasons),
        "close": trend.close,
    }


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


def super_deal_probability(candidate: SignalCandidate) -> int:
    """Итоговая проходимость для супер-сделки: 97-99% при максимальных условиях."""
    probability = max(SUPER_DEAL_MIN_PROBABILITY, candidate.probability + 2)
    trend_abs = abs(candidate.trend.score) if candidate.trend else 0
    if trend_abs >= SUPER_DEAL_TREND_SCORE_ABS:
        probability += 1
    ai = candidate.ai_optimizer or {}
    try:
        if float(ai.get("win_rate") or 0) >= 0.65 and float(ai.get("profit_factor") or 0) >= 1.4:
            probability += 1
    except Exception:
        pass
    return max(SUPER_DEAL_MIN_PROBABILITY, min(99, int(probability)))


def is_super_deal_candidate(candidate: Optional[SignalCandidate]) -> bool:
    if candidate is None:
        return False
    if candidate.probability < SUPER_DEAL_RAW_PROBABILITY_MIN:
        return False
    trend = candidate.trend
    if trend is None:
        return False
    side = candidate.side.upper()
    direction = trend.direction.upper()
    if side == "LONG":
        return direction == "BULL" and trend.score >= SUPER_DEAL_TREND_SCORE_ABS
    if side == "SHORT":
        return direction == "BEAR" and trend.score <= -SUPER_DEAL_TREND_SCORE_ABS
    return False


def apply_super_deal_filter(candidate: Optional[SignalCandidate]) -> Optional[SignalCandidate]:
    if candidate is None or not SUPER_DEAL_ENABLED:
        return candidate
    if not is_super_deal_candidate(candidate):
        return None

    probability = super_deal_probability(candidate)
    trend_score = candidate.trend.score if candidate.trend else 0
    super_reason = (
        f"🔴 Супер сделка: проходимость {probability}%, "
        f"тренд score {trend_score:+d}, условия максимальные"
    )
    return SignalCandidate(
        symbol=candidate.symbol,
        side=candidate.side,
        probability=probability,
        entry=candidate.entry,
        stop=candidate.stop,
        take_profits=list(candidate.take_profits),
        reasons=(candidate.reasons + [super_reason])[:9],
        timeframe=candidate.timeframe,
        trend=candidate.trend,
        ai_optimizer=candidate.ai_optimizer,
        is_super_deal=True,
        super_deal_score=trend_score,
    )


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
                signal_candles = await fetch_klines(session, symbol, SIGNAL_TIMEFRAME, KLINES_LIMIT)
                trend_candles = None
                if signal_candles and (TREND_FILTER_ENABLED or SUPER_DEAL_ENABLED):
                    if TREND_TIMEFRAME == SIGNAL_TIMEFRAME:
                        trend_candles = signal_candles
                    else:
                        if REQUEST_DELAY_SECONDS > 0:
                            await asyncio.sleep(REQUEST_DELAY_SECONDS)
                        trend_candles = await fetch_klines(session, symbol, TREND_TIMEFRAME, KLINES_LIMIT)
                if REQUEST_DELAY_SECONDS > 0:
                    await asyncio.sleep(REQUEST_DELAY_SECONDS)
                return signal_candles, trend_candles

        tasks = [limited_fetch(symbol) for symbol in symbols_to_scan]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    for symbol, response in zip(symbols_to_scan, results):
        if isinstance(response, Exception) or response is None:
            result.failed_symbols += 1
            result.skipped_symbols.append(symbol)
            continue

        signal_candles, trend_candles = response
        if signal_candles is None:
            result.failed_symbols += 1
            result.skipped_symbols.append(symbol)
            continue

        result.successful_symbols += 1
        candidate = analyze_candles(symbol, signal_candles)
        if candidate and (TREND_FILTER_ENABLED or SUPER_DEAL_ENABLED):
            trend = analyze_primary_trend(trend_candles, TREND_TIMEFRAME)
            filtered = apply_trend_filter(candidate, trend)
            if filtered is None:
                if trend is None or trend.direction in {"UNKNOWN", "FLAT"}:
                    result.trend_unknown += 1
                else:
                    result.trend_blocked += 1
                candidate = None
            else:
                result.trend_passed += 1
                candidate = filtered
        if candidate:
            before_neural = candidate
            candidate = apply_neural_optimizer(candidate, signal_candles)
            if NEURAL_OPTIMIZER_ENABLED:
                if candidate is None:
                    result.neural_blocked += 1
                elif candidate is not before_neural:
                    result.neural_passed += 1
        if candidate:
            candidate = apply_smart_algorithm(candidate)
        if candidate:
            before_improvements = candidate
            candidate = apply_trading_improvements_filters(candidate, signal_candles)
            if TRADING_IMPROVEMENTS_ENABLED:
                if candidate is None:
                    result.improvements_blocked += 1
                elif candidate is not before_improvements:
                    result.improvements_passed += 1
        if candidate:
            candidate = apply_super_deal_filter(candidate)
            if SUPER_DEAL_ENABLED:
                if candidate is None:
                    result.super_deal_blocked += 1
                else:
                    result.super_deal_passed += 1
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
        super_deal=candidate.is_super_deal,
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
        if trade.get("improvements_enabled") and PARTIAL_TP_ENABLED and len(tps) >= 3:
            return float(tps[2])
        tp_index = int(trade.get("tp_index", AUTO_CLOSE_TP_INDEX))
        if 1 <= tp_index <= len(tps):
            return float(tps[tp_index - 1])
    except Exception:
        return None
    return None


def trade_exit_reason(trade: dict[str, Any], last_price: float) -> Optional[str]:
    side = str(trade.get("side", "")).upper()
    stop = float(trade.get("current_stop") or trade.get("stop", 0))
    target = trade_target_price(trade)
    if side == "LONG":
        if last_price <= stop:
            return "SL" if abs(stop - float(trade.get("stop", stop))) < 1e-12 else "BREAKEVEN_SL"
        if target is not None and last_price >= target:
            if trade.get("improvements_enabled") and PARTIAL_TP_ENABLED:
                return "TP3"
            return f"TP{int(trade.get('tp_index', AUTO_CLOSE_TP_INDEX))}"
    if side == "SHORT":
        if last_price >= stop:
            return "SL" if abs(stop - float(trade.get("stop", stop))) < 1e-12 else "BREAKEVEN_SL"
        if target is not None and last_price <= target:
            if trade.get("improvements_enabled") and PARTIAL_TP_ENABLED:
                return "TP3"
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


def order_id_from_response(order: Any) -> str:
    if isinstance(order, dict):
        value = order.get("id") or order.get("orderId") or order.get("clientOrderId")
        if value:
            return str(value)
    return ""


def order_average_price(order: Any, fallback: float) -> float:
    if not isinstance(order, dict):
        return fallback
    for key in ("average", "avgPrice", "price", "lastTradeTimestamp"):
        try:
            value = order.get(key)
            if value is not None and float(value) > 0 and key != "lastTradeTimestamp":
                return float(value)
        except Exception:
            continue
    return fallback


def safe_amount(value: Any) -> float:
    try:
        return abs(float(value or 0))
    except Exception:
        return 0.0


def position_amount(position: dict[str, Any]) -> float:
    for key in ("contracts", "contractSize", "size", "positionAmt", "positionAmount"):
        amount = safe_amount(position.get(key))
        if amount > 0:
            return amount
    info = position.get("info") if isinstance(position.get("info"), dict) else {}
    for key in ("positionAmt", "positionAmount", "holdVol", "volume", "availableVolume"):
        amount = safe_amount(info.get(key))
        if amount > 0:
            return amount
    return 0.0


def position_side(position: dict[str, Any]) -> str:
    raw = str(position.get("side") or position.get("positionSide") or "").lower()
    info = position.get("info") if isinstance(position.get("info"), dict) else {}
    raw_info = str(info.get("side") or info.get("positionSide") or info.get("holdSide") or "").lower()
    value = raw or raw_info
    if "short" in value or value in {"sell", "bear"}:
        return "SHORT"
    if "long" in value or value in {"buy", "bull"}:
        return "LONG"
    try:
        signed = float(position.get("contracts") or position.get("size") or info.get("positionAmt") or 0)
        if signed < 0:
            return "SHORT"
        if signed > 0:
            return "LONG"
    except Exception:
        pass
    return ""


def trade_close_side(trade: dict[str, Any]) -> str:
    return "sell" if str(trade.get("side", "")).upper() == "LONG" else "buy"


async def execute_live_order(exchange_name: str, symbol: str, side: str, amount: float, reduce_only: bool = False) -> dict[str, Any]:
    exchange = create_ccxt_exchange(exchange_name)
    try:
        market_symbol = await find_ccxt_market_symbol(exchange, symbol)
        amount_precise = float(exchange.amount_to_precision(market_symbol, amount))
        if amount_precise <= 0:
            raise ValueError("Размер ордера получился 0 после округления биржи")
        order_side = side.lower()
        params = {"reduceOnly": bool(reduce_only)} if reduce_only else {}
        order = await exchange.create_order(market_symbol, "market", order_side, amount_precise, None, params)
        return {
            "market_symbol": market_symbol,
            "amount": amount_precise,
            "order": order,
            "average_price": order_average_price(order, 0),
        }
    finally:
        try:
            await exchange.close()
        except Exception:
            pass


async def fetch_live_last_price(exchange_name: str, symbol: str, known_market_symbol: str = "") -> Optional[float]:
    exchange = create_ccxt_exchange(exchange_name)
    try:
        market_symbol = known_market_symbol or await find_ccxt_market_symbol(exchange, symbol)
        ticker = await exchange.fetch_ticker(market_symbol)
        if isinstance(ticker, dict):
            info = ticker.get("info") if isinstance(ticker.get("info"), dict) else {}
            for key in ("mark", "markPrice", "last", "close", "bid", "ask", "indexPrice"):
                try:
                    value = ticker.get(key)
                    if value is None:
                        value = info.get(key)
                    if value is not None and float(value) > 0:
                        return float(value)
                except Exception:
                    continue
    finally:
        try:
            await exchange.close()
        except Exception:
            pass
    return None




async def fetch_account_equity_usdt(exchange_name: str) -> float:
    if AUTO_TRADE_MODE != "live" or not has_api_keys(exchange_name):
        return ACCOUNT_EQUITY_USDT
    exchange = create_ccxt_exchange(exchange_name)
    try:
        balance = await exchange.fetch_balance()
        if isinstance(balance, dict):
            usdt = balance.get("USDT")
            if isinstance(usdt, dict):
                for key in ("total", "free", "used"):
                    try:
                        value = usdt.get(key)
                        if value is not None and float(value) > 0:
                            return float(value)
                    except Exception:
                        pass
            total = balance.get("total")
            if isinstance(total, dict):
                try:
                    value = total.get("USDT")
                    if value is not None and float(value) > 0:
                        return float(value)
                except Exception:
                    pass
    except Exception:
        logging.exception("Не удалось получить баланс, использую ACCOUNT_EQUITY_USDT")
    finally:
        try:
            await exchange.close()
        except Exception:
            pass
    return ACCOUNT_EQUITY_USDT


def improved_position_notional(candidate: SignalCandidate, equity_usdt: float, regime: Optional[str] = None) -> float:
    risk_pct_to_stop = abs(float(candidate.entry) - float(candidate.stop)) / max(float(candidate.entry), 1e-12) * 100.0
    if risk_pct_to_stop <= 0:
        return min(TRADE_MARGIN_USDT, MAX_POSITION_NOTIONAL_USDT)
    risk_usdt = float(equity_usdt) * RISK_PER_TRADE_PERCENT / 100.0
    notional = risk_usdt / (risk_pct_to_stop / 100.0)
    cap = min(MAX_POSITION_NOTIONAL_USDT, max(1.0, TRADE_MARGIN_USDT))
    notional = min(max(1.0, notional), cap)
    if regime == "HIGH_VOL":
        notional *= HIGH_VOL_POSITION_FACTOR
    return max(1.0, notional)


async def fetch_live_market_quality(exchange_name: str, symbol: str, known_market_symbol: str = "") -> tuple[bool, str, dict[str, Any]]:
    if not trading_improvements_active() or not LIQUIDITY_FILTER_ENABLED:
        return True, "disabled", {}
    exchange = create_ccxt_exchange(exchange_name)
    try:
        market_symbol = known_market_symbol or await find_ccxt_market_symbol(exchange, symbol)
        ticker = await exchange.fetch_ticker(market_symbol)
        bid = ask = last = None
        quote_volume = None
        if isinstance(ticker, dict):
            info = ticker.get("info") if isinstance(ticker.get("info"), dict) else {}
            for key in ("bid", "bidPrice", "bestBid"):
                try:
                    value = ticker.get(key) if ticker.get(key) is not None else info.get(key)
                    if value is not None:
                        bid = float(value)
                        break
                except Exception:
                    pass
            for key in ("ask", "askPrice", "bestAsk"):
                try:
                    value = ticker.get(key) if ticker.get(key) is not None else info.get(key)
                    if value is not None:
                        ask = float(value)
                        break
                except Exception:
                    pass
            for key in ("last", "close", "mark", "markPrice"):
                try:
                    value = ticker.get(key) if ticker.get(key) is not None else info.get(key)
                    if value is not None:
                        last = float(value)
                        break
                except Exception:
                    pass
            for key in ("quoteVolume", "quoteVol", "amount", "turnover", "amount24"):
                try:
                    value = ticker.get(key) if ticker.get(key) is not None else info.get(key)
                    if value is not None:
                        quote_volume = float(value)
                        break
                except Exception:
                    pass

        if (not bid or not ask) and last:
            try:
                orderbook = await exchange.fetch_order_book(market_symbol, limit=5)
                bids = orderbook.get("bids") or []
                asks = orderbook.get("asks") or []
                if bids:
                    bid = float(bids[0][0])
                if asks:
                    ask = float(asks[0][0])
            except Exception:
                logging.debug("Order book unavailable for %s", market_symbol)

        meta = {"bid": bid, "ask": ask, "last": last, "quote_volume": quote_volume}
        if bid and ask and bid > 0 and ask > 0:
            mid = (bid + ask) / 2.0
            spread_pct = (ask - bid) / mid * 100.0
            meta["spread_pct"] = spread_pct
            if spread_pct > MAX_SPREAD_PCT:
                return False, f"спред {spread_pct:.3f}% выше лимита {MAX_SPREAD_PCT:g}%", meta
        if quote_volume is not None and quote_volume > 0 and quote_volume < MIN_24H_QUOTE_VOLUME_USDT:
            return False, f"24h объём {quote_volume:.0f} USDT ниже лимита {MIN_24H_QUOTE_VOLUME_USDT:.0f}", meta
        return True, "ok", meta
    except Exception as exc:
        logging.exception("Не удалось проверить спред/ликвидность")
        return False, f"ошибка проверки спреда/ликвидности: {exc}", {}
    finally:
        try:
            await exchange.close()
        except Exception:
            pass


async def create_reduce_only_trigger_order(
    exchange,
    market_symbol: str,
    purpose: str,
    close_side: str,
    amount: float,
    trigger_price: float,
) -> dict[str, Any]:
    trigger_price_precise = float(exchange.price_to_precision(market_symbol, trigger_price))
    amount_precise = float(exchange.amount_to_precision(market_symbol, amount))
    if amount_precise <= 0:
        raise ValueError("Размер защитного ордера получился 0 после округления биржи")

    # CCXT унифицирует stopLossPrice/takeProfitPrice для условных SL/TP ордеров.
    # Если биржа не поддерживает конкретный вариант, пробуем следующий формат.
    if purpose == "sl":
        attempts = [
            {"reduceOnly": True, "stopLossPrice": trigger_price_precise},
            {"reduceOnly": True, "triggerPrice": trigger_price_precise, "stopLossPrice": trigger_price_precise},
            {"reduceOnly": True, "stopPrice": trigger_price_precise, "triggerPrice": trigger_price_precise},
        ]
    else:
        attempts = [
            {"reduceOnly": True, "takeProfitPrice": trigger_price_precise},
            {"reduceOnly": True, "triggerPrice": trigger_price_precise, "takeProfitPrice": trigger_price_precise},
            {"reduceOnly": True, "stopPrice": trigger_price_precise, "triggerPrice": trigger_price_precise},
        ]

    last_error: Optional[Exception] = None
    for params in attempts:
        try:
            order = await exchange.create_order(market_symbol, "market", close_side, amount_precise, None, params)
            if isinstance(order, dict):
                status = str(order.get("status") or "").lower()
                filled = safe_amount(order.get("filled"))
                if status in {"closed", "filled"} or filled >= amount_precise * 0.5:
                    raise RuntimeError(
                        "Защитный ордер исполнился сразу. Биржа могла не распознать trigger-параметры; "
                        "проверь позицию вручную."
                    )
                return order
            return {"raw": order}
        except Exception as exc:
            last_error = exc
            logging.warning("Не удалось поставить %s защитный ордер %s params=%s: %s", purpose, market_symbol, params, exc)
    raise RuntimeError(f"Не удалось поставить защитный {purpose.upper()} ордер: {last_error}")


async def place_exchange_protective_orders(exchange_name: str, trade: dict[str, Any]) -> dict[str, Any]:
    if not USE_EXCHANGE_PROTECTIVE_ORDERS:
        return {"mode": "disabled", "error": "USE_EXCHANGE_PROTECTIVE_ORDERS=false"}

    target = trade_target_price(trade)
    if target is None:
        return {"mode": "missing_target", "error": "TP target is missing"}

    exchange = create_ccxt_exchange(exchange_name)
    protective: dict[str, Any] = {
        "mode": "separate_reduce_only_conditional",
        "created_at": time.time(),
        "sl_price": float(trade.get("current_stop") or trade.get("stop")),
        "tp_price": float(target),
    }
    try:
        market_symbol = str(trade.get("ccxt_symbol") or await find_ccxt_market_symbol(exchange, str(trade.get("symbol"))))
        amount = float(trade.get("remaining_amount") or trade.get("amount", 0))
        close_side = trade_close_side(trade)

        sl_order = await create_reduce_only_trigger_order(exchange, market_symbol, "sl", close_side, amount, float(trade.get("current_stop") or trade.get("stop")))
        protective["sl_order_id"] = order_id_from_response(sl_order)
        protective["sl_order"] = sl_order

        tp_order = await create_reduce_only_trigger_order(exchange, market_symbol, "tp", close_side, amount, float(target))
        protective["tp_order_id"] = order_id_from_response(tp_order)
        protective["tp_order"] = tp_order
        protective["ok"] = True
        return protective
    except Exception as exc:
        logging.exception("Не удалось поставить биржевые защитные ордера")
        protective["ok"] = False
        protective["error"] = str(exc)
        return protective
    finally:
        try:
            await exchange.close()
        except Exception:
            pass


async def cancel_protective_orders(trade: dict[str, Any]) -> list[str]:
    if not CANCEL_PROTECTIVE_ORDERS_ON_CLOSE:
        return []
    protective = trade.get("protective_orders") or {}
    order_ids = [
        str(protective.get("sl_order_id") or "").strip(),
        str(protective.get("tp_order_id") or "").strip(),
    ]
    order_ids = [order_id for order_id in order_ids if order_id]
    if not order_ids:
        return []

    cancelled: list[str] = []
    exchange = create_ccxt_exchange(str(trade.get("exchange")))
    try:
        market_symbol = str(trade.get("ccxt_symbol") or await find_ccxt_market_symbol(exchange, str(trade.get("symbol"))))
        for order_id in order_ids:
            for params in ({"trigger": True}, {"stop": True}, {"tpsl": True}, {}):
                try:
                    await exchange.cancel_order(order_id, market_symbol, params)
                    cancelled.append(order_id)
                    break
                except Exception as exc:
                    logging.debug("Не удалось отменить защитный ордер %s params=%s: %s", order_id, params, exc)
            await asyncio.sleep(0.05)
    finally:
        try:
            await exchange.close()
        except Exception:
            pass
    return cancelled


async def fetch_live_position_for_trade(trade: dict[str, Any]) -> Optional[dict[str, Any]]:
    exchange = create_ccxt_exchange(str(trade.get("exchange")))
    try:
        market_symbol = str(trade.get("ccxt_symbol") or await find_ccxt_market_symbol(exchange, str(trade.get("symbol"))))
        positions: list[Any] = []
        try:
            positions = await exchange.fetch_positions([market_symbol])
        except Exception:
            positions = await exchange.fetch_positions()
        expected_side = str(trade.get("side", "")).upper()
        for position in positions or []:
            if not isinstance(position, dict):
                continue
            pos_symbol = str(position.get("symbol") or "")
            info = position.get("info") if isinstance(position.get("info"), dict) else {}
            raw_symbol = str(info.get("symbol") or info.get("contract") or "")
            if market_symbol not in {pos_symbol, raw_symbol} and compact_symbol(raw_symbol) != compact_symbol(str(trade.get("symbol"))):
                continue
            amount = position_amount(position)
            if amount <= 0:
                continue
            side = position_side(position)
            if side and expected_side and side != expected_side:
                continue
            return position
    finally:
        try:
            await exchange.close()
        except Exception:
            pass
    return None


async def ensure_live_protection(bot: Bot, trade: dict[str, Any]) -> None:
    if str(trade.get("mode")) != "live" or str(trade.get("status")) != "open":
        return
    protective = trade.get("protective_orders") or {}
    if protective.get("ok") and (protective.get("sl_order_id") or protective.get("tp_order_id")):
        return

    new_protective = await place_exchange_protective_orders(str(trade.get("exchange")), trade)
    async with TRADES_LOCK:
        trades = load_trades()
        for item in trades:
            if item.get("id") == trade.get("id") and item.get("status") == "open":
                item["protective_orders"] = new_protective
                item["protection_checked_at"] = time.time()
                break
        save_trades(trades)

    if new_protective.get("ok"):
        await broadcast_to_admins(
            bot,
            "🛡 <b>Защитные ордера выставлены</b>\n"
            f"Пара: <b>{html.escape(str(trade.get('display_symbol') or trade.get('symbol')))}</b>\n"
            f"SL: <b>{html.escape(fmt_price(float(new_protective.get('sl_price', 0))))}</b>\n"
            f"TP: <b>{html.escape(fmt_price(float(new_protective.get('tp_price', 0))))}</b>",
        )
    else:
        await broadcast_to_admins(
            bot,
            "⚠️ <b>Биржевые защитные ордера не выставлены</b>\n"
            f"Пара: <b>{html.escape(str(trade.get('display_symbol') or trade.get('symbol')))}</b>\n"
            "Бот оставил fallback-мониторинг по ticker/mark price. Проверь API и биржу вручную.\n"
            f"Ошибка: <code>{html.escape(str(new_protective.get('error', 'unknown')))}</code>",
        )


async def open_autotrade_for_signal(bot: Bot, candidate: SignalCandidate) -> Optional[dict[str, Any]]:
    if not autotrading_is_enabled():
        return None

    exchange_value = MARKET_DATA_PROVIDER
    symbol = candidate.symbol

    async with TRADES_LOCK:
        open_trades = [t for t in load_trades() if t.get("status") == "open"]
        compact = compact_symbol(symbol)
        for trade in open_trades:
            if compact_symbol(str(trade.get("symbol", ""))) == compact and str(trade.get("exchange", "")).lower() == exchange_value:
                await broadcast_to_admins(bot, f"ℹ️ Автоторговля: по {html.escape(display_symbol(symbol))} уже есть открытая сделка, новую не открываю.")
                return None
        if len(open_trades) >= MAX_ACTIVE_TRADES:
            await broadcast_to_admins(bot, f"⛔️ Автоторговля: лимит открытых сделок {MAX_ACTIVE_TRADES}, новую не открываю.")
            return None

    if AUTO_TRADE_MODE == "live" and not has_api_keys(exchange_value):
        await broadcast_to_admins(
            bot,
            "⛔️ LIVE-автоторговля не открыла сделку: нет API ключей для текущей биржи.\n"
            "Добавь ключи в Railway Variables или переключи режим в PAPER."
        )
        return None

    if TRADING_IMPROVEMENTS_ENABLED:
        block_reason = loss_limit_block_reason()
        if block_reason:
            increment_improvement_counter("blocked_trade_loss_limits")
            await broadcast_to_admins(bot, f"⛔️ Улучшения торговли: сделка не открыта — {html.escape(block_reason)}.")
            return None
        block_reason = exposure_block_reason(candidate)
        if block_reason:
            increment_improvement_counter("blocked_trade_exposure")
            await broadcast_to_admins(bot, f"⛔️ Улучшения торговли: сделка не открыта — {html.escape(block_reason)}.")
            return None
        if AUTO_TRADE_MODE == "live":
            ok_quality, quality_reason, quality_meta = await fetch_live_market_quality(exchange_value, symbol)
            if not ok_quality:
                increment_improvement_counter("blocked_trade_spread_liquidity")
                await broadcast_to_admins(bot, f"⛔️ LIVE-сделка отменена: {html.escape(quality_reason)}.")
                return None

    # В обычном режиме TRADE_MARGIN_USDT трактуется как максимальный USDT-объём позиции.
    # При включённых улучшениях объём рассчитывается риск-движком по расстоянию до стопа.
    equity_usdt = ACCOUNT_EQUITY_USDT
    notional_usdt = float(TRADE_MARGIN_USDT)
    if TRADING_IMPROVEMENTS_ENABLED:
        equity_usdt = await fetch_account_equity_usdt(exchange_value)
        regime = None
        try:
            regime = str((candidate.ai_optimizer or {}).get("market_regime") or "")
        except Exception:
            regime = None
        notional_usdt = improved_position_notional(candidate, equity_usdt, regime)
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
        "trend_filter_enabled": TREND_FILTER_ENABLED,
        "trend": trend_to_dict(candidate.trend),
        "neural_optimizer_enabled": NEURAL_OPTIMIZER_ENABLED,
        "ai_optimizer": candidate.ai_optimizer,
        "is_super_deal": candidate.is_super_deal,
        "super_deal_score": candidate.super_deal_score,
        "entry": candidate.entry,
        "stop": candidate.stop,
        "take_profits": candidate.take_profits,
        "tp_index": AUTO_CLOSE_TP_INDEX,
        "improvements_enabled": TRADING_IMPROVEMENTS_ENABLED,
        "risk_per_trade_percent": RISK_PER_TRADE_PERCENT if TRADING_IMPROVEMENTS_ENABLED else None,
        "account_equity_usdt": equity_usdt if TRADING_IMPROVEMENTS_ENABLED else None,
        "initial_notional_usdt": notional_usdt,
        "notional_usdt": notional_usdt,
        "amount": amount,
        "remaining_amount": amount,
        "partial_tp_hits": [],
        "current_stop": candidate.stop,
        "opened_at": time.time(),
        "open_order_id": "paper",
        "protective_orders": {},
        "last_synced_at": None,
    }

    try:
        if AUTO_TRADE_MODE == "live":
            order_side = "buy" if candidate.side == "LONG" else "sell"
            result = await execute_live_order(exchange_value, symbol, order_side, amount, reduce_only=False)
            trade["amount"] = result["amount"]
            trade["ccxt_symbol"] = result["market_symbol"]
            trade["open_order_id"] = str((result.get("order") or {}).get("id") or "live")
            avg = float(result.get("average_price") or 0)
            if avg > 0:
                trade["entry"] = avg

            # Сразу после входа ставим биржевые reduce-only conditional SL/TP.
            trade["protective_orders"] = await place_exchange_protective_orders(exchange_value, trade)
            if (
                TRADING_IMPROVEMENTS_ENABLED
                and STRICT_PROTECTION_CHECK_ENABLED
                and not (trade.get("protective_orders") or {}).get("ok")
            ):
                # В режиме улучшений LIVE без подтверждённых защитных ордеров сразу закрывается.
                try:
                    close_side = trade_close_side(trade)
                    close_result = await execute_live_order(exchange_value, symbol, close_side, float(trade.get("remaining_amount") or trade.get("amount", 0)), reduce_only=True)
                    avg_close = float(close_result.get("average_price") or trade.get("entry") or candidate.entry)
                except Exception:
                    avg_close = float(trade.get("entry") or candidate.entry)
                trade["status"] = "closed"
                trade["closed_at"] = time.time()
                trade["close_reason"] = "NO_EXCHANGE_PROTECTION"
                trade["close_price"] = avg_close
                trade["pnl_pct"] = pct_from_entry(avg_close, float(trade.get("entry") or candidate.entry)) * (1 if candidate.side == "LONG" else -1)
                async with TRADES_LOCK:
                    trades = load_trades()
                    trades.append(trade)
                    save_trades(trades)
                increment_improvement_counter("closed_no_protection")
                await broadcast_to_admins(
                    bot,
                    "🛡⛔️ <b>LIVE-сделка закрыта аварийно</b>\n"
                    f"Пара: <b>{html.escape(display_symbol(symbol))}</b>\n"
                    "Причина: не удалось подтвердить биржевые SL/TP в режиме «Улучшения торговли».\n"
                    f"Ошибка защиты: <code>{html.escape(str((trade.get('protective_orders') or {}).get('error', 'unknown')))}</code>"
                )
                return trade

        async with TRADES_LOCK:
            trades = load_trades()
            trades.append(trade)
            save_trades(trades)

        protection_note = ""
        protective = trade.get("protective_orders") or {}
        if AUTO_TRADE_MODE == "live":
            if protective.get("ok"):
                protection_note = "\nЗащитные ордера: <b>выставлены на бирже</b>"
            else:
                protection_note = "\nЗащитные ордера: <b>fallback-мониторинг</b> — проверь биржу вручную"

        await broadcast_to_admins(
            bot,
            "💰 <b>Авто-сделка открыта</b>\n"
            f"ID: <code>{html.escape(trade_id)}</code>\n"
            f"Режим: <b>{html.escape(AUTO_TRADE_MODE.upper())}</b>\n"
            f"Биржа: <b>{html.escape(exchange_label(exchange_value))}</b>\n"
            f"Пара: <b>{html.escape(display_symbol(symbol))}</b>\n"
            f"Сторона: <b>{candidate.side}</b>\n"
            f"Объём позиции: <b>${notional_usdt:g}</b>\n"
            + (f"Риск-движок: <b>{RISK_PER_TRADE_PERCENT:g}% от ${equity_usdt:g}</b>\n" if TRADING_IMPROVEMENTS_ENABLED else "")
            + f"Amount: <b>{html.escape(str(trade['amount']))}</b>\n"
            f"Закрытие: <b>SL или TP{AUTO_CLOSE_TP_INDEX}</b>"
            + (f"\nТренд-фильтр: <b>{html.escape(trend_filter_label())}</b>" if TREND_FILTER_ENABLED else "")
            + (f"\nУлучшения торговли: <b>{html.escape(trading_improvements_label())}</b>" if TRADING_IMPROVEMENTS_ENABLED else "")
            + f"{protection_note}"
        )
        return trade
    except Exception as exc:
        logging.exception("Не удалось открыть авто-сделку")
        await broadcast_to_admins(bot, f"⚠️ Не удалось открыть авто-сделку: <code>{html.escape(str(exc))}</code>")
        return None


async def mark_trade_closed(
    bot: Bot,
    trade: dict[str, Any],
    reason: str,
    close_price: float,
    pnl_pct: Optional[float],
    extra: str = "",
) -> bool:
    now = time.time()
    async with TRADES_LOCK:
        trades = load_trades()
        updated = False
        for item in trades:
            if item.get("id") == trade.get("id") and item.get("status") in {"open", "closing"}:
                item["status"] = "closed"
                item["closed_at"] = now
                item["close_reason"] = reason
                item["close_price"] = close_price
                if pnl_pct is not None:
                    item["pnl_pct"] = pnl_pct
                item["last_synced_at"] = now
                updated = True
                break
        if updated:
            save_trades(trades)
    if not updated:
        return False

    pnl_text = "n/a" if pnl_pct is None else fmt_pct(pnl_pct)
    await broadcast_to_admins(
        bot,
        "✅ <b>Авто-сделка закрыта</b>\n"
        f"Пара: <b>{html.escape(str(trade.get('display_symbol') or trade.get('symbol')))}</b>\n"
        f"Сторона: <b>{html.escape(str(trade.get('side')))}</b>\n"
        f"Причина: <b>{html.escape(reason)}</b>\n"
        f"Цена закрытия: <b>{html.escape(fmt_price(close_price))}</b>\n"
        f"PnL примерно: <b>{html.escape(pnl_text)}</b>"
        f"{extra}"
    )
    return True


async def close_autotrade(bot: Bot, trade: dict[str, Any], reason: str, last_price: float) -> bool:
    async with TRADES_LOCK:
        trades = load_trades()
        found = False
        for item in trades:
            if item.get("id") == trade.get("id") and item.get("status") == "open":
                item["status"] = "closing"
                item["closing_reason"] = reason
                item["closing_started_at"] = time.time()
                found = True
                trade = dict(item)
                break
        if found:
            save_trades(trades)
    if not found:
        return False

    try:
        close_price = float(last_price)
        if str(trade.get("mode")) == "live":
            try:
                await cancel_protective_orders(trade)
            except Exception:
                logging.exception("Не удалось отменить защитные ордера перед закрытием")

            close_side = trade_close_side(trade)
            try:
                result = await execute_live_order(
                    str(trade.get("exchange")),
                    str(trade.get("symbol")),
                    close_side,
                    float(trade.get("remaining_amount") or trade.get("amount", 0)),
                    reduce_only=True,
                )
                if float(result.get("average_price") or 0) > 0:
                    close_price = float(result.get("average_price"))
            except Exception as exc:
                # Если биржа уже закрыла позицию защитным ордером или вручную, не открываем новую.
                position = await fetch_live_position_for_trade(trade)
                if position is not None:
                    raise exc
                fetched_price = await fetch_live_last_price(str(trade.get("exchange")), str(trade.get("symbol")), str(trade.get("ccxt_symbol") or ""))
                if fetched_price:
                    close_price = fetched_price
                reason = reason + "+SYNC_NO_POSITION"

        pnl_pct = pct_from_entry(close_price, float(trade.get("entry", close_price)))
        if str(trade.get("side")).upper() == "SHORT":
            pnl_pct = -pnl_pct
        return await mark_trade_closed(bot, trade, reason, close_price, pnl_pct)
    except Exception as exc:
        logging.exception("Не удалось закрыть авто-сделку")
        async with TRADES_LOCK:
            trades = load_trades()
            for item in trades:
                if item.get("id") == trade.get("id") and item.get("status") == "closing":
                    item["status"] = "open"
                    item["last_close_error"] = str(exc)
                    item["last_close_error_at"] = time.time()
                    break
            save_trades(trades)
        await broadcast_to_admins(bot, f"⚠️ Не удалось закрыть авто-сделку {html.escape(str(trade.get('symbol')))}: <code>{html.escape(str(exc))}</code>")
        return False




def tp_reached(side: str, price: float, target: float) -> bool:
    if side.upper() == "LONG":
        return price >= target
    return price <= target


def stop_after_tp(side: str, entry: float, target: Optional[float] = None) -> float:
    if target is not None:
        return float(target)
    offset = entry * BREAKEVEN_OFFSET_PCT / 100.0
    if side.upper() == "LONG":
        return entry + offset
    return max(entry - offset, entry * 0.0001)


async def execute_partial_close(bot: Bot, trade: dict[str, Any], tp_number: int, close_percent: float, last_price: float) -> bool:
    side = str(trade.get("side", "")).upper()
    remaining = float(trade.get("remaining_amount") or trade.get("amount") or 0)
    initial_amount = float(trade.get("amount") or remaining)
    if remaining <= 0 or initial_amount <= 0:
        return False

    amount_to_close = min(remaining, initial_amount * close_percent / 100.0)
    if amount_to_close <= 0:
        return False

    close_price = float(last_price)
    if str(trade.get("mode")) == "live":
        try:
            result = await execute_live_order(
                str(trade.get("exchange")),
                str(trade.get("symbol")),
                trade_close_side(trade),
                amount_to_close,
                reduce_only=True,
            )
            if float(result.get("average_price") or 0) > 0:
                close_price = float(result.get("average_price"))
        except Exception as exc:
            logging.exception("Ошибка частичного закрытия")
            await broadcast_to_admins(bot, f"⚠️ Не удалось частично закрыть TP{tp_number}: <code>{html.escape(str(exc))}</code>")
            return False

    entry = float(trade.get("entry") or close_price)
    pnl_pct = pct_from_entry(close_price, entry)
    if side == "SHORT":
        pnl_pct = -pnl_pct
    closed_notional = float(trade.get("initial_notional_usdt") or trade.get("notional_usdt") or 0) * (amount_to_close / initial_amount)
    realized_pnl_usdt = closed_notional * pnl_pct / 100.0
    new_remaining = max(0.0, remaining - amount_to_close)

    async with TRADES_LOCK:
        trades = load_trades()
        updated_trade: Optional[dict[str, Any]] = None
        for item in trades:
            if item.get("id") == trade.get("id") and item.get("status") == "open":
                hits = list(item.get("partial_tp_hits") or [])
                if tp_number not in hits:
                    hits.append(tp_number)
                item["partial_tp_hits"] = sorted(hits)
                item["remaining_amount"] = new_remaining
                item["realized_pnl_usdt"] = float(item.get("realized_pnl_usdt") or 0) + realized_pnl_usdt
                item["last_partial_close_at"] = time.time()
                item["last_partial_close_price"] = close_price
                item["last_partial_close_tp"] = tp_number
                if MOVE_SL_TO_BREAKEVEN_AFTER_TP1 and tp_number == 1:
                    item["current_stop"] = stop_after_tp(side, entry)
                    item["breakeven_moved_at"] = time.time()
                elif tp_number == 2:
                    tps = item.get("take_profits") or []
                    item["current_stop"] = stop_after_tp(side, entry, float(tps[0]) if tps else None)
                updated_trade = dict(item)
                break
        save_trades(trades)

    if updated_trade and str(updated_trade.get("mode")) == "live":
        try:
            await cancel_protective_orders(trade)
            if new_remaining > 0:
                updated_trade["protective_orders"] = await place_exchange_protective_orders(str(updated_trade.get("exchange")), updated_trade)
                async with TRADES_LOCK:
                    trades = load_trades()
                    for item in trades:
                        if item.get("id") == updated_trade.get("id") and item.get("status") == "open":
                            item["protective_orders"] = updated_trade.get("protective_orders") or {}
                            break
                    save_trades(trades)
        except Exception:
            logging.exception("Не удалось переставить защиту после частичного TP")

    await broadcast_to_admins(
        bot,
        f"🎯 <b>Частичный TP{tp_number}</b>\n"
        f"Пара: <b>{html.escape(str(trade.get('display_symbol') or trade.get('symbol')))}</b>\n"
        f"Закрыто: <b>{close_percent:g}%</b> позиции\n"
        f"Цена: <b>{html.escape(fmt_price(close_price))}</b>\n"
        f"Остаток amount: <b>{new_remaining:g}</b>\n"
        f"PnL части: <b>{html.escape(fmt_pct(pnl_pct))}</b>"
        + (" \n🛡 Стоп перенесён в безубыток/прибыль." if tp_number in {1, 2} else "")
    )
    increment_improvement_counter(f"partial_tp{tp_number}")
    return True


async def manage_partial_take_profits(bot: Bot, trade: dict[str, Any], last_price: float) -> bool:
    if not (trade.get("improvements_enabled") and PARTIAL_TP_ENABLED and trading_improvements_active()):
        return False
    tps = [float(x) for x in (trade.get("take_profits") or []) if float(x) > 0]
    if len(tps) < 3:
        return False
    side = str(trade.get("side", "")).upper()
    hits = set(int(x) for x in (trade.get("partial_tp_hits") or []) if str(x).isdigit())

    # TP3 — финальное закрытие остатка.
    if tp_reached(side, float(last_price), tps[2]):
        await close_autotrade(bot, trade, "TP3", float(last_price))
        return True

    if 2 not in hits and tp_reached(side, float(last_price), tps[1]):
        return await execute_partial_close(bot, trade, 2, TP2_CLOSE_PERCENT, float(last_price))

    if 1 not in hits and tp_reached(side, float(last_price), tps[0]):
        return await execute_partial_close(bot, trade, 1, TP1_CLOSE_PERCENT, float(last_price))

    return False


async def sync_exchange_positions(bot: Bot) -> None:
    open_live_trades = [t for t in get_open_trades() if str(t.get("mode")) == "live"]
    if not open_live_trades:
        return
    for trade in open_live_trades:
        try:
            position = await fetch_live_position_for_trade(trade)
            last_price = await fetch_live_last_price(str(trade.get("exchange")), str(trade.get("symbol")), str(trade.get("ccxt_symbol") or ""))
            now = time.time()
            if position is None:
                close_price = float(last_price or trade.get("entry") or 0)
                pnl_pct = None
                if close_price > 0:
                    pnl_pct = pct_from_entry(close_price, float(trade.get("entry", close_price)))
                    if str(trade.get("side", "")).upper() == "SHORT":
                        pnl_pct = -pnl_pct
                await cancel_protective_orders(trade)
                await mark_trade_closed(bot, trade, "EXCHANGE_SYNC_CLOSED", close_price, pnl_pct, "\nℹ️ Позиция не найдена на бирже — сделка закрыта в учёте бота.")
                continue

            amount = position_amount(position)
            async with TRADES_LOCK:
                trades = load_trades()
                for item in trades:
                    if item.get("id") == trade.get("id") and item.get("status") == "open":
                        item["exchange_position_amount"] = amount
                        item["last_synced_at"] = now
                        if last_price:
                            item["last_exchange_price"] = last_price
                        break
                save_trades(trades)
            await ensure_live_protection(bot, trade)
        except Exception as exc:
            logging.exception("Ошибка синхронизации позиции %s", trade.get("id"))
            async with TRADES_LOCK:
                trades = load_trades()
                for item in trades:
                    if item.get("id") == trade.get("id") and item.get("status") == "open":
                        item["last_sync_error"] = str(exc)
                        item["last_sync_error_at"] = time.time()
                        break
                save_trades(trades)
        await asyncio.sleep(0.15)


async def trade_monitor_worker(bot: Bot) -> None:
    await asyncio.sleep(15)
    last_sync_at = 0.0
    if SYNC_POSITIONS_ON_START:
        try:
            await sync_exchange_positions(bot)
            last_sync_at = time.time()
        except Exception:
            logging.exception("Ошибка стартовой синхронизации позиций")
    while True:
        try:
            now = time.time()
            if now - last_sync_at >= SYNC_POSITIONS_INTERVAL_SECONDS:
                await sync_exchange_positions(bot)
                last_sync_at = now

            open_trades = get_open_trades()
            if open_trades:
                async with aiohttp.ClientSession() as session:
                    for trade in open_trades:
                        last_price: Optional[float] = None
                        if str(trade.get("mode")) == "live":
                            last_price = await fetch_live_last_price(
                                str(trade.get("exchange", MARKET_DATA_PROVIDER)),
                                str(trade.get("symbol")),
                                str(trade.get("ccxt_symbol") or ""),
                            )
                        if last_price is None:
                            candles = await fetch_klines_for_exchange(
                                session,
                                str(trade.get("exchange", MARKET_DATA_PROVIDER)),
                                str(trade.get("symbol")),
                                SIGNAL_TIMEFRAME,
                                3,
                            )
                            if candles:
                                last_price = float(candles[-1]["close"])
                        if last_price is None:
                            continue
                        if await manage_partial_take_profits(bot, trade, float(last_price)):
                            await asyncio.sleep(0.15)
                            continue
                        reason = trade_exit_reason(trade, float(last_price))
                        if reason:
                            await close_autotrade(bot, trade, reason, float(last_price))
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
    logging.info(
        "Авто-сканер включён: provider=%s symbols_mode=%s timeframe=%s trend_filter=%s trend_timeframe=%s interval=%ss threshold=%s%%",
        MARKET_DATA_PROVIDER,
        symbols_mode_text(),
        SIGNAL_TIMEFRAME,
        TREND_FILTER_ENABLED,
        TREND_TIMEFRAME,
        SCAN_INTERVAL_SECONDS,
        MIN_SIGNAL_PROBABILITY,
    )
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
        trend_candles = None
        if candles and (TREND_FILTER_ENABLED or SUPER_DEAL_ENABLED):
            if TREND_TIMEFRAME == SIGNAL_TIMEFRAME:
                trend_candles = candles
            else:
                trend_candles = await asyncio.wait_for(fetch_klines(session, normalized, TREND_TIMEFRAME, KLINES_LIMIT), timeout=30)
    if not candles:
        return None, False, 0
    candidate = analyze_candles(normalized, candles)
    if candidate and (TREND_FILTER_ENABLED or SUPER_DEAL_ENABLED):
        trend = analyze_primary_trend(trend_candles, TREND_TIMEFRAME)
        candidate = apply_trend_filter(candidate, trend)
    if candidate:
        candidate = apply_neural_optimizer(candidate, candles)
    if candidate:
        candidate = apply_smart_algorithm(candidate)
    if candidate:
        candidate = apply_trading_improvements_filters(candidate, candles)
    if candidate:
        candidate = apply_super_deal_filter(candidate)
    return candidate, True, len(candles)


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
        super_deal=candidate.is_super_deal,
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
        "Команды: /help, /status, /settings, /scan, /super_deal, /improvements, /id, /stop",
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
            "• /neural — нейро-оптимизатор алгоритмов\n"
            "• /trend — статус фильтра старшего тренда\n"
            "• /super_deal — режим супер-сделок 97-99% и trend score ±7\n"
            "• /improvements — Улучшения торговли ON/OFF\n"
            "• /stats — расширенная статистика сделок и фильтров\n"
            "• /panic — остановить автоторговлю, отменить защиту и закрыть позиции\n"
            "• /api — API ключи для LIVE-торговли\n"
            "• /margin 10 — маржа/объём на сделку в USDT\n"
            "• /trades — активные авто-сделки\n"
            "• /close_trade ID — вручную закрыть авто-сделку\n"
            "• /sync_trades — сверить LIVE-позиции с биржей\n"
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
        "• /neural — нейро-оптимизатор алгоритмов\n"
        "• /trend — фильтр старшего тренда\n"
        "• /super_deal — режим супер-сделок\n"
        "• /improvements — Улучшения торговли\n"
        "• /stats — статистика торговли\n"
        "• /api — API ключи для автоторговли\n"
        "• /trades — активные авто-сделки\n"
        "• /close_trade ID — ручное закрытие авто-сделки\n"
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
        f"Нейросети: <b>{html.escape(neural_optimizer_label())}</b>\n"
        f"AI-статус: <b>{html.escape(neural_optimizer_stats_text())}</b>\n"
        f"Smart-история: <b>{html.escape(smart_learning_stats_text())}</b>\n"
        f"Фильтр тренда: <b>{html.escape(trend_filter_label())}</b>\n"
        f"Супер сделка: <b>{html.escape(super_deal_label())}</b>\n"
        f"Улучшения торговли: <b>{html.escape(trading_improvements_label())}</b>\n"
        f"Автоторговля: <b>{html.escape(autotrade_label())}</b>\n"
        f"API текущей биржи: <b>{'есть' if has_api_keys(MARKET_DATA_PROVIDER) else 'нет'}</b>\n"
        f"Маржа/объём сделки: <b>${TRADE_MARGIN_USDT:g}</b>\n"
        f"Авто-закрытие: <b>SL или TP{AUTO_CLOSE_TP_INDEX}</b>\n"
        f"Биржевые защитные ордера: <b>{'включены' if USE_EXCHANGE_PROTECTIVE_ORDERS else 'выключены'}</b>\n"
        f"Синхронизация позиций: <b>каждые {SYNC_POSITIONS_INTERVAL_SECONDS} сек.</b>\n"
        f"Хранилище data: <code>{html.escape(str(DATA_DIR))}</code>\n"
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


@dp.message(Command("neural"))
async def cmd_neural(message: Message) -> None:
    if not is_admin(message.from_user.id):
        await message.answer("Настройка доступна только админу.")
        return

    state = load_json(NEURAL_OPTIMIZER_FILE, {})
    last_best = state.get("last_best") if isinstance(state, dict) else {}
    recent_lines: list[str] = []
    if isinstance(last_best, dict) and last_best:
        items = sorted(
            last_best.items(),
            key=lambda kv: float((kv[1] or {}).get("updated_at") or 0),
            reverse=True,
        )[:5]
        for symbol, info in items:
            if not isinstance(info, dict):
                continue
            recent_lines.append(
                f"• <b>{html.escape(symbol)}</b>: {html.escape(str(info.get('profile_name', 'n/a')))} — "
                f"WR {float(info.get('win_rate') or 0) * 100:.0f}%, "
                f"PF {float(info.get('profit_factor') or 0):.2f}, "
                f"avg {float(info.get('avg_pnl') or 0):+.2f}%, "
                f"{int(info.get('trades') or 0)} тест-сдел."
            )

    profiles = neural_strategy_profiles()[:NEURAL_OPTIMIZER_MAX_PROFILES]
    profile_text = "\n".join(f"• {html.escape(str(p.get('name')))}" for p in profiles)
    recent_text = "\n".join(recent_lines) if recent_lines else "Пока нет выбранных профилей. Они появятся после сканов при включённом модуле."

    await message.answer(
        "<b>🤖 Нейросети / AI-оптимизатор</b>\n\n"
        f"Статус: <b>{html.escape(neural_optimizer_label())}</b>\n"
        f"Порог сделок в backtest: <b>{NEURAL_OPTIMIZER_MIN_TRADES}</b>\n"
        f"Мин. winrate: <b>{NEURAL_OPTIMIZER_MIN_WIN_RATE * 100:.0f}%</b>\n"
        f"Мин. profit factor: <b>{NEURAL_OPTIMIZER_MIN_PROFIT_FACTOR:.2f}</b>\n"
        f"Мин. средний PnL: <b>{NEURAL_OPTIMIZER_MIN_AVG_PNL:+.2f}%</b>\n"
        f"Горизонт теста: <b>{NEURAL_OPTIMIZER_HORIZON_CANDLES}</b> свечей\n\n"
        "<b>Что делает:</b> перебирает алгоритмы на свежей истории каждой монеты, "
        "выбирает лучший профиль и пропускает сигнал/сделку только если метрики выше порогов. "
        "Это снижает количество входов, но не гарантирует прибыль.\n\n"
        "<b>Алгоритмы в переборе:</b>\n"
        f"{profile_text}\n\n"
        "<b>Последние выбранные лучшие профили:</b>\n"
        f"{recent_text}",
        reply_markup=neural_optimizer_keyboard(),
    )


@dp.message(Command("trend"))
async def cmd_trend(message: Message) -> None:
    if not is_admin(message.from_user.id):
        await message.answer("Настройка доступна только админу.")
        return
    await message.answer(
        "<b>🧭 Фильтр основного тренда</b>\n\n"
        f"Статус: <b>{html.escape(trend_filter_label())}</b>\n"
        f"Рабочий ТФ сигнала: <b>{html.escape(SIGNAL_TIMEFRAME)}</b>\n"
        f"Старший ТФ тренда: <b>{html.escape(TREND_TIMEFRAME)}</b>\n"
        f"Минимальный score: <b>{TREND_MIN_SCORE}</b>\n\n"
        "Когда включено, бот сначала ищет обычный сетап, затем проверяет старший ТФ. "
        "LONG проходит только при бычьем тренде, SHORT — только при медвежьем. "
        "Флэт/неясный тренд отсекается. Фильтр применяется и к отправке сигналов, и к авто-открытию сделок.",
        reply_markup=trend_filter_keyboard(),
    )


@dp.message(Command("super_deal"))
async def cmd_super_deal(message: Message) -> None:
    if not is_admin(message.from_user.id):
        await message.answer("Настройка доступна только админу.")
        return
    await message.answer(
        "<b>🔴 Супер сделка</b>\n\n"
        f"Статус: <b>{html.escape(super_deal_label())}</b>\n"
        f"Итоговая проходимость: <b>{SUPER_DEAL_MIN_PROBABILITY}-99%</b>\n"
        f"Минимум базовой проходимости: <b>{SUPER_DEAL_RAW_PROBABILITY_MIN}%</b>\n"
        f"Обязательный trend score: <b>+{SUPER_DEAL_TREND_SCORE_ABS}</b> для LONG или <b>-{SUPER_DEAL_TREND_SCORE_ABS}</b> для SHORT\n"
        f"Таймфрейм тренда: <b>{html.escape(TREND_TIMEFRAME)}</b>\n\n"
        "Когда включено, бот отсекает все обычные сетапы и отправляет/открывает только супер-сделки. "
        "Сигнал будет начинаться с: <b>🔴🔴🔴 Внимание, есть супер сделка!</b>\n\n"
        "Важно: это не гарантия прибыли, а самый строгий технический фильтр.",
        reply_markup=super_deal_keyboard(),
    )


@dp.message(Command("improvements"))
async def cmd_improvements(message: Message) -> None:
    if not is_admin(message.from_user.id):
        await message.answer("Настройка доступна только админу.")
        return
    await message.answer(
        improvements_stats_text()
        + "\n\n<b>Что включает ON:</b>\n"
        "• риск-движок по % от депозита и расстоянию до SL\n"
        "• дневной/недельный лимит убытка и пауза после серии минусов\n"
        "• частичные TP1/TP2/TP3 и перенос SL в breakeven\n"
        "• проверка защитных ордеров LIVE, спреда и ликвидности\n"
        "• рейтинг монет, корреляционный фильтр, режим рынка\n"
        "• walk-forward проверка AI-оптимизатора\n"
        "• команды /panic и /stats",
        reply_markup=trading_improvements_keyboard(),
    )


@dp.message(Command("stats"))
async def cmd_stats(message: Message) -> None:
    if not is_admin(message.from_user.id):
        await message.answer("Статистика доступна только админу.")
        return
    await message.answer(improvements_stats_text())


@dp.message(Command("panic"))
async def cmd_panic(message: Message, bot: Bot) -> None:
    global AUTO_TRADE_MODE
    if not is_admin(message.from_user.id):
        await message.answer("PANIC доступен только админу.")
        return

    previous_mode = AUTO_TRADE_MODE
    AUTO_TRADE_MODE = "off"
    save_runtime_settings()
    open_trades = get_open_trades()
    await message.answer(
        "🚨 <b>PANIC MODE</b>\n"
        "Автоторговля выключена. Закрываю открытые сделки и отменяю защитные ордера..."
    )

    closed = 0
    failed = 0
    for trade in open_trades:
        try:
            last_price = None
            if str(trade.get("mode")) == "live":
                try:
                    await cancel_protective_orders(trade)
                except Exception:
                    logging.exception("PANIC: ошибка отмены защиты")
                last_price = await fetch_live_last_price(str(trade.get("exchange")), str(trade.get("symbol")), str(trade.get("ccxt_symbol") or ""))
            if last_price is None:
                last_price = float(trade.get("last_exchange_price") or trade.get("entry") or 0)
            if last_price <= 0:
                failed += 1
                continue
            ok = await close_autotrade(bot, trade, "PANIC", float(last_price))
            if ok:
                closed += 1
            else:
                failed += 1
            await asyncio.sleep(0.2)
        except Exception:
            logging.exception("PANIC: ошибка закрытия сделки")
            failed += 1

    increment_improvement_counter("panic_used")
    await message.answer(
        "🚨 <b>PANIC завершён</b>\n"
        f"Предыдущий режим: <b>{html.escape(previous_mode.upper())}</b>\n"
        f"Автоторговля сейчас: <b>{html.escape(AUTO_TRADE_MODE.upper())}</b>\n"
        f"Закрыто сделок: <b>{closed}</b>\n"
        f"Ошибок: <b>{failed}</b>\n\n"
        "Проверь биржу вручную: в быстром рынке API может отвечать с задержкой."
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
    if not ALLOW_API_KEYS_FILE:
        await message.answer(
            "🔐 Файловое хранение API-ключей отключено.\n\n"
            "Добавь ключи в Railway Variables вместо Telegram-команды:\n"
            "<code>MEXC_API_KEY</code>, <code>MEXC_API_SECRET</code>\n"
            "<code>BINGX_API_KEY</code>, <code>BINGX_API_SECRET</code>\n\n"
            "Это безопаснее: секреты не попадут в <code>data/api_keys.json</code>. "
            "Чтобы вернуть команду /api_set, установи <code>ALLOW_API_KEYS_FILE=true</code>."
        )
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
        await message.answer("🧹 Файловые API ключи очищены. Если ключи заданы в Railway Variables, удали их в Railway Dashboard.")
        return
    if exchange not in EXCHANGE_OPTIONS:
        await message.answer("Формат: <code>/api_clear MEXC</code>, <code>/api_clear BINGX</code> или <code>/api_clear all</code>.")
        return
    keys = load_api_keys()
    keys.pop(exchange, None)
    save_api_keys(keys)
    await message.answer(f"🧹 Файловые API ключи для {html.escape(exchange_label(exchange))} очищены. Railway Variables этой командой не удаляются.")


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


@dp.message(Command("sync_trades"))
async def cmd_sync_trades(message: Message, bot: Bot) -> None:
    if not is_admin(message.from_user.id):
        await message.answer("Синхронизация доступна только админу.")
        return
    await message.answer("🔄 Сверяю открытые LIVE-сделки с биржей...")
    try:
        await sync_exchange_positions(bot)
    except Exception as exc:
        logging.exception("Ошибка ручной синхронизации")
        await message.answer(f"⚠️ Ошибка синхронизации: <code>{html.escape(str(exc))}</code>")
        return
    await message.answer("✅ Синхронизация завершена.\n\n" + trades_status_text())


@dp.message(Command("close_trade"))
async def cmd_close_trade(message: Message, command: CommandObject, bot: Bot) -> None:
    if not is_admin(message.from_user.id):
        await message.answer("Закрытие сделок доступно только админу.")
        return
    arg = (command.args or "").strip()
    if not arg:
        await message.answer("Формат: <code>/close_trade ID</code>\nID можно посмотреть в /trades.")
        return
    trade_key = arg.split()[0].strip()
    open_trades = get_open_trades()
    trade = None
    for item in open_trades:
        if str(item.get("id")) == trade_key or compact_symbol(str(item.get("symbol", ""))) == compact_symbol(trade_key):
            trade = item
            break
    if trade is None:
        await message.answer("Открытая сделка не найдена. Проверь /trades.")
        return

    last_price = None
    if str(trade.get("mode")) == "live":
        last_price = await fetch_live_last_price(str(trade.get("exchange")), str(trade.get("symbol")), str(trade.get("ccxt_symbol") or ""))
    if last_price is None:
        timeout = aiohttp.ClientTimeout(total=15)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            candles = await fetch_klines_for_exchange(session, str(trade.get("exchange", MARKET_DATA_PROVIDER)), str(trade.get("symbol")), SIGNAL_TIMEFRAME, 3)
            if candles:
                last_price = float(candles[-1]["close"])
    if last_price is None:
        await message.answer("Не удалось получить текущую цену для закрытия. Попробуй /sync_trades и проверь биржу.")
        return

    ok = await close_autotrade(bot, trade, "MANUAL", float(last_price))
    if ok:
        await message.answer("✅ Команда закрытия выполнена. Проверь биржу и /trades.")
    else:
        await message.answer("⚠️ Сделка не была закрыта. Проверь Railway Logs и биржу.")


@dp.callback_query(F.data.startswith("settings:"))
async def settings_callback(callback: CallbackQuery) -> None:
    global SIGNAL_TIMEFRAME, MIN_SIGNAL_PROBABILITY, SCAN_INTERVAL_SECONDS, MARKET_DATA_PROVIDER
    global AUTO_TRADE_MODE, TRADE_MARGIN_USDT, AUTO_CLOSE_TP_INDEX, SMART_ALGORITHM_ENABLED
    global NEURAL_OPTIMIZER_ENABLED, SUPER_DEAL_ENABLED, TRADING_IMPROVEMENTS_ENABLED
    global TREND_FILTER_ENABLED, TREND_TIMEFRAME

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

    if data == "settings:neural":
        await message.edit_text(
            "<b>🤖 Нейросети / AI-оптимизатор</b>\n\n"
            f"Сейчас: <b>{html.escape(neural_optimizer_label())}</b>\n"
            f"Статус: <b>{html.escape(neural_optimizer_stats_text())}</b>\n\n"
            "Когда включено, бот перебирает набор алгоритмов на свежей истории каждой монеты, "
            "выбирает лучший по winrate/profit factor/среднему PnL и только потом пропускает сигнал/сделку. "
            "Модуль может резко уменьшить количество сделок. Это не гарантия прибыли.",
            reply_markup=neural_optimizer_keyboard(),
        )
        await callback.answer()
        return

    if data == "settings:neural_stats":
        await message.edit_text(
            "<b>📊 AI-оптимизатор: лучший алгоритм</b>\n\n"
            f"{html.escape(neural_optimizer_stats_text())}\n\n"
            "Подробности по последним выбранным профилям смотри командой /neural.",
            reply_markup=neural_optimizer_keyboard(),
        )
        await callback.answer()
        return

    if data == "settings:trend":
        await message.edit_text(
            "<b>🧭 Фильтр основного тренда</b>\n\n"
            f"Сейчас: <b>{html.escape(trend_filter_label())}</b>\n"
            f"Рабочий ТФ сигнала: <b>{html.escape(SIGNAL_TIMEFRAME)}</b>\n"
            f"Старший ТФ тренда: <b>{html.escape(TREND_TIMEFRAME)}</b>\n"
            f"Минимальный score: <b>{TREND_MIN_SCORE}</b>\n\n"
            "Когда включено: LONG только при бычьем старшем тренде, SHORT только при медвежьем. "
            "Флэт/неясный тренд отсекается до отправки сигнала и до авто-входа.",
            reply_markup=trend_filter_keyboard(),
        )
        await callback.answer()
        return

    if data == "settings:trend_timeframe":
        await message.edit_text(
            f"<b>⏱ Выбери старший таймфрейм тренда</b>\n\nСейчас: <b>{html.escape(TREND_TIMEFRAME)}</b>\n\n"
            "Для LIVE обычно разумно 1h или 4h. Чем выше ТФ, тем меньше сигналов, но меньше шума.",
            reply_markup=trend_timeframe_keyboard(),
        )
        await callback.answer()
        return

    if data == "settings:super_deal":
        await message.edit_text(
            "<b>🔴 Супер сделка</b>\n\n"
            f"Сейчас: <b>{html.escape(super_deal_label())}</b>\n"
            f"Условия: проходимость <b>{SUPER_DEAL_MIN_PROBABILITY}-99%</b>, "
            f"trend score <b>+{SUPER_DEAL_TREND_SCORE_ABS}</b> для LONG или <b>-{SUPER_DEAL_TREND_SCORE_ABS}</b> для SHORT.\n\n"
            "Когда включено, бот не отправляет обычные сигналы и не открывает обычные сделки — "
            "только самые строгие супер-сделки. Это не гарантия прибыли.",
            reply_markup=super_deal_keyboard(),
        )
        await callback.answer()
        return

    if data == "settings:improvements":
        await message.edit_text(
            improvements_stats_text()
            + "\n\nON = бот торгует с дополнительными защитными фильтрами. OFF = прежний режим.",
            reply_markup=trading_improvements_keyboard(),
        )
        await callback.answer()
        return

    if data == "settings:improvements_stats":
        await message.edit_text(improvements_stats_text(), reply_markup=trading_improvements_keyboard())
        await callback.answer()
        return

    if data == "settings:autotrade":
        await message.edit_text(
            "<b>💰 Автоторговля</b>\n\n"
            f"Режим: <b>{html.escape(autotrade_label())}</b>\n"
            f"Маржа/объём сделки: <b>${TRADE_MARGIN_USDT:g}</b>\n"
            f"Авто-закрытие: <b>SL или TP{AUTO_CLOSE_TP_INDEX}</b>\n"
            f"Открытых сделок: <b>{len(get_open_trades())}</b>\n\n"
            "OFF — только сигналы. PAPER — тест без ордеров. LIVE — реальные ордера + биржевые SL/TP где поддерживаются + синхронизация.",
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
            "Для LIVE бот пытается выставить reduce-only conditional SL/TP на бирже и дополнительно контролирует цену по ticker/mark price.",
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
        await callback.answer("Файловые ключи очищены; Railway Variables удаляются только в Railway", show_alert=True)
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

    if data.startswith("settings:set_neural:"):
        value = data.split(":", 2)[2].lower()
        if value in {"on", "off"}:
            NEURAL_OPTIMIZER_ENABLED = value == "on"
            save_runtime_settings()
            await message.edit_text(settings_menu_text(), reply_markup=settings_keyboard())
            await callback.answer("Нейро-оптимизатор включён" if NEURAL_OPTIMIZER_ENABLED else "Нейро-оптимизатор выключен")
        else:
            await callback.answer("Неверное значение", show_alert=True)
        return

    if data.startswith("settings:set_trend:"):
        value = data.split(":", 2)[2].lower()
        if value in {"on", "off"}:
            TREND_FILTER_ENABLED = value == "on"
            save_runtime_settings()
            await message.edit_text(settings_menu_text(), reply_markup=settings_keyboard())
            await callback.answer("Фильтр тренда включён" if TREND_FILTER_ENABLED else "Фильтр тренда выключен")
        else:
            await callback.answer("Неверное значение", show_alert=True)
        return

    if data.startswith("settings:set_trend_timeframe:"):
        value = data.split(":", 2)[2]
        if value in TREND_TIMEFRAME_OPTIONS:
            TREND_TIMEFRAME = value
            save_runtime_settings()
            await message.edit_text(settings_menu_text(), reply_markup=settings_keyboard())
            await callback.answer(f"Старший ТФ тренда: {value}")
        else:
            await callback.answer("Неверный таймфрейм тренда", show_alert=True)
        return

    if data.startswith("settings:set_super_deal:"):
        value = data.split(":", 2)[2].lower()
        if value in {"on", "off"}:
            SUPER_DEAL_ENABLED = value == "on"
            save_runtime_settings()
            await message.edit_text(settings_menu_text(), reply_markup=settings_keyboard())
            await callback.answer("Супер-сделка включена" if SUPER_DEAL_ENABLED else "Супер-сделка выключена")
        else:
            await callback.answer("Неверное значение", show_alert=True)
        return

    if data.startswith("settings:set_improvements:"):
        value = data.split(":", 2)[2].lower()
        if value in {"on", "off"}:
            TRADING_IMPROVEMENTS_ENABLED = value == "on"
            save_runtime_settings()
            await message.edit_text(settings_menu_text(), reply_markup=settings_keyboard())
            await callback.answer("Улучшения торговли включены" if TRADING_IMPROVEMENTS_ENABLED else "Улучшения торговли выключены")
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


@dp.message(F.text == "🤖 Нейросети")
async def button_neural(message: Message) -> None:
    await cmd_neural(message)


@dp.message(F.text == "🧭 Фильтр тренда")
async def button_trend(message: Message) -> None:
    await cmd_trend(message)


@dp.message(F.text == "🔴 Супер сделка")
async def button_super_deal(message: Message) -> None:
    await cmd_super_deal(message)


@dp.message(F.text == "🚀 Улучшения торговли")
async def button_improvements(message: Message) -> None:
    await cmd_improvements(message)


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
