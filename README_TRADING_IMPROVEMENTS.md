# 🚀 Улучшения торговли

Добавлен master-переключатель **«🚀 Улучшения торговли»**.

Когда он выключен, бот работает в прежнем режиме: старый скоринг, старое открытие сделок, старое авто-закрытие по SL/TP.

Когда включён, бот использует дополнительные защитные и оптимизационные модули:

1. **Риск-движок** — размер позиции считается от риска до стопа, а не просто фиксированной суммой.
2. **Дневной и недельный лимит убытка** — если лимит достигнут, новые сделки не открываются.
3. **Пауза после серии минусов** — после заданного количества убыточных сделок бот уходит в паузу.
4. **Breakeven после TP1** — после первого тейка стоп переносится в безубыток с небольшим offset.
5. **Частичное закрытие TP1/TP2/TP3** — по умолчанию 40% / 30% / остаток.
6. **Проверка защитных ордеров LIVE** — если биржевые SL/TP не выставлены, LIVE-позиция аварийно закрывается.
7. **Фильтр спреда и ликвидности** — перед LIVE-входом проверяется bid/ask spread и 24h volume.
8. **Рейтинг монет** — бот блокирует монеты, которые плохо торгуются по истории именно этого бота.
9. **Корреляционный фильтр** — ограничивает количество сделок в одну сторону и количество ALT-сделок.
10. **Фильтр рыночного режима** — блокирует флэт/низкую волатильность/аномальные свечи.
11. **Walk-forward AI optimizer** — AI-профиль должен подтвердиться не только на train, но и на свежем test-участке.
12. **Расширенная статистика + PANIC** — команды `/stats` и `/panic`.

## Команды

```text
/improvements — статус и меню улучшений
/stats — статистика сделок и фильтров
/panic — выключить автоторговлю, отменить защиту и закрыть открытые сделки
```

## Меню

```text
/settings → 🚀 Улучшения торговли → ON/OFF
```

## Основные переменные Railway

```env
TRADING_IMPROVEMENTS_ENABLED=false
ACCOUNT_EQUITY_USDT=100
RISK_PER_TRADE_PERCENT=0.5
MAX_POSITION_NOTIONAL_USDT=5
MAX_DAILY_LOSS_PERCENT=2
MAX_WEEKLY_LOSS_PERCENT=5
MAX_CONSECUTIVE_LOSSES=3
PAUSE_AFTER_LOSS_STREAK_HOURS=12

PARTIAL_TP_ENABLED=true
TP1_CLOSE_PERCENT=40
TP2_CLOSE_PERCENT=30
TP3_CLOSE_PERCENT=30
MOVE_SL_TO_BREAKEVEN_AFTER_TP1=true
BREAKEVEN_OFFSET_PCT=0.03

STRICT_PROTECTION_CHECK_ENABLED=true
MAX_SPREAD_PCT=0.15
MIN_24H_QUOTE_VOLUME_USDT=500000
MIN_LAST_CANDLE_VOLUME_USDT=10000
MIN_AVG_CANDLE_VOLUME_USDT=8000
MIN_ATR_PCT=0.08
MAX_ATR_PCT=8

COIN_RATING_FILTER_ENABLED=true
SYMBOL_RATING_MIN_TRADES=5
MIN_SYMBOL_WIN_RATE=0.55
MIN_SYMBOL_PROFIT_FACTOR=1.1

CORRELATION_FILTER_ENABLED=true
MAX_SAME_DIRECTION_TRADES=1
MAX_ALT_TRADES=2

MARKET_REGIME_FILTER_ENABLED=true
MAX_PUMP_CANDLE_PCT=6
HIGH_VOL_POSITION_FACTOR=0.5

WALK_FORWARD_OPTIMIZER_ENABLED=true
WALK_FORWARD_TRAIN_RATIO=0.7
WALK_FORWARD_MIN_TEST_TRADES=2
```

## Важное

Это не гарантия прибыли. Улучшения делают бота строже: сигналов и сделок станет меньше, зато риск-контроль станет сильнее. Первый запуск в LIVE — только минимальным объёмом и с ручной проверкой, что SL/TP реально появились на бирже.
