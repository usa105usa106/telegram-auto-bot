# 🤖 Нейросети / AI-оптимизатор алгоритмов

Добавлен отдельный модуль, который можно включать и выключать независимо от старого smart-алгоритма и тренд-фильтра.

Важно: модуль не гарантирует прибыль. Он не обещает «вечный плюс», а делает более безопасную вещь:
перебирает несколько алгоритмических профилей на свежей истории свечей, выбирает лучший по метрикам и пропускает сигнал/сделку только если у выбранного профиля есть положительное преимущество.

## Как включить

Через Telegram:

```text
/settings → 🤖 Нейросети → ON
```

или через Railway Variables:

```env
NEURAL_OPTIMIZER_ENABLED=true
```

## Команда

```text
/neural
```

Показывает:
- включён модуль или нет;
- какие алгоритмы перебираются;
- текущие пороги;
- последние выбранные лучшие профили по монетам.

## Как работает

1. Базовый алгоритм находит LONG/SHORT сигнал.
2. Если включён тренд-фильтр, сигнал сначала проходит фильтр старшего ТФ.
3. AI-оптимизатор перебирает несколько профилей:
   - EMA + RSI + MACD momentum;
   - пробой + объём;
   - консервативный тренд;
   - MACD + RSI drive;
   - откат к EMA21;
   - RSI reversal;
   - high-volume trend;
   - EMA strict breakout.
4. Для каждого профиля бот делает быстрый backtest на свежих свечах этой монеты.
5. Выбирается лучший профиль по:
   - winrate;
   - profit factor;
   - средний PnL;
   - количество тест-сделок;
   - fitness score.
6. Если лучший профиль не проходит пороги — сигнал не отправляется и авто-сделка не открывается.
7. Если проходит — в сигнал добавляется строка `🤖 AI: выбран алгоритм ...`.

## Переменные окружения

```env
NEURAL_OPTIMIZER_ENABLED=false
NEURAL_OPTIMIZER_STRICT_MODE=true
NEURAL_OPTIMIZER_MIN_TRADES=6
NEURAL_OPTIMIZER_MIN_WIN_RATE=0.55
NEURAL_OPTIMIZER_MIN_PROFIT_FACTOR=1.15
NEURAL_OPTIMIZER_MIN_AVG_PNL=0.05
NEURAL_OPTIMIZER_HORIZON_CANDLES=24
NEURAL_OPTIMIZER_PROBABILITY_BONUS=5
NEURAL_OPTIMIZER_MAX_PROFILES=10
```

## Рекомендация для LIVE

Осторожный режим:

```env
SIGNAL_TIMEFRAME=30m
TREND_FILTER_ENABLED=true
TREND_TIMEFRAME=4h
NEURAL_OPTIMIZER_ENABLED=true
NEURAL_OPTIMIZER_STRICT_MODE=true
NEURAL_OPTIMIZER_MIN_TRADES=6
NEURAL_OPTIMIZER_MIN_WIN_RATE=0.55
NEURAL_OPTIMIZER_MIN_PROFIT_FACTOR=1.15
MIN_SIGNAL_PROBABILITY=85
MAX_ACTIVE_TRADES=1
```

Если сделок почти нет, можно смягчить:

```env
NEURAL_OPTIMIZER_MIN_WIN_RATE=0.52
NEURAL_OPTIMIZER_MIN_PROFIT_FACTOR=1.05
NEURAL_OPTIMIZER_STRICT_MODE=false
```

## Где хранится состояние

Файл:

```text
data/neural_optimizer.json
```

Если на Railway используется Volume, файл будет сохраняться между деплоями.

## Важное ограничение

Это не полноценная большая нейросеть с GPU и обучением на миллионах сделок. Это лёгкий встроенный AI/optimizer без дополнительных зависимостей, чтобы бот мог работать на Railway.
Он перебирает алгоритмы и адаптируется к текущей истории рынка, но убытки всё равно возможны.
