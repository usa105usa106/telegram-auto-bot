# Режим «Только BTC/ETH»

Добавлен отдельный режим для более строгого анализа только BTCUSDT и ETHUSDT.

## Как включить

В Telegram:

1. Открой `/settings`.
2. Нажми `₿ Только BTC/ETH`.
3. Выбери `ON` или `OFF`.

Также доступна команда:

```text
/btc_eth
```

## Что меняется при ON

- авто-скан берёт только `BTCUSDT` и `ETHUSDT`;
- ручной скан других монет блокируется с подсказкой выключить режим;
- перед отправкой сигнала применяется строгий BTC/ETH-фильтр;
- фильтр проверяет базовый score, старший тренд, EMA, RSI, MACD, ATR, объём, близость к EMA21 и подтверждения на нескольких таймфреймах;
- автоторговля использует тот же pipeline, поэтому тоже будет открывать сделки только по BTC/ETH-сигналам.

## Переменные окружения

```env
BTC_ETH_ONLY_MODE_ENABLED=false
BTC_ETH_ONLY_MIN_PROBABILITY=90
BTC_ETH_ONLY_TREND_SCORE_ABS=5
BTC_ETH_ONLY_MIN_TF_CONFIRMATIONS=2
BTC_ETH_ONLY_MIN_CONFIRMATION_SCORE=5
BTC_ETH_CONFIRMATION_TIMEFRAMES=1h,4h
BTC_ETH_ONLY_MIN_VOLUME_RATIO=1.05
BTC_ETH_ONLY_MIN_ATR_PCT=0.05
BTC_ETH_ONLY_MAX_ATR_PCT=4.0
BTC_ETH_ONLY_MAX_ENTRY_ATR_DISTANCE=1.6
```

## Важно

Режим делает бота более консервативным и уменьшает количество сделок, но не гарантирует прибыль. Перед LIVE лучше проверить режим в `PAPER` и собрать статистику закрытых сделок.
