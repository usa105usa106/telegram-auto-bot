# LIVE safety upgrade

Эта сборка доработана для более безопасного LIVE-режима.

## Что изменено

1. **Биржевые защитные ордера**
   - После LIVE-входа бот пытается выставить reduce-only conditional SL и TP на бирже через CCXT.
   - Используются `stopLossPrice` / `takeProfitPrice` и fallback-варианты `triggerPrice`.
   - Если биржа не принимает protective orders, бот не бросает сделку без контроля: включается fallback-мониторинг по ticker/mark price.

2. **Мониторинг цены**
   - LIVE больше не опирается только на close последней свечи.
   - Для открытых LIVE-сделок бот берёт `fetch_ticker()` и старается использовать mark/last/close/bid/ask.
   - Свечи используются только как fallback, если ticker недоступен.

3. **Синхронизация с биржей**
   - На старте и далее по расписанию бот сверяет открытые LIVE-сделки с реальными позициями через `fetch_positions()`.
   - Если позиции на бирже нет, сделка закрывается в учёте бота с причиной `EXCHANGE_SYNC_CLOSED`.
   - Если позиция есть, бот обновляет `exchange_position_amount`, `last_synced_at`, `last_exchange_price`.

4. **JSON-lock и атомарная запись**
   - Для `trades.json` добавлен `asyncio.Lock`.
   - Запись JSON теперь идёт через временный файл и `os.replace()`, чтобы снизить риск повреждения файла.

5. **Ручное закрытие**
   - Новая команда: `/close_trade ID`.
   - ID смотри в `/trades`.
   - При LIVE-закрытии бот сначала пытается отменить защитные ордера, потом отправляет reduce-only market close.

6. **Синхронизация вручную**
   - Новая команда: `/sync_trades`.
   - Полезно после ручного закрытия позиции на бирже или рестарта Railway.

7. **API-ключи**
   - `/api_set` снова включён по умолчанию, как в старой версии.
   - Команда сохраняет ключи в `data/api_keys.json`.
   - Более безопасный вариант всё ещё доступен: можно хранить ключи в Railway Variables / `.env`:
     - `MEXC_API_KEY`
     - `MEXC_API_SECRET`
     - `BINGX_API_KEY`
     - `BINGX_API_SECRET`
   - Если хочешь запретить сохранение ключей через Telegram, установи `ALLOW_API_KEYS_FILE=false`.

8. **Railway Volume / постоянное хранилище**
   - `DATA_DIR` теперь можно задать через переменную окружения.
   - Если Railway отдаёт `RAILWAY_VOLUME_MOUNT_PATH`, бот использует его автоматически.
   - Рекомендуется подключить Railway Volume и задать `DATA_DIR=/data` или использовать `RAILWAY_VOLUME_MOUNT_PATH`.

## Новые переменные окружения

```env
USE_EXCHANGE_PROTECTIVE_ORDERS=true
CANCEL_PROTECTIVE_ORDERS_ON_CLOSE=true
SYNC_POSITIONS_ON_START=true
SYNC_POSITIONS_INTERVAL_SECONDS=120
ALLOW_API_KEYS_FILE=true
DATA_DIR=/data
```

## Важное предупреждение

CCXT унифицирует защитные ордера, но поддержка conditional SL/TP зависит от конкретной биржи, типа рынка и версии API. Поэтому после первого запуска LIVE обязательно:

1. Запусти минимальный объём.
2. Убедись в интерфейсе биржи, что позиция открылась.
3. Убедись, что SL и TP реально появились как reduce-only / conditional / trigger orders.
4. Проверь `/trades` и `/sync_trades`.

Если в `/trades` у сделки указано `защитные ордера: fallback`, не оставляй позицию без ручной проверки.
