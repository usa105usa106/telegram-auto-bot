Telegram Long/Short Signal Bot — MEXC Futures + настройки в боте

Что нового:
- Источник данных: MEXC Futures.
- Бот сам берет топ-100 MEXC Futures по 24h обороту.
- Настройки можно менять прямо в Telegram через /settings или кнопку ⚙️ Настройки.
- Доступно изменение:
  1) Таймфрейм: 5m, 15m, 30m, 1h, 4h
  2) Минимальная проходимость: 60, 70, 75, 80, 85, 90%
  3) Интервал скана: 2, 5, 10, 15, 30, 60 минут

Railway Variables, которые всё ещё нужны:
BOT_TOKEN=токен_от_BotFather
ADMIN_IDS=твой_telegram_id

Переменные MIN_SIGNAL_PROBABILITY, SIGNAL_TIMEFRAME и SCAN_INTERVAL_SECONDS теперь можно не менять в Railway — меняются в самом боте.

Как обновить:
1. Замени в GitHub файлы:
   bot.py
   requirements.txt
   Procfile
   railway.json
   runtime.txt
2. Нажми Commit changes.
3. Дождись redeploy в Railway.
4. В Telegram отправь /start, затем /settings.

Важно:
Настройки сохраняются в data/settings.json внутри Railway-сервиса. При новом деплое/сбросе контейнера они могут вернуться к значениям по умолчанию. Если такое случится, просто заново выставь их через /settings.
