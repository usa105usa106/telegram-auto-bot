MEXC Futures auto-signal bot

Что изменено:
- Источник данных зашит в код: MEXC Futures.
- MARKET_DATA_PROVIDER в Railway больше добавлять не нужно.
- Старую переменную SYMBOLS бот по умолчанию игнорирует.
- Бот сам берёт топ-100 MEXC Futures по 24h обороту через публичный ticker endpoint.
- Свечи берутся через MEXC Futures kline endpoint.
- Символы в сигналах отображаются как на MEXC: BTC_USDT, ETH_USDT и т.д.

Что загрузить в GitHub:
- bot.py
- requirements.txt
- Procfile
- railway.json
- runtime.txt

После загрузки:
1. Commit changes в GitHub.
2. Railway сам сделает redeploy.
3. В Telegram отправь /status и /scan.

Ожидаемый статус:
Источник данных: MEXC Futures
Режим монет: топ 100 MEXC Futures по 24h обороту

Если хочешь временно использовать старый список SYMBOLS из Railway, добавь переменную USE_ENV_SYMBOLS=true. Но обычно это не нужно.
