Исправление источника данных.

Если бот показывает: Данные получены 0 / N, значит Railway не получает свечи с биржи.
Эта версия добавляет OKX как источник данных и делает его первым в auto-режиме.

В Railway Variables поставь:
MARKET_DATA_PROVIDER=okx

Для теста:
MIN_SIGNAL_PROBABILITY=60
SYMBOLS=BTCUSDT,ETHUSDT,SOLUSDT,TRXUSDT,AVAXUSDT,ADAUSDT,DOGEUSDT,XRPUSDT
SIGNAL_TIMEFRAME=15m

После изменения переменных сделай Restart/Redeploy и отправь /scan.
