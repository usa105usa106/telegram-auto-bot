# v1025 — ETH/BTC Profit, RSS новости и TradingView-style анализ

Что изменено:

- CryptoPanic больше не нужен для ETH Profit.
- Новостной фон берётся из бесплатных RSS/public источников:
  - CoinDesk RSS
  - Cointelegraph RSS
  - Decrypt RSS
  - CryptoSlate RSS
  - Bitcoin Magazine RSS
  - Google News RSS по BTC/ETH/crypto запросам
- Добавлена отдельная кнопка `₿ BTC Profit`.
- Добавлена команда `/btc_profit on/off`.
- Ручной запуск BTC анализа: `btc profit`.
- ETH Profit оставлен: `/eth_profit on/off`, ручной запуск `eth profit`.
- Когда ETH Profit или BTC Profit включён, обычный авто-скан, автоторговля и обычный ручной скан монет ставятся на паузу.
- Одновременно может быть включён только один Profit-режим: если включить BTC Profit, ETH Profit выключится, и наоборот.
- Добавлен локальный TradingView-style теханализ:
  - это не платный/закрытый TradingView API;
  - бот сам считает похожую сводку по EMA/RSI/MACD/наклону/объёму на всех таймфреймах;
  - результат мягко усиливает LONG/SHORT.
- В отчёте теперь выводится строка `TradingView-style теханализ`.
- Версия `/ping`: `1025`.

Важно: анализ не гарантирует прибыль. Новости используются как фильтр риска и мягкое усиление направления, а не как главный сигнал.
