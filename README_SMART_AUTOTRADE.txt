Telegram Long/Short Signal Bot — MEXC/BingX + Auto Trading + Smart Algorithm

Что добавлено в этой версии:
- /settings -> кнопка "🧠 Умный алгоритм"
- Вкл/выкл умного алгоритма прямо в Telegram
- /smart — статистика smart-алгоритма
- Smart-алгоритм анализирует закрытые авто-сделки из data/trades.json
- После серии минусов бот становится строже, штрафует убыточные пары/стороны и может отсеивать слабые сетапы
- Закрытые сделки теперь сохраняют pnl_pct, чтобы алгоритм мог учиться на истории

Важно:
- Это не гарантия прибыли и не настоящий ML-модельный прогноз.
- Самообучение работает только после появления истории закрытых PAPER/LIVE авто-сделок.
- До накопления истории бот использует обычный скоринг и добавляет пометку "мало истории".
- Для безопасности сначала тестируй в PAPER-режиме.

Файлы для GitHub/Railway:
- bot.py
- requirements.txt
- Procfile
- railway.json
- runtime.txt

Railway Variables по-прежнему нужны:
- BOT_TOKEN
- ADMIN_IDS

API-ключи можно добавить в самом боте командой /api_set.
Withdraw/вывод средств для API-ключа не включать.


=== LIVE SAFETY UPDATE ===

Добавлена доработка LIVE-режима:
- биржевые reduce-only conditional SL/TP через CCXT, если поддерживается биржей;
- fallback-мониторинг по ticker/mark price;
- синхронизация открытых сделок с биржей через /sync_trades и по расписанию;
- ручное закрытие /close_trade ID;
- asyncio.Lock и атомарная запись trades.json;
- API-ключи по умолчанию только через Railway Variables/.env;
- DATA_DIR можно направить на Railway Volume.

Подробности смотри в README_LIVE_SAFETY.md.
