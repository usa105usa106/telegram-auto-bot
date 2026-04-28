Telegram Signal Bot - auto scanner fixed

Main change:
- MARKET_DATA_PROVIDER=auto: Bybit first, Binance fallback.
- Admin reports after every auto scan, so you can see that automatic scanning works.
- /scan now shows data received / failed pairs and best setups even if they are below the threshold.

Recommended Railway variables:
BOT_TOKEN=your_token
ADMIN_IDS=your_telegram_id
MIN_SIGNAL_PROBABILITY=60
AUTO_SIGNALS_ENABLED=true
SYMBOLS=BTCUSDT,ETHUSDT,SOLUSDT,TRXUSDT,AVAXUSDT,ADAUSDT,DOGEUSDT,XRPUSDT
SIGNAL_TIMEFRAME=1h
SCAN_INTERVAL_SECONDS=600
SIGNAL_COOLDOWN_MINUTES=360
MAX_SIGNALS_PER_SCAN=3
MARKET_DATA_PROVIDER=auto
AUTO_SCAN_REPORTS_TO_ADMINS=true
AUTO_SCAN_REPORT_EVERY_N_SCANS=1

After testing, if admin reports are too noisy, set:
AUTO_SCAN_REPORTS_TO_ADMINS=false
