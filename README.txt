MULTIQUANT GOLD BOT - FRESH ACCOUNT BUILD

Files:
- worker.js
- wrangler.toml

Important:
- Crypto bot is separate and is not included here.
- Keep Telegram token, CRON_SECRET and ADMIN_CHAT_ID out of GitHub.
- Configure them as Cloudflare Worker secrets/vars in the new account.
- Worker uses XAU-USDT-SWAP and posts to @multiquantacademy.
- Maximum 5 open positions.
- Only 5M, 15M, 30M, 1H and 4H signal timeframes.
- Normal ticker updates do NOT write to Durable Objects SQLite.
- SQLite writes happen for actual TP/close events and normal state/signal events.
