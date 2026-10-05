import { DurableObject } from "cloudflare:workers";

const CFG = {
  BOT_USERNAME: "multiquantgoldbot",
  CHANNEL_CHAT_ID: "@multiquantacademy",
  CHANNEL_USERNAME: "@multiquantacademy",
  OKX_PUBLIC_WS_URL: "wss://ws.okx.com/ws/v5/public",
  OKX_BUSINESS_WS_URL: "wss://ws.okx.com/ws/v5/business",
  OKX_INST_ID: "XAU-USDT-SWAP",
  MAX_OPEN_POSITIONS: 5,
  // Fixed community/MT5 reference conversion: Community Price = OKX Price - 3.88
  GOLD_PRICE_OFFSET: -3.88,
  TP1_POINTS: 7,
  TP2_POINTS: 13,
  TP3_POINTS: 22,
  TP1_POINTS_5M: 5,
  TP2_POINTS_5M: 8,
  TP3_POINTS_5M: 13,
  REVERSAL_MIN_FAVORABLE_POINTS: 5,
  REVERSAL_GAP_5M: 3,
  REVERSAL_GAP_LARGE_TF: 5,
  // 5M is now an active signal timeframe. Strategy logic remains unchanged.
  TIMEFRAMES: [
    { key: "5M", channel: "candle5m", bar: "5m", history: 180, signal: true },
    { key: "15M", channel: "candle15m", bar: "15m", history: 160, signal: true },
    { key: "30M", channel: "candle30m", bar: "30m", history: 140, signal: true },
    { key: "1H", channel: "candle1H", bar: "1H", history: 130, signal: true },
    { key: "4H", channel: "candle4H", bar: "4H", history: 110, signal: true }
  ]
};

const SIGNAL_TIMEFRAMES = CFG.TIMEFRAMES.filter(x => x.signal);

const TF = Object.fromEntries(CFG.TIMEFRAMES.map(x => [x.key, x]));

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname === "/") return new Response("MultiQuant Gold Bot is running.");

    const id = env.GOLD_ENGINE.idFromName("gold-main");
    const stub = env.GOLD_ENGINE.get(id);

    if (url.pathname === "/health") {
      const r = await stub.fetch("https://gold-engine/health");
      return new Response(await r.text(), { status: r.status, headers: { "content-type": "application/json" } });
    }

    if (url.pathname === "/webhook" && request.method === "POST") {
      const update = await request.json();
      await stub.fetch("https://gold-engine/update", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(update)
      });
      return new Response("OK");
    }

    if (url.pathname === "/setup-webhook") {
      if (!env.CRON_SECRET || url.searchParams.get("secret") !== env.CRON_SECRET) return new Response("Forbidden", { status: 403 });
      const webhookUrl = new URL(request.url);
      webhookUrl.pathname = "/webhook";
      return json(await tg(env, "setWebhook", { url: webhookUrl.toString(), allowed_updates: ["message"] }));
    }

    if (url.pathname === "/test") {
      if (!env.CRON_SECRET || url.searchParams.get("secret") !== env.CRON_SECRET) return new Response("Forbidden", { status: 403 });
      const r = await stub.fetch("https://gold-engine/test");
      return new Response(await r.text(), { status: r.status, headers: { "content-type": "application/json" } });
    }

    return new Response("Not Found", { status: 404 });
  },

  async scheduled(event, env, ctx) {
    const id = env.GOLD_ENGINE.idFromName("gold-main");
    const stub = env.GOLD_ENGINE.get(id);
    ctx.waitUntil(stub.fetch("https://gold-engine/tick", { method: "POST" }));
  }
};

export class GoldEngine extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    this.ctx = ctx;
    this.env = env;
    this.publicWs = null;
    this.businessWs = null;
    this.reconnectTimer = null;
    this.candles = {};
    for (const tf of CFG.TIMEFRAMES) this.candles[tf.key] = [];
    this.lastPrice = 0; // Community/MT5 reference price
    this.lastOkxPrice = 0; // Raw OKX price used by strategy
    this.lastTickerTs = 0;
    this.lastSignalKey = "";
    this.signalLocks = new Map();
    this.lastEvaluatedCandleTs = new Map();
    this.signalSendInFlight = false;
    this.openPositions = new Map();
    // Position monitoring is single-flight. OKX can emit many ticks while a
    // Telegram request is awaiting; never allow overlapping monitor passes.
    this.monitorRunning = false;
    this.pendingMonitorPrice = 0;
    this.closedRetryRunning = false;
    this.initialized = false;
    this.initPromise = null;
    this.ctx.blockConcurrencyWhile(async () => { await this.init(); });
  }

  async init() {
    if (this.initialized) return;
    if (this.initPromise) return this.initPromise;
    this.initPromise = (async () => {
      await this.ensureSchema();
      await this.loadState();
      await this.loadOpenPositionsCache();
      await this.bootstrapAllTimeframes();
      this.connectSockets();
      this.initialized = true;
    })();
    return this.initPromise;
  }

  async ensureSchema() {
    const sql = this.ctx.storage.sql;
    sql.exec(`CREATE TABLE IF NOT EXISTS gold_positions (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      symbol TEXT NOT NULL,
      direction TEXT NOT NULL,
      entry_low REAL NOT NULL,
      entry_high REAL NOT NULL,
      entry_mid REAL NOT NULL,
      sl_price REAL NOT NULL,
      tp1_price REAL NOT NULL,
      tp2_price REAL NOT NULL,
      tp3_price REAL NOT NULL,
      tp1_hit INTEGER DEFAULT 0,
      tp2_hit INTEGER DEFAULT 0,
      tp3_hit INTEGER DEFAULT 0,
      tp1_notified INTEGER DEFAULT 0,
      tp2_notified INTEGER DEFAULT 0,
      tp3_notified INTEGER DEFAULT 0,
      close_notified INTEGER DEFAULT 0,
      highest_price REAL,
      lowest_price REAL,
      status TEXT DEFAULT 'OPEN',
      channel_message_id INTEGER,
      setup TEXT,
      timeframe TEXT,
      opened_at TEXT NOT NULL,
      closed_at TEXT,
      close_price REAL,
      close_reason TEXT,
      realized_points REAL,
      peak_points REAL
    );`);
    sql.exec(`CREATE TABLE IF NOT EXISTS gold_signal_history (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      symbol TEXT NOT NULL,
      direction TEXT NOT NULL,
      entry_mid REAL NOT NULL,
      sl_price REAL NOT NULL,
      tp1_price REAL NOT NULL,
      tp2_price REAL NOT NULL,
      tp3_price REAL NOT NULL,
      setup TEXT,
      timeframe TEXT,
      candle_ts INTEGER,
      signal_key TEXT,
      sent_at TEXT NOT NULL
    );`);
    // Safe migration for databases created by the previous Gold bot version.
    try { sql.exec("ALTER TABLE gold_signal_history ADD COLUMN timeframe TEXT"); } catch (_) {}
    try { sql.exec("ALTER TABLE gold_signal_history ADD COLUMN candle_ts INTEGER"); } catch (_) {}
    try { sql.exec("ALTER TABLE gold_signal_history ADD COLUMN signal_key TEXT"); } catch (_) {}
    try { sql.exec("ALTER TABLE gold_positions ADD COLUMN tp1_notified INTEGER DEFAULT 0"); } catch (_) {}
    try { sql.exec("ALTER TABLE gold_positions ADD COLUMN tp2_notified INTEGER DEFAULT 0"); } catch (_) {}
    try { sql.exec("ALTER TABLE gold_positions ADD COLUMN tp3_notified INTEGER DEFAULT 0"); } catch (_) {}
    try { sql.exec("ALTER TABLE gold_positions ADD COLUMN close_notified INTEGER DEFAULT 0"); } catch (_) {}
    sql.exec(`CREATE TABLE IF NOT EXISTS gold_meta (key TEXT PRIMARY KEY, value TEXT);`);
    sql.exec(`CREATE TABLE IF NOT EXISTS gold_candle_evaluations (timeframe TEXT NOT NULL, candle_ts INTEGER NOT NULL, evaluated_at TEXT NOT NULL, PRIMARY KEY(timeframe, candle_ts));`);
  }

  async loadState() {
    const rows = this.ctx.storage.sql.exec("SELECT key,value FROM gold_meta").toArray();
    for (const row of rows) {
      if (row.key === "lastSignalKey") this.lastSignalKey = String(row.value || "");
      if (row.key.startsWith("lastEvaluatedCandle:")) {
        this.lastEvaluatedCandleTs.set(row.key.slice("lastEvaluatedCandle:".length), Number(row.value || 0));
      }
    }
    const evalRows = this.ctx.storage.sql.exec("SELECT timeframe, MAX(candle_ts) AS candle_ts FROM gold_candle_evaluations GROUP BY timeframe").toArray();
    for (const row of evalRows) {
      const ts = Number(row.candle_ts || 0);
      if (ts > 0) this.lastEvaluatedCandleTs.set(String(row.timeframe), ts);
    }
  }

  async saveMeta(key, value) {
    this.ctx.storage.sql.exec("INSERT OR REPLACE INTO gold_meta(key,value) VALUES(?,?)", key, String(value));
  }

  async loadOpenPositionsCache() {
    const rows = this.ctx.storage.sql
      .exec("SELECT * FROM gold_positions WHERE status='OPEN' ORDER BY id ASC")
      .toArray();
    this.openPositions.clear();
    for (const row of rows) {
      this.openPositions.set(Number(row.id), {
        ...row,
        id: Number(row.id),
        entry_mid: Number(row.entry_mid),
        sl_price: Number(row.sl_price),
        tp1_price: Number(row.tp1_price),
        tp2_price: Number(row.tp2_price),
        tp3_price: Number(row.tp3_price),
        tp1_hit: Number(row.tp1_hit || 0),
        tp2_hit: Number(row.tp2_hit || 0),
        tp3_hit: Number(row.tp3_hit || 0),
        tp1_notified: Number(row.tp1_notified || 0),
        tp2_notified: Number(row.tp2_notified || 0),
        tp3_notified: Number(row.tp3_notified || 0),
        close_notified: Number(row.close_notified || 0),
        highest_price: Number(row.highest_price || row.entry_mid),
        lowest_price: Number(row.lowest_price || row.entry_mid)
      });
    }
  }

  async bootstrapAllTimeframes() {
    // OKX can return 429 if all candle-history requests fire together.
    // Bootstrap sequentially so the live WebSocket remains the primary feed.
    const results = [];
    for (const tf of CFG.TIMEFRAMES) {
      results.push(await this.bootstrapTimeframe(tf));
      await sleep(700);
    }
    const summary = {};
    for (const r of results) summary[r.key] = r.count;
    console.log("GOLD MTF BOOTSTRAP", JSON.stringify(summary));
  }

  async bootstrapTimeframe(tf) {
    let rows = [];
    let lastError = "";
    const endpoints = [
      `https://www.okx.com/api/v5/market/candles?instId=${encodeURIComponent(CFG.OKX_INST_ID)}&bar=${encodeURIComponent(tf.bar)}&limit=${Math.min(tf.history, 300)}`,
      `https://www.okx.com/api/v5/market/history-candles?instId=${encodeURIComponent(CFG.OKX_INST_ID)}&bar=${encodeURIComponent(tf.bar)}&limit=${Math.min(tf.history, 100)}`
    ];

    for (const endpoint of endpoints) {
      try {
        let r = await fetch(endpoint, { headers: { "accept": "application/json" } });
        let body = await r.text();
        if (r.status === 429) {
          await sleep(1800);
          r = await fetch(endpoint, { headers: { "accept": "application/json" } });
          body = await r.text();
        }
        if (!r.ok) throw new Error(`HTTP ${r.status}: ${body.slice(0, 300)}`);
        const p = JSON.parse(body);
        if (String(p.code || "0") !== "0") throw new Error(`OKX ${p.code}: ${p.msg || "unknown"}`);
        if (!Array.isArray(p.data) || !p.data.length) throw new Error("OKX returned no candles");
        rows = p.data.slice().reverse();
        break;
      } catch (e) {
        lastError = String(e?.message || e);
      }
    }

    if (!rows.length) {
      console.error("GOLD MTF BOOTSTRAP ERROR", tf.key, lastError);
      return { key: tf.key, count: 0 };
    }

    const candles = rows.map(parseCandle).filter(c => c.c > 0);
    const dedup = new Map(candles.map(c => [c.ts, c]));
    this.candles[tf.key] = Array.from(dedup.values()).sort((a, b) => a.ts - b.ts).slice(-tf.history);
    return { key: tf.key, count: this.candles[tf.key].length };
  }

  connectSockets() {
    this.connectBusiness();
    this.connectPublic();
  }

  connectBusiness() {
    try {
      if (this.businessWs && (this.businessWs.readyState === WebSocket.OPEN || this.businessWs.readyState === WebSocket.CONNECTING)) return;
      const ws = new WebSocket(this.env.OKX_BUSINESS_WS_URL || CFG.OKX_BUSINESS_WS_URL);
      this.businessWs = ws;
      ws.addEventListener("open", () => {
        const args = CFG.TIMEFRAMES.map(tf => ({ channel: tf.channel, instId: CFG.OKX_INST_ID }));
        ws.send(JSON.stringify({ op: "subscribe", args }));
        console.log("GOLD OKX BUSINESS WS OPEN", JSON.stringify({ subscriptions: args.map(x => x.channel) }));
      });
      ws.addEventListener("message", e => this.onBusinessMessage(e.data));
      ws.addEventListener("close", () => { if (this.businessWs === ws) this.businessWs = null; this.scheduleReconnect(); });
      ws.addEventListener("error", e => console.error("GOLD BUSINESS WS ERROR", e));
    } catch (e) {
      console.error("GOLD BUSINESS WS CONNECT ERROR", e);
      this.scheduleReconnect();
    }
  }

  connectPublic() {
    try {
      if (this.publicWs && (this.publicWs.readyState === WebSocket.OPEN || this.publicWs.readyState === WebSocket.CONNECTING)) return;
      const ws = new WebSocket(this.env.OKX_PUBLIC_WS_URL || CFG.OKX_PUBLIC_WS_URL);
      this.publicWs = ws;
      ws.addEventListener("open", () => {
        ws.send(JSON.stringify({ op: "subscribe", args: [{ channel: "tickers", instId: CFG.OKX_INST_ID }] }));
        console.log("GOLD OKX PUBLIC WS OPEN");
      });
      ws.addEventListener("message", e => this.onPublicMessage(e.data));
      ws.addEventListener("close", () => { if (this.publicWs === ws) this.publicWs = null; this.scheduleReconnect(); });
      ws.addEventListener("error", e => console.error("GOLD PUBLIC WS ERROR", e));
    } catch (e) {
      console.error("GOLD PUBLIC WS CONNECT ERROR", e);
      this.scheduleReconnect();
    }
  }

  scheduleReconnect() {
    if (this.reconnectTimer) return;
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      this.connectSockets();
    }, 5000);
  }

  parseMessage(data) {
    try {
      if (typeof data === "string" && data.toLowerCase() === "ping") return { ping: true };
      return typeof data === "string" ? JSON.parse(data) : data;
    } catch (_) { return null; }
  }

  onPublicMessage(data) {
    const m = this.parseMessage(data);
    if (!m) return;
    if (m.ping) { try { this.publicWs?.send("pong"); } catch (_) {} return; }
    if (!Array.isArray(m.data)) return;
    for (const x of m.data) {
      const price = Number(x.last);
      if (price > 0) {
        this.lastOkxPrice = price;
        this.lastPrice = toCommunityPrice(price);
        this.lastTickerTs = Number(x.ts || Date.now());
        this.monitorPositions(this.lastPrice).catch(e => console.error("GOLD MONITOR ERROR", e));
      }
    }
  }

  onBusinessMessage(data) {
    const m = this.parseMessage(data);
    if (!m) return;
    if (m.ping) { try { this.businessWs?.send("pong"); } catch (_) {} return; }
    if (m.event === "error") { console.error("GOLD OKX BUSINESS SUBSCRIBE ERROR", JSON.stringify(m)); return; }
    if (!Array.isArray(m.data) || !m.data.length) return;

    const channel = String(m.arg?.channel || "");
    const tf = CFG.TIMEFRAMES.find(x => x.channel === channel);
    if (!tf) return;

    for (const x of m.data) {
      const candle = parseCandle(x);
      if (!(candle.c > 0)) continue;
      // Candle closes are for strategy analysis only. The live trade/monitoring
      // reference MUST remain the raw OKX ticker last converted by exactly -3.88.
      // Never let a business/candle update overwrite lastOkxPrice/lastPrice.
      const arr = this.candles[tf.key] || (this.candles[tf.key] = []);
      const idx = arr.findIndex(c => c.ts === candle.ts);
      if (idx >= 0) arr[idx] = candle;
      else arr.push(candle);
      this.candles[tf.key] = arr.slice(-tf.history);

      // IMPORTANT: evaluate signals only once, when a candle is confirmed closed.
      // OKX can send many updates per second for the same live candle. Evaluating
      // every update can create repeated signals as the live price/entry moves.
      // Position monitoring remains tick-driven separately.
      if (candle.confirm === 1 && tf.signal) {
        const lastEvaluated = Number(this.lastEvaluatedCandleTs.get(tf.key) || 0);
        if (candle.ts !== lastEvaluated) {
          // Persist the candle evaluation marker BEFORE starting async signal work.
          // This prevents duplicate evaluations after WS reconnects or rapid duplicate
          // OKX messages, even if the Durable Object is rehydrated.
          this.lastEvaluatedCandleTs.set(tf.key, candle.ts);
          this.ctx.storage.sql.exec(
            "INSERT OR IGNORE INTO gold_candle_evaluations(timeframe,candle_ts,evaluated_at) VALUES(?,?,?)",
            tf.key, candle.ts, new Date().toISOString()
          );
          this.saveMeta(`lastEvaluatedCandle:${tf.key}`, candle.ts);
          this.evaluateSignalForTimeframe(tf.key, false).catch(e => console.error("GOLD SIGNAL EVAL ERROR", tf.key, e));
        }
      }
    }
  }

  async alarmTick() {
    await this.init();
    this.connectSockets();
    // Primary monitoring uses the live OKX ticker converted by -3.88.
    // The 1-minute alarm is the backup pass in case a websocket tick was missed.
    if (this.lastPrice > 0) await this.monitorPositions(this.lastPrice);
    // Closed-position notification recovery runs only on the 1-minute backup,
    // never on every OKX tick. This prevents retry storms and Telegram 429 floods.
    await this.retryPendingClosedNotifications();
    // If a socket restarted after a cold DO wake, refresh history if a
    // timeframe is too short for indicators.
    for (const tf of CFG.TIMEFRAMES) {
      if ((this.candles[tf.key] || []).length < 35) await this.bootstrapTimeframe(tf);
    }
  }

  async fetch(request) {
    await this.init();
    const url = new URL(request.url);
    if (url.pathname === "/health") {
      const candleCounts = {};
      for (const tf of CFG.TIMEFRAMES) candleCounts[tf.key] = (this.candles[tf.key] || []).length;
      return json({
        ok: true,
        instrument: CFG.OKX_INST_ID,
        price: this.lastPrice,
        candles: candleCounts,
        publicWs: this.publicWs?.readyState || 0,
        businessWs: this.businessWs?.readyState || 0,
        openPositions: Number(this.ctx.storage.sql.exec("SELECT COUNT(*) AS n FROM gold_positions WHERE status='OPEN'").one()?.n || 0),
        maxOpenPositions: CFG.MAX_OPEN_POSITIONS
      });
    }
    if (url.pathname === "/tick") {
      await this.alarmTick();
      return json({ ok: true, price: this.lastPrice });
    }
    if (url.pathname === "/test") {
      const results = [];
      for (const tf of SIGNAL_TIMEFRAMES) {
        const s = await this.evaluateSignalForTimeframe(tf.key, true);
        results.push({ timeframe: tf.key, signal: !!s });
      }
      return json({ ok: true, price: this.lastPrice, results });
    }
    if (url.pathname === "/update" && request.method === "POST") {
      await this.handleTelegram(await request.json());
      return new Response("OK");
    }
    return new Response("Gold Engine OK");
  }

  async handleTelegram(update) {
    const msg = update?.message;
    if (!msg?.chat || msg.chat.type !== "private") return;
    const chatId = String(msg.chat.id);
    const text = String(msg.text || "").trim().toLowerCase();
    const admin = String(this.env.ADMIN_CHAT_ID || "");
    if (admin && chatId !== admin) return;
    if (text === "/positions" || text === "/openpositions") return this.sendPositions(chatId);
    if (text === "/weekly") return this.sendReport(chatId, 7);
    if (text === "/monthly") return this.sendReport(chatId, 31, true);
    if (text === "/goldtest") {
      const results = [];
      for (const tf of SIGNAL_TIMEFRAMES) {
        const s = await this.evaluateSignalForTimeframe(tf.key, true);
        if (s) results.push(`${tf.key} ${s.direction} ${s.setup}`);
      }
      return this.sendText(chatId, `🥇 Gold MTF scanner test completed.\nLive Price: ${fmt(this.lastPrice)}\nSignals: ${results.length ? results.join(", ") : "No qualifying setup"}`);
    }
    if (text === "/health") {
      const counts = CFG.TIMEFRAMES.map(tf => `${tf.key}:${(this.candles[tf.key] || []).length}`).join(" | ");
      return this.sendText(chatId, `🥇 Gold Bot\nPrice: ${fmt(this.lastPrice)}\nCandles: ${counts}\nPublic WS: ${this.publicWs?.readyState || 0}\nBusiness WS: ${this.businessWs?.readyState || 0}`);
    }
  }

  async evaluateSignalForTimeframe(timeframe, force = false) {
    const tf = TF[timeframe];
    if (!tf || !(this.lastOkxPrice > 0)) return null;
    const candles = this.candles[timeframe] || [];
    if (candles.length < 35) return null;

    // Strategy calculations remain on the raw OKX price scale.
    const rawSetup = buildGoldSetup(candles, this.lastOkxPrice, timeframe, this.candles);
    if (!rawSetup) return null;

    // Manual test is analysis-only. It MUST NOT publish a production signal,
    // create a position, or write signal history.
    if (force) return applyCommunityPrice(rawSetup);

    const setup = applyCommunityPrice(rawSetup);
    const candleTs = Number(candles[candles.length - 1]?.ts || 0);
    const key = signalFingerprint(setup, candleTs);
    const now = Date.now();

    const openCount = this.openPositions.size;
    if (openCount >= CFG.MAX_OPEN_POSITIONS) return null;
    if (this.signalSendInFlight) return null;
    const lockUntil = Number(this.signalLocks.get(timeframe) || 0);
    if (lockUntil > now) return null;
    if (key === this.lastSignalKey || this.wasSignalAlreadySent(key)) return null;

    const sameSetupOpen = Array.from(this.openPositions.values()).some(p =>
      p.direction === setup.direction && p.setup === setup.setup && p.timeframe === setup.timeframe
    );
    if (sameSetupOpen) return null;

    this.signalLocks.set(timeframe, now + 15000);
    this.signalSendInFlight = true;

    try {
      const text = formatSignal(setup);
      const sent = await sendChannel(this.env, text);
      if (!sent?.message_id) return null;

      this.lastSignalKey = key;
      await this.saveMeta("lastSignalKey", key);
      this.ctx.storage.sql.exec(`INSERT INTO gold_signal_history(symbol,direction,entry_mid,sl_price,tp1_price,tp2_price,tp3_price,setup,timeframe,candle_ts,signal_key,sent_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`,
        CFG.OKX_INST_ID, setup.direction, setup.entryMid, setup.slPrice, setup.tp1Price, setup.tp2Price, setup.tp3Price,
        setup.setup, setup.timeframe, candleTs, key, new Date().toISOString());

      this.ctx.storage.sql.exec(`INSERT INTO gold_positions(symbol,direction,entry_low,entry_high,entry_mid,sl_price,tp1_price,tp2_price,tp3_price,highest_price,lowest_price,status,channel_message_id,setup,timeframe,opened_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
        CFG.OKX_INST_ID, setup.direction, setup.entryLow, setup.entryHigh, setup.entryMid, setup.slPrice, setup.tp1Price, setup.tp2Price, setup.tp3Price,
        setup.entryMid, setup.entryMid, "OPEN", Number(sent.message_id), setup.setup, setup.timeframe, new Date().toISOString());
      const inserted = this.ctx.storage.sql.exec("SELECT last_insert_rowid() AS id").one();
      const positionId = Number(inserted?.id || 0);
      if (positionId) {
        this.openPositions.set(positionId, {
          id: positionId, symbol: CFG.OKX_INST_ID, direction: setup.direction,
          entry_low: setup.entryLow, entry_high: setup.entryHigh, entry_mid: setup.entryMid,
          sl_price: setup.slPrice, tp1_price: setup.tp1Price, tp2_price: setup.tp2Price, tp3_price: setup.tp3Price,
          tp1_hit: 0, tp2_hit: 0, tp3_hit: 0,
          tp1_notified: 0, tp2_notified: 0, tp3_notified: 0, close_notified: 0,
          highest_price: setup.entryMid, lowest_price: setup.entryMid,
          status: "OPEN", channel_message_id: Number(sent.message_id), setup: setup.setup, timeframe: setup.timeframe,
          opened_at: new Date().toISOString()
        });
      }

      console.log("GOLD SIGNAL SENT", JSON.stringify({ timeframe: setup.timeframe, direction: setup.direction, setup: setup.setup, entry: setup.entry, sl: setup.sl, tp1: setup.tp1, tp2: setup.tp2, tp3: setup.tp3, score: setup.score, offset: CFG.GOLD_PRICE_OFFSET }));
      return setup;
    } finally {
      this.signalLocks.delete(timeframe);
      this.signalSendInFlight = false;
    }
  }

  wasSignalAlreadySent(key) {
    const rows = this.ctx.storage.sql
      .exec("SELECT id FROM gold_signal_history WHERE signal_key=? LIMIT 1", key)
      .toArray();
    return rows.length > 0;
  }

  wasRecentSameSetup(_) { return false; }

  async monitorPositions(price) {
    if (!(price > 0)) return;

    // SINGLE-FLIGHT GUARD: live OKX ticks can arrive while Telegram awaits.
    // Keep only the latest price and let the current pass finish first.
    if (this.monitorRunning) {
      this.pendingMonitorPrice = price;
      return;
    }

    this.monitorRunning = true;
    try {
      let nextPrice = price;
      for (let pass = 0; pass < 2; pass++) {
        this.pendingMonitorPrice = 0;
        await this._monitorPositionsOnce(nextPrice);
        const latest = Number(this.pendingMonitorPrice || 0);
        if (!(latest > 0) || latest === nextPrice) break;
        nextPrice = latest;
      }
    } finally {
      this.monitorRunning = false;
    }
  }

  async _monitorPositionsOnce(price) {
    // The monitoring price is ALWAYS the live community/MT5 reference:
    // raw OKX ticker last - 3.88. Candle prices are never used here.
    if (this.openPositions.size) {
      for (const [positionId, pos] of Array.from(this.openPositions.entries())) {
        if (pos.status !== "OPEN") continue;

        const direction = pos.direction;
        const high = Math.max(Number(pos.highest_price || pos.entry_mid), price);
        const low = Math.min(Number(pos.lowest_price || pos.entry_mid), price);
        pos.highest_price = high;
        pos.lowest_price = low;

        let tp1 = Number(pos.tp1_hit || 0);
        let tp2 = Number(pos.tp2_hit || 0);
        let tp3 = Number(pos.tp3_hit || 0);
        let n1 = Number(pos.tp1_notified || 0);
        let n2 = Number(pos.tp2_notified || 0);
        let n3 = Number(pos.tp3_notified || 0);
        const reached = [];
        const tests = [[1, Number(pos.tp1_price)], [2, Number(pos.tp2_price)], [3, Number(pos.tp3_price)]];

        for (const [n, target] of tests) {
          const alreadyHit = n === 1 ? tp1 : n === 2 ? tp2 : tp3;
          if (alreadyHit || !(target > 0)) continue;
          const hit = direction === "LONG" ? high >= target : low <= target;
          if (!hit) continue;
          if (n === 1) tp1 = 1;
          if (n === 2) tp2 = 1;
          if (n === 3) tp3 = 1;
          reached.push({ n, target });
        }

        const sl = Number(pos.sl_price);
        const slTouched = direction === "LONG" ? price <= sl : price >= sl;
        const entry = Number(pos.entry_mid);
        const peakPoints = direction === "LONG" ? high - entry : entry - low;
        const favorableEnough = peakPoints >= CFG.REVERSAL_MIN_FAVORABLE_POINTS;
        const reversalGap = pos.timeframe === "5M" ? CFG.REVERSAL_GAP_5M : CFG.REVERSAL_GAP_LARGE_TF;
        const mfeReversalClose = favorableEnough && (direction === "LONG"
          ? price <= high - reversalGap
          : price >= low + reversalGap);
        const directSl = !favorableEnough && slTouched;
        const shouldClose = directSl || mfeReversalClose;

        // Persist hit/high/low state BEFORE awaiting Telegram. If Telegram is
        // temporarily unavailable, notification flags remain pending and will
        // be retried on the next live tick/alarm instead of being lost forever.
        if (reached.length || shouldClose) {
          pos.tp1_hit = tp1;
          pos.tp2_hit = tp2;
          pos.tp3_hit = tp3;
          this.ctx.storage.sql.exec(
            "UPDATE gold_positions SET tp1_hit=?,tp2_hit=?,tp3_hit=?,highest_price=?,lowest_price=? WHERE id=?",
            tp1, tp2, tp3, high, low, positionId
          );
        } else {
          this.ctx.storage.sql.exec(
            "UPDATE gold_positions SET highest_price=?,lowest_price=? WHERE id=?",
            high, low, positionId
          );
        }

        // IMPORTANT: Never retry pending Telegram notifications from the live tick loop.
        // A failed send is persisted as pending and recovered only by the 1-minute alarm.
        // This prevents a Telegram/API failure from creating a retry storm.
        const pending = [];
        if (!shouldClose) continue;

        const closePrice = directSl ? price : (direction === "LONG" ? high : low);
        const realizedPoints = direction === "LONG" ? closePrice - entry : entry - closePrice;
        const reason = directSl ? "Direct SL" : "Reversal Close";

        this.ctx.storage.sql.exec(
          "UPDATE gold_positions SET status='CLOSED',tp1_hit=?,tp2_hit=?,tp3_hit=?,highest_price=?,lowest_price=?,closed_at=?,close_price=?,close_reason=?,realized_points=?,peak_points=? WHERE id=?",
          tp1, tp2, tp3, high, low, new Date().toISOString(), closePrice, reason, realizedPoints, peakPoints, positionId
        );
        pos.status = "CLOSED";
        pos.close_price = closePrice;
        pos.close_reason = reason;
        pos.realized_points = realizedPoints;
        pos.peak_points = peakPoints;

        const closeMsg = formatCloseMessage(pos, directSl, closePrice, realizedPoints, {
          tp1: !!tp1, tp2: !!tp2, tp3: !!tp3
        });
        let closeSent = true;
        if (pos.channel_message_id) {
          const sent = await sendReply(this.env, closeMsg, pos.channel_message_id);
          closeSent = !!sent;
          if (closeSent) {
            pos.close_notified = 1;
            this.ctx.storage.sql.exec("UPDATE gold_positions SET close_notified=1 WHERE id=?", positionId);
          }
        }

        // Keep the closed row in DB for reporting. Remove it from the live cache.
        this.openPositions.delete(positionId);
        if (!closeSent) console.error("GOLD CLOSE NOTIFICATION PENDING", positionId);
      }
    }

  }

  async retryPendingClosedNotifications() {
    // Never run two DB-backed notification recovery passes concurrently.
    if (this.closedRetryRunning) return;
    this.closedRetryRunning = true;
    try {
      const rows = this.ctx.storage.sql.exec(
      "SELECT * FROM gold_positions WHERE status='CLOSED' AND channel_message_id IS NOT NULL AND (close_notified=0 OR (tp1_hit=1 AND tp1_notified=0) OR (tp2_hit=1 AND tp2_notified=0) OR (tp3_hit=1 AND tp3_notified=0)) ORDER BY id ASC LIMIT 1"
    ).toArray();
    for (const row of rows) {
      const pos = this.normalizePositionRow(row);

      // Retry any TP notification that was recorded as hit but not confirmed
      // delivered. This survives worker/Telegram failures even after the trade closes.
      const pendingTp = [];
      if (pos.tp1_hit && !pos.tp1_notified) pendingTp.push({ n: 1, target: Number(pos.tp1_price) });
      if (pos.tp2_hit && !pos.tp2_notified) pendingTp.push({ n: 2, target: Number(pos.tp2_price) });
      if (pos.tp3_hit && !pos.tp3_notified) pendingTp.push({ n: 3, target: Number(pos.tp3_price) });
      for (const hit of pendingTp) {
        const sent = await this.sendTpHit(pos, hit);
        if (!sent) continue;
        if (hit.n === 1) pos.tp1_notified = 1;
        if (hit.n === 2) pos.tp2_notified = 1;
        if (hit.n === 3) pos.tp3_notified = 1;
        this.ctx.storage.sql.exec(
          "UPDATE gold_positions SET tp1_notified=?,tp2_notified=?,tp3_notified=? WHERE id=?",
          pos.tp1_notified, pos.tp2_notified, pos.tp3_notified, Number(pos.id)
        );
      }

      if (!pos.close_notified) {
        const msg = formatCloseMessage(pos, String(pos.close_reason) === "Direct SL", Number(pos.close_price), Number(pos.realized_points || 0), {
          tp1: !!Number(pos.tp1_hit), tp2: !!Number(pos.tp2_hit), tp3: !!Number(pos.tp3_hit)
        });
        const sent = await sendReply(this.env, msg, pos.channel_message_id);
        if (sent) this.ctx.storage.sql.exec("UPDATE gold_positions SET close_notified=1 WHERE id=?", Number(pos.id));
      }
    }
    } finally {
      this.closedRetryRunning = false;
    }
  }

  normalizePositionRow(row) {
    return {
      ...row,
      id: Number(row.id),
      entry_mid: Number(row.entry_mid),
      sl_price: Number(row.sl_price),
      tp1_price: Number(row.tp1_price),
      tp2_price: Number(row.tp2_price),
      tp3_price: Number(row.tp3_price),
      tp1_hit: Number(row.tp1_hit || 0),
      tp2_hit: Number(row.tp2_hit || 0),
      tp3_hit: Number(row.tp3_hit || 0),
      tp1_notified: Number(row.tp1_notified || 0),
      tp2_notified: Number(row.tp2_notified || 0),
      tp3_notified: Number(row.tp3_notified || 0),
      close_notified: Number(row.close_notified || 0),
      highest_price: Number(row.highest_price || row.entry_mid),
      lowest_price: Number(row.lowest_price || row.entry_mid)
    };
  }

  async sendTpHit(pos, hit) {
    const entry = Number(pos.entry_mid);
    const current = Number(hit.target);
    const points = pos.direction === "LONG" ? current - entry : entry - current;
    const heading = hit.n === 1 ? "**✅ TP1 HIT 😎**" : hit.n === 2 ? "**✅ TP2 HIT 🤩**" : "**✅ TP3 HIT 🏆**";
    const next = hit.n === 1 ? "**🎯 TP2 — Coming Soon**" : hit.n === 2 ? "**🎯 TP3 — Coming Soon**" : "**🏆 Position Still Running**";
    const msg = `${heading}\n\n**Entry:** **${fmt(entry)}**\n**Current:** **${fmt(current)}**\n**Points:** **${points >= 0 ? "+" : ""}${points.toFixed(2)}**\n\n${next}`;
    if (!pos.channel_message_id) return true;
    const sent = await sendReply(this.env, msg, pos.channel_message_id);
    return !!sent;
  }

  async sendPositions(chatId) {
    const rows = this.ctx.storage.sql.exec("SELECT * FROM gold_positions WHERE status='OPEN' ORDER BY id ASC").toArray();
    if (!rows.length) {
      return this.sendText(chatId, `🥇 **LIVE GOLD POSITIONS**\n\n━━━━━━━━━━━━━━━━━━━━\n\n📊 **SUMMARY**\n\nOpen Positions : **0**\n\n━━━━━━━━━━━━━━━━━━━━\n\n_No open gold position right now._`);
    }

    const price = Number(this.lastPrice || 0);
    const moves = rows.map(p => p.direction === "LONG" ? price - Number(p.entry_mid) : Number(p.entry_mid) - price);
    const totalMove = moves.reduce((sum, move) => sum + move, 0);
    const buys = rows.filter(p => p.direction === "LONG").length;
    const sells = rows.filter(p => p.direction === "SHORT").length;
    const inProfit = moves.filter(x => x > 0).length;
    const inLoss = moves.filter(x => x < 0).length;
    const flat = moves.filter(x => x === 0).length;
    const tp1Hits = rows.filter(p => Number(p.tp1_hit) === 1).length;
    const tp2Hits = rows.filter(p => Number(p.tp2_hit) === 1).length;
    const tp3Hits = rows.filter(p => Number(p.tp3_hit) === 1).length;

    const summary = [
      `🥇 **LIVE GOLD POSITIONS**`,
      `━━━━━━━━━━━━━━━━━━━━`,
      ``,
      `📊 **SUMMARY**`,
      ``,
      `Open Positions : **${rows.length}**`,
      `🟢 BUY         : **${buys}**`,
      `🔴 SELL        : **${sells}**`,
      ``,
      `📈 In Profit   : **${inProfit}**`,
      `📉 In Loss     : **${inLoss}**`,
      `⏳ Flat        : **${flat}**`,
      ``,
      `💰 **TOTAL P/L : ${signedPoints(totalMove)} Points**`,
      ``,
      `🎯 TP1 Hit     : **${tp1Hits}**`,
      `🎯 TP2 Hit     : **${tp2Hits}**`,
      `🎯 TP3 Hit     : **${tp3Hits}**`,
      ``,
      `💵 Current     : **${fmt(price)}**`,
      ``,
      `━━━━━━━━━━━━━━━━━━━━`,
      `📌 **POSITION DETAILS**`,
      `━━━━━━━━━━━━━━━━━━━━`
    ];

    const chunks = [];
    let current = summary.slice();
    const maxChars = 3500;

    for (let i = 0; i < rows.length; i++) {
      const p = rows[i];
      const currentPrice = price;
      const move = p.direction === "LONG" ? currentPrice - Number(p.entry_mid) : Number(p.entry_mid) - currentPrice;
      const directionLabel = p.direction === "LONG" ? "🟢 BUY" : "🔴 SELL";
      const block = [
        ``,
        `**${String(i + 1).padStart(2, "0")} ┃ ${directionLabel} GOLD — ${p.timeframe || "Gold"}**`,
        `━━━━━━━━━━━━━━━━━━━━`,
        ``,
        `📅 Opened  : *${fmtDate(p.opened_at)}*`,
        ``,
        `📍 **Entry**   : **${fmt(p.entry_low)} – ${fmt(p.entry_high)}**`,
        `💰 **Current** : **${fmt(currentPrice)}**`,
        `📊 **Move**    : **${signedPoints(move)} Points**`,
        ``,
        `🛑 **Stop Loss** : **${fmt(p.sl_price)}**`,
        ``,
        `🎯 **TP1** : **${fmt(p.tp1_price)}**${p.tp1_hit ? " ✅" : ""}`,
        `🎯 **TP2** : **${fmt(p.tp2_price)}**${p.tp2_hit ? " ✅" : ""}`,
        `🎯 **TP3** : **${fmt(p.tp3_price)}**${p.tp3_hit ? " ✅" : ""}`,
        ``,
        `📊 *Setup: ${escapeMarkdown(p.setup || "Gold Setup")}*`
      ].join("\n");

      if ((current.join("\n").length + block.length + 20) > maxChars && current.length > 3) {
        chunks.push(current.join("\n"));
        current = [
          `🥇 **LIVE GOLD POSITIONS — CONTINUED**`,
          `━━━━━━━━━━━━━━━━━━━━`,
          `📌 **POSITION DETAILS**`,
          `━━━━━━━━━━━━━━━━━━━━`
        ];
      }
      current.push(block);
    }
    if (current.length) chunks.push(current.join("\n"));

    for (let i = 0; i < chunks.length; i++) {
      await this.sendText(chatId, `${chunks[i]}\n\n━━━━━━━━━━━━━━━━━━━━\n📄 *${i + 1}/${chunks.length}*`);
    }
    return null;
  }

  async sendReport(chatId, days, calendarMonth = false) {
    const now = new Date();
    const indiaNow = new Date(now.toLocaleString("en-US", { timeZone: "Asia/Kolkata" }));
    const startDate = calendarMonth
      ? new Date(Date.UTC(indiaNow.getFullYear(), indiaNow.getMonth(), 1) - (5.5 * 60 * 60 * 1000))
      : new Date(Date.now() - days * 86400000);
    const start = startDate.toISOString();

    // Signals are counted from signal history, while results are calculated
    // from the positions generated by those signals. This keeps the report
    // understandable: "signals sent" is not confused with "closed trades".
    const signals = this.ctx.storage.sql
      .exec("SELECT * FROM gold_signal_history WHERE sent_at >= ? ORDER BY sent_at ASC", start)
      .toArray();
    const positions = this.ctx.storage.sql
      .exec("SELECT * FROM gold_positions WHERE opened_at >= ? ORDER BY opened_at ASC", start)
      .toArray();

    const totalSignals = signals.length;
    const closed = positions.filter(p => String(p.status) === "CLOSED");
    const running = positions.filter(p => String(p.status) === "OPEN");
    const wins = closed.filter(p => Number(p.realized_points) > 0);
    const losses = closed.filter(p => Number(p.realized_points) < 0);
    const breakeven = closed.filter(p => Number(p.realized_points) === 0);
    const grossProfit = wins.reduce((a, p) => a + Number(p.realized_points || 0), 0);
    const grossLoss = losses.reduce((a, p) => a + Number(p.realized_points || 0), 0);
    const net = grossProfit + grossLoss + breakeven.reduce((a, p) => a + Number(p.realized_points || 0), 0);
    const winRate = closed.length ? (wins.length / closed.length) * 100 : 0;
    const pending = Math.max(0, totalSignals - closed.length - running.length);
    const title = calendarMonth ? "THIS MONTH GOLD REPORT" : "LAST 7 DAYS GOLD REPORT";
    const periodLabel = calendarMonth
      ? `${fmtDate(startDate).split(",")[0]} – ${fmtDate(new Date()).split(",")[0]}`
      : `${fmtDate(startDate).split(",")[0]} – ${fmtDate(new Date()).split(",")[0]}`;

    const timeframeOrder = ["5M", "15M", "30M", "1H", "4H"];
    const tfLines = timeframeOrder.map(tf => {
      const count = signals.filter(s => String(s.timeframe) === tf).length;
      return `${tf.padEnd(5, " ")} : **${count} Signals**`;
    }).filter((_, i) => signals.some(s => String(s.timeframe) === timeframeOrder[i]));

    const lines = [
      `🥇 **${title}**`,
      `━━━━━━━━━━━━━━━━━━━━`,
      ``,
      `📅 **PERIOD**`,
      `*${periodLabel}*`,
      ``,
      `📊 **SIGNAL SUMMARY**`,
      ``,
      `Total Signals : **${totalSignals}**`,
      `Closed        : **${closed.length}**`,
      `Running       : **${running.length}**`,
      ...(pending ? [`Pending       : **${pending}**`] : []),
      ``,
      `🟢 Profitable : **${wins.length}**`,
      `🔴 Losing     : **${losses.length}**`,
      `⚪ Break-even : **${breakeven.length}**`,
      ``,
      `📈 **Win Rate : ${winRate.toFixed(1)}%**`,
      ``,
      `━━━━━━━━━━━━━━━━━━━━`,
      `💰 **PERFORMANCE**`,
      `━━━━━━━━━━━━━━━━━━━━`,
      ``,
      `Gross Profit : **${signedPoints(grossProfit)} Points**`,
      `Gross Loss   : **${signedPoints(grossLoss)} Points**`,
      ``,
      `🏆 **NET RESULT : ${signedPoints(net)} Points**`,
      ``,
      `━━━━━━━━━━━━━━━━━━━━`,
      `🎯 **TARGET PERFORMANCE**`,
      `━━━━━━━━━━━━━━━━━━━━`,
      ``,
      `TP1 Hit : **${closed.filter(p => Number(p.tp1_hit) === 1).length}**`,
      `TP2 Hit : **${closed.filter(p => Number(p.tp2_hit) === 1).length}**`,
      `TP3 Hit : **${closed.filter(p => Number(p.tp3_hit) === 1).length}**`,
      `SL Hit  : **${closed.filter(p => String(p.close_reason) === "Direct SL").length}**`,
      `Protected/Reversal Exit : **${closed.filter(p => String(p.close_reason) === "Reversal Close").length}**`,
      ``,
      `━━━━━━━━━━━━━━━━━━━━`,
      `📊 **TIMEFRAME BREAKDOWN**`,
      `━━━━━━━━━━━━━━━━━━━━`,
      ``,
      ...(tfLines.length ? tfLines : [`No signals in this period.`]),
      ``,
      `━━━━━━━━━━━━━━━━━━━━`,
      `📌 **REPORT RESULT**`,
      `━━━━━━━━━━━━━━━━━━━━`,
      ``,
      net > 0 ? `✅ *Profitable Period*` : net < 0 ? `❌ *Loss-Making Period*` : `⚪ *Break-even Period*`,
      `📈 Net Result : **${signedPoints(net)} Points**`,
      `📊 Win Rate   : **${winRate.toFixed(1)}%**`,
      ``,
      `_Closed/Running figures refer to signals generated during the selected period._`
    ];

    return this.sendText(chatId, lines.join("\n"));
  }

  async sendText(chatId, text) { return tg(this.env, "sendMessage", { chat_id: chatId, text, disable_web_page_preview: true }); }
}

function parseCandle(x) {
  return { ts: Number(x?.[0]), o: Number(x?.[1]), h: Number(x?.[2]), l: Number(x?.[3]), c: Number(x?.[4]), v: Number(x?.[5] || 0), confirm: Number(x?.[8] || 0) };
}

function buildGoldSetup(candles, price, timeframe, allCandles = {}) {
  const rows = candles.slice(-120);
  if (rows.length < 45 || !(price > 0)) return null;

  const closes = rows.map(x => x.c);
  const highs = rows.map(x => x.h);
  const lows = rows.map(x => x.l);
  const vols = rows.map(x => x.v);
  const e9 = ema(closes, 9), e21 = ema(closes, 21), e50 = ema(closes, 50);
  const rv = rsi(closes, 14), atrv = atr(rows, 14);
  if (![e9, e21, e50, rv, atrv].every(Number.isFinite) || atrv <= 0) return null;

  const setup = detectGoldSetup(rows, price);
  if (!setup) return null;

  let score = 50;
  const direction = setup.direction;
  const higherBias = getHigherTimeframeBias(allCandles, timeframe);

  // Trend alignment.
  if (direction === "LONG" && e9 > e21) score += 8;
  if (direction === "SHORT" && e9 < e21) score += 8;
  if (direction === "LONG" && e21 > e50) score += 7;
  if (direction === "SHORT" && e21 < e50) score += 7;
  if (higherBias === direction) score += 12;
  if (higherBias && higherBias !== direction) score -= 10;

  // Momentum confirmation.
  if (direction === "LONG" && rv >= 50 && rv <= 70) score += 7;
  if (direction === "SHORT" && rv >= 30 && rv <= 50) score += 7;
  if (direction === "LONG" && rv > 76) score -= 8;
  if (direction === "SHORT" && rv < 24) score -= 8;

  const avgVol = vols.slice(-21, -1).reduce((a, b) => a + b, 0) / Math.max(1, vols.slice(-21, -1).length);
  const vr = avgVol > 0 ? vols[vols.length - 1] / avgVol : 1;
  if (vr >= 1.10) score += 4;
  if (vr >= 1.40) score += 4;

  // Pattern quality bonus.
  if (["FLAG", "PENNANT", "TRIANGLE", "WEDGE", "BREAKOUT"].includes(setup.type)) score += 8;
  if (["DOUBLE", "ENGULFING", "PULLBACK", "BOUNCE"].includes(setup.type)) score += 6;
  if (setup.confirmedBreak) score += 8;
  if (timeframe === "1H") score += 3;
  if (timeframe === "4H") score += 5;
  if (score < 68) return null;

  const recentLow = Math.min(...lows.slice(-15));
  const recentHigh = Math.max(...highs.slice(-15));
  const buffer = Math.max(0.35, atrv * 0.12);
  const zoneFactor = { "5M": 0.16, "15M": 0.22, "30M": 0.26, "1H": 0.30, "4H": 0.34 }[timeframe] || 0.22;
  const zoneWidth = clamp(atrv * zoneFactor, 0.60, 3.50);

  let center = Number(setup.anchor || price);
  if (!(center > 0)) center = price;
  center = clamp(center, price - atrv * 0.9, price + atrv * 0.9);

  let entryLow = center - zoneWidth / 2;
  let entryHigh = center + zoneWidth / 2;

  // For continuation/breakout setups, do not put the zone too far behind price.
  if (setup.confirmedBreak) {
    if (direction === "LONG") entryLow = Math.max(entryLow, price - atrv * 0.35);
    else entryHigh = Math.min(entryHigh, price + atrv * 0.35);
  }

  let sl;
  if (direction === "LONG") {
    sl = Math.min(Number(setup.stopAnchor || recentLow) - buffer, entryLow - buffer);
  } else {
    sl = Math.max(Number(setup.stopAnchor || recentHigh) + buffer, entryHigh + buffer);
  }

  entryLow = Math.max(0.01, entryLow);
  entryHigh = Math.max(entryLow, entryHigh);
  const entryMid = (entryLow + entryHigh) / 2;

  // Timeframe-aware structural SL: calculate the SL from chart structure first,
  // then ACCEPT the setup only if the natural structural risk fits that
  // timeframe's range. Do not artificially widen/tighten the SL.
  // This keeps 5M scalps tight while allowing larger timeframes proportionally
  // more room, without turning the SL into a fixed/random distance.
  const SL_RANGES = {
    "5M":  { min: 3, max: 5 },
    "15M": { min: 4, max: 7 },
    "30M": { min: 5, max: 8 },
    "1H":  { min: 6, max: 10 },
    "4H":  { min: 7, max: 12 }
  };
  const slRange = SL_RANGES[timeframe] || { min: 5, max: 8 };
  const risk = Math.abs(entryMid - sl);

  // If the chart's natural structural SL is outside the timeframe range,
  // reject the setup instead of moving the SL away from the actual structure.
  if (!(risk >= slRange.min && risk <= slRange.max)) return null;

  // Fixed Gold targets: 5M = 5 / 8 / 13; 15M+ = 7 / 13 / 22.
  const is5M = timeframe === "5M";
  const tp1Dist = is5M ? CFG.TP1_POINTS_5M : CFG.TP1_POINTS;
  const tp2Dist = is5M ? CFG.TP2_POINTS_5M : CFG.TP2_POINTS;
  const tp3Dist = is5M ? CFG.TP3_POINTS_5M : CFG.TP3_POINTS;

  const tp1 = direction === "LONG" ? entryMid + tp1Dist : entryMid - tp1Dist;
  const tp2 = direction === "LONG" ? entryMid + tp2Dist : entryMid - tp2Dist;
  const tp3 = direction === "LONG" ? entryMid + tp3Dist : entryMid - tp3Dist;
  if (direction === "LONG" && !(sl < entryLow && tp1 < tp2 && tp2 < tp3)) return null;
  if (direction === "SHORT" && !(sl > entryHigh && tp1 > tp2 && tp2 > tp3)) return null;

  return {
    direction,
    entryLow,
    entryHigh,
    entryMid,
    entry: `${fmt(entryLow)} – ${fmt(entryHigh)}`,
    slPrice: sl,
    sl: fmt(sl),
    tp1Price: tp1,
    tp2Price: tp2,
    tp3Price: tp3,
    tp1: fmt(tp1),
    tp2: fmt(tp2),
    tp3: fmt(tp3),
    setup: setup.name,
    pattern: setup.pattern || setup.name,
    timeframe,
    score,
    atr: atrv
  };
}

function getHigherTimeframeBias(allCandles, timeframe) {
  const order = ["15M", "30M", "1H", "4H"];
  const idx = order.indexOf(timeframe);
  if (idx < 0) return null;
  const higher = order.slice(idx + 1);
  const votes = [];
  for (const tf of higher) {
    const rows = (allCandles[tf] || []).slice(-80);
    if (rows.length < 35) continue;
    const c = rows.map(x => x.c);
    const e21 = ema(c, 21), e50 = ema(c, 50);
    if (!Number.isFinite(e21) || !Number.isFinite(e50)) continue;
    if (e21 > e50) votes.push("LONG");
    if (e21 < e50) votes.push("SHORT");
  }
  if (!votes.length) return null;
  const longs = votes.filter(x => x === "LONG").length;
  const shorts = votes.length - longs;
  if (longs > shorts) return "LONG";
  if (shorts > longs) return "SHORT";
  return null;
}

function detectGoldSetup(rows, price) {
  const n = rows.length;
  if (n < 40) return null;
  const c = rows.map(x => x.c), h = rows.map(x => x.h), l = rows.map(x => x.l), v = rows.map(x => x.v);
  const last = c[n - 1], prev = c[n - 2], prev2 = c[n - 3];
  const atrv = atr(rows, 14);
  if (!(atrv > 0)) return null;

  const high20 = Math.max(...h.slice(-21, -1));
  const low20 = Math.min(...l.slice(-21, -1));
  const recentHigh = Math.max(...h.slice(-12, -1));
  const recentLow = Math.min(...l.slice(-12, -1));

  // Classic horizontal breakout.
  if (last > high20 && prev <= high20) return { name: "Bullish Breakout", pattern: "Breakout", direction: "LONG", type: "BREAKOUT", anchor: high20, stopAnchor: recentLow, confirmedBreak: true };
  if (last < low20 && prev >= low20) return { name: "Bearish Breakdown", pattern: "Breakdown", direction: "SHORT", type: "BREAKOUT", anchor: low20, stopAnchor: recentHigh, confirmedBreak: true };

  // W / M (double bottom / double top) with neckline break confirmation.
  const aLow = Math.min(...l.slice(-18, -9)), bLow = Math.min(...l.slice(-9, -2));
  const aHigh = Math.max(...h.slice(-18, -9)), bHigh = Math.max(...h.slice(-9, -2));
  const necklineW = Math.max(...h.slice(-18, -2));
  const necklineM = Math.min(...l.slice(-18, -2));
  if (Math.abs(aLow - bLow) <= atrv * 0.65 && last > necklineW * 0.998) {
    return { name: "W Pattern / Double Bottom", pattern: "W", direction: "LONG", type: "DOUBLE", anchor: necklineW, stopAnchor: Math.min(aLow, bLow), confirmedBreak: last > necklineW };
  }
  if (Math.abs(aHigh - bHigh) <= atrv * 0.65 && last < necklineM * 1.002) {
    return { name: "M Pattern / Double Top", pattern: "M", direction: "SHORT", type: "DOUBLE", anchor: necklineM, stopAnchor: Math.max(aHigh, bHigh), confirmedBreak: last < necklineM };
  }

  // Flag / pennant: impulse followed by compact consolidation and breakout.
  const impulse = c[n - 9] - c[n - 25];
  const consHigh = Math.max(...h.slice(-9, -1)), consLow = Math.min(...l.slice(-9, -1));
  const consRange = consHigh - consLow;
  const priorRange = Math.max(...h.slice(-25, -9)) - Math.min(...l.slice(-25, -9));
  const compact = consRange <= priorRange * 0.55;
  if (Math.abs(impulse) >= atrv * 2.2 && compact) {
    if (last > consHigh && impulse > 0) return { name: "Bullish Flag Breakout", pattern: "Flag", direction: "LONG", type: "FLAG", anchor: consHigh, stopAnchor: consLow, confirmedBreak: true };
    if (last < consLow && impulse < 0) return { name: "Bearish Flag Breakdown", pattern: "Flag", direction: "SHORT", type: "FLAG", anchor: consLow, stopAnchor: consHigh, confirmedBreak: true };
  }

  // Pennant / triangle: contracting range followed by breakout.
  const r1 = Math.max(...h.slice(-18, -9)) - Math.min(...l.slice(-18, -9));
  const r2 = Math.max(...h.slice(-9, -1)) - Math.min(...l.slice(-9, -1));
  const contracting = r2 < r1 * 0.72;
  const triHigh = Math.max(...h.slice(-9, -1)), triLow = Math.min(...l.slice(-9, -1));
  if (contracting) {
    if (last > triHigh) return { name: "Bullish Triangle Breakout", pattern: "Triangle", direction: "LONG", type: "TRIANGLE", anchor: triHigh, stopAnchor: triLow, confirmedBreak: true };
    if (last < triLow) return { name: "Bearish Triangle Breakdown", pattern: "Triangle", direction: "SHORT", type: "TRIANGLE", anchor: triLow, stopAnchor: triHigh, confirmedBreak: true };
  }

  // Rising/falling wedge: narrowing range plus opposing slope breakout.
  const highSlope = slope(h.slice(-12)), lowSlope = slope(l.slice(-12));
  const wedgeRangeNow = Math.max(...h.slice(-6)) - Math.min(...l.slice(-6));
  const wedgeRangePrev = Math.max(...h.slice(-18, -6)) - Math.min(...l.slice(-18, -6));
  if (wedgeRangeNow < wedgeRangePrev * 0.70) {
    if (highSlope < 0 && lowSlope < 0 && last > recentHigh) return { name: "Falling Wedge Breakout", pattern: "Wedge", direction: "LONG", type: "WEDGE", anchor: recentHigh, stopAnchor: recentLow, confirmedBreak: true };
    if (highSlope > 0 && lowSlope > 0 && last < recentLow) return { name: "Rising Wedge Breakdown", pattern: "Wedge", direction: "SHORT", type: "WEDGE", anchor: recentLow, stopAnchor: recentHigh, confirmedBreak: true };
  }

  // Trend pullback / continuation.
  const e9 = ema(c, 9), e21 = ema(c, 21);
  const pullbackBand = atrv * 0.35;
  if (e9 > e21 && Math.abs(last - e9) <= pullbackBand && last > prev && prev <= prev2) {
    return { name: "Bullish Trend Pullback", pattern: "Trend Pullback", direction: "LONG", type: "PULLBACK", anchor: e9, stopAnchor: recentLow, confirmedBreak: false };
  }
  if (e9 < e21 && Math.abs(last - e9) <= pullbackBand && last < prev && prev >= prev2) {
    return { name: "Bearish Trend Pullback", pattern: "Trend Pullback", direction: "SHORT", type: "PULLBACK", anchor: e9, stopAnchor: recentHigh, confirmedBreak: false };
  }

  // Support bounce / resistance rejection.
  const nearLow = Math.abs(price - recentLow) <= atrv * 0.55;
  const nearHigh = Math.abs(price - recentHigh) <= atrv * 0.55;
  if (nearLow && last > prev && last > prev2) return { name: "Support Bounce", pattern: "Support Bounce", direction: "LONG", type: "BOUNCE", anchor: recentLow, stopAnchor: recentLow, confirmedBreak: false };
  if (nearHigh && last < prev && last < prev2) return { name: "Resistance Rejection", pattern: "Resistance Rejection", direction: "SHORT", type: "BOUNCE", anchor: recentHigh, stopAnchor: recentHigh, confirmedBreak: false };

  // Engulfing reversal near a meaningful recent level.
  const bullEngulf = prev < prev2 && last > prev2 && last > prev && Math.abs(last - prev) > atrv * 0.45;
  const bearEngulf = prev > prev2 && last < prev2 && last < prev && Math.abs(last - prev) > atrv * 0.45;
  if (bullEngulf && nearLow) return { name: "Bullish Engulfing", pattern: "Engulfing", direction: "LONG", type: "ENGULFING", anchor: recentLow, stopAnchor: recentLow, confirmedBreak: false };
  if (bearEngulf && nearHigh) return { name: "Bearish Engulfing", pattern: "Engulfing", direction: "SHORT", type: "ENGULFING", anchor: recentHigh, stopAnchor: recentHigh, confirmedBreak: false };

  return null;
}

function slope(values) {
  const n = values.length;
  if (n < 2) return 0;
  const meanX = (n - 1) / 2;
  const meanY = values.reduce((a, b) => a + b, 0) / n;
  let num = 0, den = 0;
  for (let i = 0; i < n; i++) { num += (i - meanX) * (values[i] - meanY); den += (i - meanX) ** 2; }
  return den ? num / den : 0;
}

function signalFingerprint(s, candleTs) {
  // Fingerprint the market setup/candle, not the moving live entry price.
  // This makes the same closed candle impossible to become a new signal just
  // because the ticker moved slightly.
  return [candleTs, s.timeframe, s.direction, s.setup].join("|");
}

function ema(a, p) {
  if (a.length < p) return NaN;
  const k = 2 / (p + 1);
  let e = a.slice(0, p).reduce((x, y) => x + y, 0) / p;
  for (let i = p; i < a.length; i++) e = a[i] * k + e * (1 - k);
  return e;
}

function rsi(a, p) {
  if (a.length <= p) return NaN;
  let g = 0, l = 0;
  for (let i = 1; i <= p; i++) { const d = a[i] - a[i - 1]; if (d >= 0) g += d; else l -= d; }
  let ag = g / p, al = l / p;
  for (let i = p + 1; i < a.length; i++) {
    const d = a[i] - a[i - 1];
    ag = ((ag * (p - 1)) + (d > 0 ? d : 0)) / p;
    al = ((al * (p - 1)) + (d < 0 ? -d : 0)) / p;
  }
  if (al === 0) return 100;
  return 100 - (100 / (1 + ag / al));
}

function atr(rows, p) {
  if (rows.length <= p) return NaN;
  const tr = [];
  for (let i = 1; i < rows.length; i++) tr.push(Math.max(rows[i].h - rows[i].l, Math.abs(rows[i].h - rows[i - 1].c), Math.abs(rows[i].l - rows[i - 1].c)));
  return tr.slice(-p).reduce((a, b) => a + b, 0) / p;
}

function clamp(v, a, b) { return Math.max(a, Math.min(b, v)); }
function fmt(v) { const n = Number(v); return Number.isFinite(n) && n > 0 ? n.toFixed(2) : "N/A"; }
function fmtDate(v) {
  try {
    return new Date(v).toLocaleString("en-IN", {
      timeZone: "Asia/Kolkata",
      day: "numeric",
      month: "short",
      year: "numeric",
      hour: "numeric",
      minute: "2-digit",
      hour12: true
    });
  } catch (_) {
    return String(v || "N/A");
  }
}

function toCommunityPrice(okxPrice) {
  const n = Number(okxPrice);
  return Number.isFinite(n) && n > 0 ? n + CFG.GOLD_PRICE_OFFSET : 0;
}

function applyCommunityPrice(s) {
  const offset = CFG.GOLD_PRICE_OFFSET;
  const out = { ...s };
  out.entryLow = Number(s.entryLow) + offset;
  out.entryHigh = Number(s.entryHigh) + offset;
  out.entryMid = Number(s.entryMid) + offset;
  out.slPrice = Number(s.slPrice) + offset;
  out.tp1Price = Number(s.tp1Price) + offset;
  out.tp2Price = Number(s.tp2Price) + offset;
  out.tp3Price = Number(s.tp3Price) + offset;
  out.entry = `${fmt(out.entryLow)} — ${fmt(out.entryHigh)}`;
  out.sl = fmt(out.slPrice);
  out.tp1 = fmt(out.tp1Price);
  out.tp2 = fmt(out.tp2Price);
  out.tp3 = fmt(out.tp3Price);
  return out;
}

function formatSignal(s) {
  const direction = s.direction === "LONG";
  const dirLine = direction ? "**🟢 ══ 📈 𝗕𝗨𝗬 ══**" : "**🔴 ══ 📉 𝗦𝗘𝗟𝗟 ══**";
  return `**🥇 GOLD (XAUUSD) SIGNAL**\n\n${dirLine}\n\n📍 **Entry Zone:** **${s.entry}**\n\n🎯 **TP1:** **${s.tp1}**\n\n🎯 **TP2:** **${s.tp2}**\n\n🎯 **TP3:** **${s.tp3}**\n\n🛑 **Stop Loss:** **${s.sl}**\n\n📊 *Setup: ${s.setup} — ${s.timeframe}*\n\n*Move your SL to entry after the 1st TP is hit.*\n\n#XAUUSD #Gold #GoldSignal #Forex #Trading`;
}

function formatCloseMessage(pos, directSl, closePrice, points, hits) {
  if (directSl) {
    return `**🤦‍♂️ STOP LOSS HIT**\n\n**Entry:** **${fmt(pos.entry_mid)}**\n**Current:** **${fmt(closePrice)}**\n**Points:** **${points >= 0 ? "+" : ""}${points.toFixed(2)}**\n\n**🔄 Next Signal — Coming Soon**`;
  }

  const tpLines = [];
  if (hits.tp1) tpLines.push("**✅ TP1 HIT 😎**");
  if (hits.tp2) tpLines.push("**✅ TP2 HIT 🤩**");
  if (hits.tp3) tpLines.push("**✅ TP3 HIT 🏆**");
  return `**🏆 POSITION CLOSED**\n\n**Entry:** **${fmt(pos.entry_mid)}**\n**Closed:** **${fmt(closePrice)}**\n**Points:** **${points >= 0 ? "+" : ""}${points.toFixed(2)}**${tpLines.length ? `\n\n${tpLines.join("\n")}` : ""}\n\n**🔄 Next Signal — Coming Soon**`;
}

function signedPoints(v) {
  const n = Number(v);
  if (!Number.isFinite(n)) return "0.0";
  return `${n >= 0 ? "+" : ""}${n.toFixed(1)}`;
}

function escapeMarkdown(v) {
  return String(v || "").replace(/([_*`])/g, "\\$1");
}

async function sendChannel(env, text) { return tg(env, "sendMessage", { chat_id: env.CHANNEL_CHAT_ID || CFG.CHANNEL_CHAT_ID, text, disable_web_page_preview: true }); }
async function sendReply(env, text, id) {
  const key = `reply:${Number(id) || 0}:${String(text).slice(0, 220)}`;
  if (telegramInFlightKeys.has(key)) return null;
  telegramInFlightKeys.add(key);
  try {
    return await tg(env, "sendMessage", { chat_id: env.CHANNEL_CHAT_ID || CFG.CHANNEL_CHAT_ID, text, reply_to_message_id: Number(id), allow_sending_without_reply: true, disable_web_page_preview: true });
  } finally {
    telegramInFlightKeys.delete(key);
  }
}

// Telegram safety queue: serialize outbound requests in the current Worker isolate,
// keep a small gap between messages, and honor Telegram's retry_after on HTTP 429.
// This prevents a burst of TP/SL/report notifications from turning into a retry storm.
let telegramQueue = Promise.resolve();
let telegramNextAt = 0;
// Emergency duplicate-send guards. A notification key may be in-flight only once.
const telegramInFlightKeys = new Set();

function enqueueTelegram(task) {
  const run = telegramQueue.then(task, task);
  telegramQueue = run.catch(() => null);
  return run;
}

async function tg(env, method, body) {
  if (!env.TELEGRAM_BOT_TOKEN) return null;
  return enqueueTelegram(async () => {
    const payload = method === "sendMessage"
      ? { ...body, parse_mode: body?.parse_mode || "Markdown" }
      : body;

    const maxAttempts = 4;
    for (let attempt = 1; attempt <= maxAttempts; attempt++) {
      try {
        const wait = Math.max(0, telegramNextAt - Date.now());
        if (wait > 0) await sleep(wait);

        const r = await fetch("https://api.telegram.org/bot" + env.TELEGRAM_BOT_TOKEN + "/" + method, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify(payload)
        });

        const p = await r.json();

        if (r.ok) {
          // Keep messages gently spaced even when Telegram accepts them quickly.
          telegramNextAt = Date.now() + 350;
          return p.result || p;
        }

        if (r.status === 429) {
          const retryAfter = Math.max(1, Number(p?.parameters?.retry_after || 3));
          console.error("TELEGRAM RATE LIMIT", method, "retry_after=" + retryAfter + "s", "attempt=" + attempt);
          telegramNextAt = Date.now() + retryAfter * 1000 + 250;
          if (attempt < maxAttempts) {
            await sleep(retryAfter * 1000 + 250);
            continue;
          }
        }

        console.error("TELEGRAM ERROR", method, r.status, p);
        return null;
      } catch (e) {
        console.error("TELEGRAM FETCH ERROR", method, e);
        if (attempt < maxAttempts) {
          telegramNextAt = Date.now() + 1000;
          await sleep(1000);
          continue;
        }
        return null;
      }
    }
    return null;
  });
}
function sleep(ms) { return new Promise(resolve => setTimeout(resolve, ms)); }
function json(x) { return new Response(JSON.stringify(x), { headers: { "content-type": "application/json" } }); }
