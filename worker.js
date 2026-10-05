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
  TP1_POINTS: 8,
  TP2_POINTS: 15,
  TP3_POINTS: 25,
  REVERSAL_MIN_FAVORABLE_POINTS: 5,
  // 5M is execution/entry timing only. Signals are generated from 15M+ structure.
  TIMEFRAMES: [
    { key: "5M", channel: "candle5m", bar: "5m", history: 180, signal: false },
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
    this.monitorInFlight = false;
    this.pendingMonitorPrice = 0;
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
      this.lastOkxPrice = candle.c;
      this.lastPrice = toCommunityPrice(candle.c);
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
    if (this.lastPrice > 0) await this.monitorPositions(this.lastPrice);
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
          tp1_hit: 0, tp2_hit: 0, tp3_hit: 0, highest_price: setup.entryMid, lowest_price: setup.entryMid,
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

    // SINGLE-FLIGHT MONITOR: OKX can emit many ticks while Telegram awaits.
    // Never allow two monitor runs to inspect/update the same position concurrently.
    if (this.monitorInFlight) {
      this.pendingMonitorPrice = price;
      return;
    }

    this.monitorInFlight = true;
    try {
      if (!this.openPositions.size) return;
      await this._monitorPositionsOnce(price);
    } finally {
      this.monitorInFlight = false;
      const pending = Number(this.pendingMonitorPrice || 0);
      this.pendingMonitorPrice = 0;
      if (pending > 0 && pending !== price && this.openPositions.size) {
        // Process only the latest tick after the current run finishes.
        this.monitorPositions(pending).catch(e => console.error("GOLD MONITOR ERROR", e));
      }
    }
  }

  async _monitorPositionsOnce(price) {
    if (!(price > 0) || !this.openPositions.size) return;

    // Price passed here is already converted to the community/MT5 reference scale.
    // Do not write high/low to SQLite on ordinary ticks.
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
      const peakPoints = direction === "LONG"
        ? high - Number(pos.entry_mid)
        : Number(pos.entry_mid) - low;
      const favorableEnough = peakPoints >= CFG.REVERSAL_MIN_FAVORABLE_POINTS;

      // Preserve the old protection idea, but report the final close from the
      // best favorable/reversal price whenever the trade has moved >=5 points.
      let effectiveStop = sl;
      if (tp1 || tp2) effectiveStop = Number(pos.entry_mid);
      if (tp3) {
        if (direction === "LONG" && high > Number(pos.entry_mid)) {
          effectiveStop = Number(pos.entry_mid) + (high - Number(pos.entry_mid)) * 0.80;
        }
        if (direction === "SHORT" && low < Number(pos.entry_mid)) {
          effectiveStop = Number(pos.entry_mid) - (Number(pos.entry_mid) - low) * 0.80;
        }
      }

      const protectedClose = (tp1 || tp2) && (direction === "LONG" ? price <= effectiveStop : price >= effectiveStop);
      const trailingClose = tp3 && (direction === "LONG" ? price <= effectiveStop : price >= effectiveStop);

      // Direct SL = original SL is hit before any 5-point favorable move.
      // Any close after a >=5 point favorable move is a reversal/protected close.
      const directSl = !tp1 && !tp2 && !tp3 && slTouched && !favorableEnough;
      const reversalClose = (slTouched && favorableEnough) || protectedClose || trailingClose;
      const shouldClose = directSl || reversalClose;

      if (!reached.length && !shouldClose) continue;

      pos.tp1_hit = tp1;
      pos.tp2_hit = tp2;
      pos.tp3_hit = tp3;

      if (shouldClose) {
        // If the trade first moved >=5 points in favor, report the best favorable
        // price (MFE) as the close reference, not the later SL price.
        const closePrice = directSl
          ? price
          : (direction === "LONG" ? high : low);
        const realizedPoints = direction === "LONG"
          ? closePrice - Number(pos.entry_mid)
          : Number(pos.entry_mid) - closePrice;
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

        // If TP(s) and SL/reversal happen in the same market tick, send TP updates
        // first, then the final close message.
        for (const hit of reached) await this.sendTpHit(pos, hit);

        const msg = formatCloseMessage(pos, directSl, closePrice, realizedPoints, {
          tp1: !!tp1, tp2: !!tp2, tp3: !!tp3
        });
        if (pos.channel_message_id) await sendReply(this.env, msg, pos.channel_message_id);
        this.openPositions.delete(positionId);
        continue;
      }

      // One DB write for actual TP events only. Ordinary ticker movement stays in memory.
      this.ctx.storage.sql.exec(
        "UPDATE gold_positions SET tp1_hit=?,tp2_hit=?,tp3_hit=?,highest_price=?,lowest_price=? WHERE id=?",
        tp1, tp2, tp3, high, low, positionId
      );
      for (const hit of reached) await this.sendTpHit(pos, hit);
    }
  }

  async sendTpHit(pos, hit) {
    const entry = Number(pos.entry_mid);
    const current = Number(hit.target);
    const points = pos.direction === "LONG" ? current - entry : entry - current;
    const heading = hit.n === 1 ? "**✅ TP1 HIT 😎**" : hit.n === 2 ? "**✅ TP2 HIT 🤩**" : "**✅ TP3 HIT 🏆**";
    const next = hit.n === 1 ? "**🎯 TP2 — Coming Soon**" : hit.n === 2 ? "**🎯 TP3 — Coming Soon**" : "**🏆 Position Still Running**";
    const msg = `${heading}\n\n**Entry:** **${fmt(entry)}**\n**Current:** **${fmt(current)}**\n**Points:** **${points >= 0 ? "+" : ""}${points.toFixed(2)}**\n\n${next}`;
    if (pos.channel_message_id) await sendReply(this.env, msg, pos.channel_message_id);
  }

  async sendPositions(chatId) {
    const rows = this.ctx.storage.sql.exec("SELECT * FROM gold_positions WHERE status='OPEN' ORDER BY id ASC").toArray();
    if (!rows.length) return this.sendText(chatId, "🥇 LIVE GOLD POSITIONS\n\nNo open gold position.");

    const chunks = [];
    const price = Number(this.lastPrice || 0);
    const totalMove = rows.reduce((sum, p) => {
      const move = p.direction === "LONG" ? price - Number(p.entry_mid) : Number(p.entry_mid) - price;
      return sum + move;
    }, 0);
    const buys = rows.filter(p => p.direction === "LONG").length;
    const sells = rows.filter(p => p.direction === "SHORT").length;
    const tp1Hits = rows.filter(p => p.tp1_hit).length;
    const tp2Hits = rows.filter(p => p.tp2_hit).length;
    const tp3Hits = rows.filter(p => p.tp3_hit).length;
    const summary = [
      `🥇 LIVE GOLD POSITIONS`,
      `━━━━━━━━━━━━━━━━━━━━`,
      `📊 SUMMARY`,
      `Open Positions : ${rows.length}`,
      `🟢 BUY          : ${buys}`,
      `🔴 SELL         : ${sells}`,
      `💰 TOTAL P/L   : ${totalMove >= 0 ? "+" : ""}${totalMove.toFixed(1)} Points`,
      `📈 TOTAL P/L   : ${totalMove >= 0 ? "+" : ""}${(totalMove * 10).toFixed(0)} Pips`,
      `🎯 TP1 Hits    : ${tp1Hits}`,
      `🎯 TP2 Hits    : ${tp2Hits}`,
      `🎯 TP3 Hits    : ${tp3Hits}`,
      `💵 Current     : ${fmt(price)}`,
      `━━━━━━━━━━━━━━━━━━━━`,
      `📌 POSITION DETAILS`
    ];
    let current = summary;
    const maxChars = 3500;

    for (let i = 0; i < rows.length; i++) {
      const p = rows[i];
      const currentPrice = this.lastPrice;
      const move = p.direction === "LONG" ? currentPrice - p.entry_mid : p.entry_mid - currentPrice;
      const block = [
        `\n${String(i + 1).padStart(2, "0")} ┃ ${p.direction === "LONG" ? "🟢 BUY" : "🔴 SELL"} GOLD — ${p.timeframe || "Gold"}`,
        `━━━━━━━━━━━━━━━━━━━━`,
        `📅 Opened : ${fmtDate(p.opened_at)}`,
        `📍 Entry  : ${fmt(p.entry_low)} – ${fmt(p.entry_high)}`,
        `💰 Current: ${fmt(currentPrice)}`,
        `📊 Move   : ${move >= 0 ? "+" : ""}${move.toFixed(1)} Points`,
        `📈 Pips   : ${(move * 10 >= 0 ? "+" : "")}${(move * 10).toFixed(0)} Pips`,
        `🛑 SL     : ${fmt(p.sl_price)}`,
        `🎯 TP1    : ${fmt(p.tp1_price)}${p.tp1_hit ? " ✅" : ""}`,
        `🎯 TP2    : ${fmt(p.tp2_price)}${p.tp2_hit ? " ✅" : ""}`,
        `🎯 TP3    : ${fmt(p.tp3_price)}${p.tp3_hit ? " ✅" : ""}`,
        `📊 Setup  : ${p.setup || "Gold Setup"}`
      ].join("\n");
      if ((current.join("\n").length + block.length) > maxChars && current.length > 2) {
        chunks.push(current.join("\n"));
        current = [`🥇 LIVE GOLD POSITIONS — CONTINUED`, `📌 POSITION DETAILS`, `━━━━━━━━━━━━━━━━━━━━`];
      }
      current.push(block);
    }
    if (current.length > 2) chunks.push(current.join("\n"));

    for (let i = 0; i < chunks.length; i++) {
      await this.sendText(chatId, `${chunks[i]}\n\n📄 ${i + 1}/${chunks.length}`);
    }
    return null;
  }

  async sendReport(chatId, days, calendarMonth = false) {
    const start = calendarMonth ? new Date(new Date().getFullYear(), new Date().getMonth(), 1).toISOString() : new Date(Date.now() - days * 86400000).toISOString();
    const rows = this.ctx.storage.sql.exec("SELECT * FROM gold_positions WHERE closed_at IS NOT NULL AND closed_at >= ? ORDER BY closed_at DESC", start).toArray();
    const open = this.ctx.storage.sql.exec("SELECT COUNT(*) AS n FROM gold_positions WHERE status='OPEN'").one();
    const wins = rows.filter(r => Number(r.realized_points) > 0).length;
    const losses = rows.filter(r => Number(r.realized_points) < 0).length;
    const net = rows.reduce((a, r) => a + Number(r.realized_points || 0), 0);
    const title = calendarMonth ? "THIS MONTH GOLD REPORT" : "LAST 7 DAYS GOLD REPORT";
    const lines = [`🥇 ${title}`, "", `Closed: ${rows.length}`, `Open: ${Number(open?.n || 0)}`, `Profitable: ${wins}`, `Losing: ${losses}`,
      `Win Rate: ${rows.length ? ((wins / rows.length) * 100).toFixed(1) : "0.0"}%`, `Net: ${net >= 0 ? "+" : ""}${net.toFixed(1)} Points`,
      `Net Pips: ${net >= 0 ? "+" : ""}${(net * 10).toFixed(0)}`, `TP1 Hits: ${rows.filter(r => r.tp1_hit).length}`, `TP2 Hits: ${rows.filter(r => r.tp2_hit).length}`,
      `TP3 Hits: ${rows.filter(r => r.tp3_hit).length}`, `Direct SL: ${rows.filter(r => r.close_reason === "Direct SL").length}`, `Protected Exit: ${rows.filter(r => r.close_reason === "Protected Exit").length}`];
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
  const zoneFactor = { "15M": 0.22, "30M": 0.26, "1H": 0.30, "4H": 0.34 }[timeframe] || 0.22;
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

  // Dynamic risk: structural SL first, ATR guard second.
  let risk = Math.abs(entryMid - sl);
  const minRisk = clamp(atrv * 0.45, 5.5, 8.0);
  const maxRisk = clamp(atrv * 1.35, 10, 18);
  if (risk < minRisk || risk > maxRisk) {
    risk = clamp(risk, minRisk, maxRisk);
    sl = direction === "LONG" ? entryMid - risk : entryMid + risk;
  }
  risk = Math.abs(entryMid - sl);
  if (!(risk >= 5.5 && risk <= 18)) return null;

  // FINAL FIXED GOLD TARGETS: 8 / 15 / 25 points from Entry Mid.
  const tp1Dist = CFG.TP1_POINTS;
  const tp2Dist = CFG.TP2_POINTS;
  const tp3Dist = CFG.TP3_POINTS;

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
function fmtDate(v) { try { return new Date(v).toLocaleString("en-IN", { timeZone: "Asia/Kolkata" }); } catch (_) { return String(v || "N/A"); } }

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

async function sendChannel(env, text) { return tg(env, "sendMessage", { chat_id: env.CHANNEL_CHAT_ID || CFG.CHANNEL_CHAT_ID, text, disable_web_page_preview: true }); }
async function sendReply(env, text, id) { return tg(env, "sendMessage", { chat_id: env.CHANNEL_CHAT_ID || CFG.CHANNEL_CHAT_ID, text, reply_to_message_id: Number(id), allow_sending_without_reply: true, disable_web_page_preview: true }); }
let telegramSendChain = Promise.resolve();
let telegramNextSendAt = 0;

function enqueueTelegramSend(job) {
  const run = telegramSendChain.then(job, job);
  telegramSendChain = run.catch(() => null);
  return run;
}

async function tg(env, method, body) {
  if (!env.TELEGRAM_BOT_TOKEN) return null;
  return enqueueTelegramSend(async () => {
    // Keep channel traffic comfortably below Telegram's bot send rate.
    const wait = Math.max(0, telegramNextSendAt - Date.now());
    if (wait) await sleep(wait);

    const payload = method === "sendMessage" ? { ...body, parse_mode: body?.parse_mode || "Markdown" } : body;
    for (let attempt = 1; attempt <= 4; attempt++) {
      try {
        const r = await fetch("https://api.telegram.org/bot" + env.TELEGRAM_BOT_TOKEN + "/" + method, {
          method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(payload)
        });
        const p = await r.json();
        if (r.ok) {
          telegramNextSendAt = Date.now() + (method === "sendMessage" ? 1100 : 250);
          return p.result || p;
        }
        if (r.status === 429) {
          const retry = Math.max(1, Number(p?.parameters?.retry_after || 1));
          console.error("TELEGRAM RATE LIMIT", method, "retry_after=" + retry + "s", "attempt=" + attempt);
          telegramNextSendAt = Date.now() + retry * 1000;
          if (attempt < 4) {
            await sleep(retry * 1000);
            continue;
          }
        }
        console.error("TELEGRAM ERROR", method, r.status, p);
        telegramNextSendAt = Date.now() + 1500;
        return null;
      } catch (e) {
        console.error("TELEGRAM FETCH ERROR", e);
        telegramNextSendAt = Date.now() + 1500;
        return null;
      }
    }
    return null;
  });
}
function sleep(ms) { return new Promise(resolve => setTimeout(resolve, ms)); }
function json(x) { return new Response(JSON.stringify(x), { headers: { "content-type": "application/json" } }); }
