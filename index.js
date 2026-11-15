require("dotenv").config();

const cors = require("cors");
const WebSocket = require("ws");
const mongoose = require("mongoose");
const express = require("express");
const http = require("http");
const { Server } = require("socket.io");
const { randomUUID } = require("crypto");

const connectDB = require("./db");
const Candle = require("./models/Candle");
const Alert = require("./models/Alert");
const createMarketAnalytics = require("./analytics");

const PORT = Number(process.env.PORT || 5000);
const RETENTION_HOURS = 3;
const RETENTION_MS = RETENTION_HOURS * 60 * 60 * 1000;

const SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT"];

const STREAMS = SYMBOLS.flatMap(symbol => [
  `${symbol.toLowerCase()}@trade`,
  `${symbol.toLowerCase()}@kline_1m`
]);

const BINANCE_URL =
  `wss://stream.binance.com:9443/stream?streams=${STREAMS.join("/")}`;

const CANDLE_BATCH_SIZE = 100;
const ALERT_BATCH_SIZE = 100;
const WRITE_FLUSH_INTERVAL_MS = 2000;
const MAX_PENDING_ALERTS = 5000;

const app = express();

app.use(cors({
  origin: [
    "http://localhost:5173",
    "http://127.0.0.1:5173"
  ],
  methods: ["GET", "POST"]
}));

app.use(express.json());

const server = http.createServer(app);

const io = new Server(server, {
  cors: {
    origin: [
      "http://localhost:5173",
      "http://127.0.0.1:5173"
    ],
    methods: ["GET", "POST"]
  }
});

const analytics = createMarketAnalytics({
  priceWindowMs: 20000,
  priceThresholdPct: 0.5,
  volumeBucketMs: 10000,
  volumeMultiplier: 2.5,
  volumeStdDevMultiplier: 3,
  minVolumeBaselineBuckets: 12,
  cooldownMs: 20000
});

const market = Object.fromEntries(SYMBOLS.map(symbol => [
  symbol,
  {
    symbol,
    price: 0,
    volume10s: 0,
    trades10s: 0,
    totalEvents: 0,
    recentTrades: []
  }
]));

let binanceSocket = null;
let reconnectTimer = null;
let snapshotTimer = null;
let liveStatsTimer = null;
let writeFlushTimer = null;
let isShuttingDown = false;

const pendingCandles = new Map();
let pendingAlerts = [];

let candleFlushRunning = false;
let alertFlushRunning = false;

const metrics = {
  receivedTrades: 0,
  processedTrades: 0,
  receivedFinalCandles: 0,
  candleBatchesWritten: 0,
  candleDocumentsWritten: 0,
  alertBatchesWritten: 0,
  alertDocumentsWritten: 0,
  databaseErrors: 0,
  lastCandleFlushAt: null,
  lastAlertFlushAt: null
};


// -------------------------------------
// MARKET SNAPSHOT
// -------------------------------------

function marketSnapshot() {
  return Object.fromEntries(SYMBOLS.map(symbol => {
    const state = market[symbol];

    return [symbol, {
      symbol,
      price: state.price,
      volume10s: state.volume10s,
      trades10s: state.trades10s,
      totalEvents: state.totalEvents
    }];
  }));
}


// -------------------------------------
// NORMALIZE BINANCE TRADES
// -------------------------------------

function normalizeTrade(raw) {
  const price = Number(raw.p);
  const quantity = Number(raw.q);
  const tradeId = Number(raw.t);
  const time = Number(raw.T);

  if (
    !SYMBOLS.includes(raw.s) ||
    !Number.isSafeInteger(tradeId) ||
    !Number.isFinite(price) ||
    price <= 0 ||
    !Number.isFinite(quantity) ||
    quantity <= 0 ||
    !Number.isFinite(time)
  ) {
    return null;
  }

  return {
    symbol: raw.s,
    tradeId,
    price,
    quantity,
    tradeTime: new Date(time),
    isBuyerMaker: Boolean(raw.m)
  };
}


// -------------------------------------
// PROCESS LIVE TRADES IN MEMORY
// -------------------------------------

function handleTrade(raw) {
  metrics.receivedTrades++;

  const trade = normalizeTrade(raw);
  if (!trade) return;

  metrics.processedTrades++;

  const state = market[trade.symbol];

  state.price = trade.price;
  state.totalEvents++;

  state.recentTrades.push({
    time: trade.tradeTime.getTime(),
    quantity: trade.quantity
  });

  const cutoff = Date.now() - 10000;

  while (
    state.recentTrades.length &&
    state.recentTrades[0].time < cutoff
  ) {
    state.recentTrades.shift();
  }

  state.trades10s = state.recentTrades.length;

  state.volume10s = state.recentTrades.reduce(
    (sum, item) => sum + item.quantity,
    0
  );

  // Individual trades are processed in memory only.
  // They are NOT inserted into MongoDB.

  const detected = analytics.processTrade({
    symbol: trade.symbol,
    price: trade.price,
    quantity: trade.quantity,
    tradeTime: trade.tradeTime,
    tradeId: trade.tradeId
  });

  if (detected.length) {
    const uniquelyIdentifiedAlerts = detected.map(alert => ({
      ...alert,
      id: `${alert.symbol}-${alert.type}-${alert.timestamp.getTime()}-${randomUUID()}`
    }));

    pendingAlerts.push(...uniquelyIdentifiedAlerts);

    if (pendingAlerts.length > MAX_PENDING_ALERTS) {
      pendingAlerts = pendingAlerts.slice(-MAX_PENDING_ALERTS);
    }

    io.emit("market:alerts", uniquelyIdentifiedAlerts);
  }

  io.emit("market:trades", [{
    ...trade,
    eventTime: new Date(),
    source: "BINANCE"
  }]);
}


// -------------------------------------
// QUEUE FINALIZED 1-MINUTE CANDLES
// -------------------------------------

function queueFinalizedKline(kline) {
  if (
    !kline ||
    kline.x !== true ||
    !SYMBOLS.includes(kline.s)
  ) {
    return;
  }

  const candleTime = new Date(Number(kline.t));

  const values = {
    symbol: kline.s,
    interval: "1m",
    candleTime,

    open: Number(kline.o),
    high: Number(kline.h),
    low: Number(kline.l),
    close: Number(kline.c),

    volume: Number(kline.v),
    quoteVolume: Number(kline.q),
    trades: Number(kline.n),

    updatedAt: new Date()
  };

  if (
    !Number.isFinite(candleTime.getTime()) ||
    ![
      values.open,
      values.high,
      values.low,
      values.close,
      values.volume,
      values.quoteVolume,
      values.trades
    ].every(Number.isFinite)
  ) {
    return;
  }

  metrics.receivedFinalCandles++;

  const key =
    `${values.symbol}|${values.interval}|${candleTime.getTime()}`;

  pendingCandles.set(key, values);
}


// -------------------------------------
// BATCH WRITE CANDLES
// -------------------------------------

async function flushCandleWrites() {
  if (
    candleFlushRunning ||
    pendingCandles.size === 0
  ) {
    return;
  }

  candleFlushRunning = true;

  const batch = Array.from(
    pendingCandles.entries()
  ).slice(0, CANDLE_BATCH_SIZE);

  batch.forEach(([key]) => {
    pendingCandles.delete(key);
  });

  const operations = batch.map(([, values]) => ({
    updateOne: {
      filter: {
        symbol: values.symbol,
        interval: values.interval,
        candleTime: values.candleTime
      },

      update: {
        $set: values
      },

      upsert: true
    }
  }));

  try {
    await Candle.bulkWrite(
      operations,
      { ordered: false }
    );

    metrics.candleBatchesWritten++;
    metrics.candleDocumentsWritten += batch.length;
    metrics.lastCandleFlushAt = new Date();

  } catch (error) {
    metrics.databaseErrors++;

    console.error(
      "Candle bulkWrite failed:",
      error.message
    );

    // Requeue failed items unless a newer update
    // for that candle has already arrived.

    for (const [key, values] of batch) {
      if (!pendingCandles.has(key)) {
        pendingCandles.set(key, values);
      }
    }

  } finally {
    candleFlushRunning = false;
  }
}


// -------------------------------------
// BATCH WRITE ALERTS
// -------------------------------------

async function flushAlertWrites() {
  if (
    alertFlushRunning ||
    pendingAlerts.length === 0
  ) {
    return;
  }

  alertFlushRunning = true;

  const batch = pendingAlerts.splice(
    0,
    ALERT_BATCH_SIZE
  );

  const operations = batch.map(alert => ({
    updateOne: {
      filter: {
        alertId: String(alert.id)
      },

      update: {
        $setOnInsert: {
          alertId: String(alert.id),

          type: alert.type,
          symbol: alert.symbol,
          message: alert.message,
          severity: alert.severity,

          observedValue: Number(alert.observedValue),
          baselineValue: Number(alert.baselineValue),

          changePct:
            alert.changePct == null
              ? null
              : Number(alert.changePct),

          timestamp: alert.timestamp || new Date()
        }
      },

      upsert: true
    }
  }));

  try {
    await Alert.bulkWrite(
      operations,
      { ordered: false }
    );

    metrics.alertBatchesWritten++;
    metrics.alertDocumentsWritten += batch.length;
    metrics.lastAlertFlushAt = new Date();

  } catch (error) {
    metrics.databaseErrors++;

    console.error(
      "Alert bulkWrite failed:",
      error.message
    );

    pendingAlerts = batch
      .concat(pendingAlerts)
      .slice(-MAX_PENDING_ALERTS);

  } finally {
    alertFlushRunning = false;
  }
}


// -------------------------------------
// FLUSH ALL DATABASE QUEUES
// -------------------------------------

async function flushAllWrites() {
  await Promise.all([
    flushCandleWrites(),
    flushAlertWrites()
  ]);
}


// -------------------------------------
// ENSURE 3-HOUR TTL INDEXES
// -------------------------------------

async function ensureThreeHourTTL(model, field) {
  try {
    await model.createCollection();

  } catch (error) {
    if (
      error.code !== 48 &&
      error.codeName !== "NamespaceExists"
    ) {
      throw error;
    }
  }

  const indexes = await model.collection.indexes();

  const existing = indexes.find(index =>
    index.key &&
    Object.keys(index.key).length === 1 &&
    index.key[field] === 1
  );

  if (existing) {
    await mongoose.connection.db.command({
      collMod: model.collection.collectionName,

      index: {
        name: existing.name,
        expireAfterSeconds: 10800
      }
    });
  }
}


// -------------------------------------
// BINANCE WEBSOCKET
// -------------------------------------

function connectBinance() {
  if (isShuttingDown) return;

  console.log("Connecting to Binance streams...");

  binanceSocket = new WebSocket(BINANCE_URL);

  binanceSocket.on("open", () => {
    console.log("Binance WebSocket connected.");

    console.log(
      "Tracking:",
      SYMBOLS.join(", ")
    );
  });

  binanceSocket.on("message", raw => {
    try {
      const message = JSON.parse(raw.toString());
      const data = message.data;

      if (!data) return;

      if (data.k) {
        queueFinalizedKline(data.k);

      } else if (
        data.e === "trade" ||
        data.p !== undefined
      ) {
        handleTrade(data);
      }

    } catch (error) {
      console.error(
        "Binance message processing error:",
        error.message
      );
    }
  });

  binanceSocket.on("error", error => {
    console.error(
      "Binance WebSocket error:",
      error.message
    );
  });

  binanceSocket.on("close", () => {
    if (isShuttingDown) return;

    console.warn(
      "Binance connection closed. Reconnecting in 5 seconds."
    );

    if (!reconnectTimer) {
      reconnectTimer = setTimeout(() => {
        reconnectTimer = null;
        connectBinance();
      }, 5000);
    }
  });
}


// -------------------------------------
// HEALTH CHECK
// -------------------------------------

app.get("/health", (_req, res) => {
  res.json({
    status: "running",

    database:
      mongoose.connection.readyState === 1
        ? "connected"
        : "disconnected",

    connectedClients: io.engine.clientsCount,
    timestamp: new Date()
  });
});


// -------------------------------------
// SYSTEM STATUS
// -------------------------------------

app.get("/api/system/status", (_req, res) => {
  res.json({
    status: "running",

    database:
      mongoose.connection.readyState === 1
        ? "connected"
        : "disconnected",

    symbols: SYMBOLS,
    retentionHours: RETENTION_HOURS,

    storageMode:
      "Finalized 1-minute candles; raw trades in memory only",

    batching: {
      candleBatchSize: CANDLE_BATCH_SIZE,
      alertBatchSize: ALERT_BATCH_SIZE,
      flushIntervalMs: WRITE_FLUSH_INTERVAL_MS,

      pendingCandles: pendingCandles.size,
      pendingAlerts: pendingAlerts.length,

      candleFlushRunning,
      alertFlushRunning
    },

    metrics,

    connectedClients: io.engine.clientsCount,
    timestamp: new Date()
  });
});


// -------------------------------------
// HISTORICAL MARKET DATA API
// -------------------------------------

app.get("/api/market/history", async (req, res) => {
  try {
    const symbol = String(
      req.query.symbol || "BTCUSDT"
    ).toUpperCase();

    const range = String(
      req.query.range || "3h"
    );

    const interval = String(
      req.query.interval || "1m"
    );

    const rangeMs = {
      "1h": 60 * 60 * 1000,
      "3h": RETENTION_MS
    }[range];

    const bucket = {
      "1m": {
        unit: "minute",
        binSize: 1
      },

      "5m": {
        unit: "minute",
        binSize: 5
      },

      "15m": {
        unit: "minute",
        binSize: 15
      },

      "1h": {
        unit: "hour",
        binSize: 1
      }

    }[interval];

    if (!SYMBOLS.includes(symbol)) {
      return res.status(400).json({
        error: "Unsupported symbol."
      });
    }

    if (!rangeMs || !bucket) {
      return res.status(400).json({
        error:
          "Use range 1h or 3h and interval 1m, 5m, 15m or 1h."
      });
    }

    const to = new Date();

    const from = new Date(
      to.getTime() - rangeMs
    );

    const candles = await Candle.aggregate([
      {
        $match: {
          symbol,
          interval: "1m",

          candleTime: {
            $gte: from,
            $lte: to
          }
        }
      },

      {
        $sort: {
          candleTime: 1
        }
      },

      {
        $group: {
          _id: {
            $dateTrunc: {
              date: "$candleTime",
              unit: bucket.unit,
              binSize: bucket.binSize,
              timezone: "UTC"
            }
          },

          open: {
            $first: "$open"
          },

          high: {
            $max: "$high"
          },

          low: {
            $min: "$low"
          },

          close: {
            $last: "$close"
          },

          volume: {
            $sum: "$volume"
          },

          quoteVolume: {
            $sum: "$quoteVolume"
          },

          trades: {
            $sum: "$trades"
          }
        }
      },

      {
        $sort: {
          _id: 1
        }
      },

      {
        $project: {
          _id: 0,
          time: "$_id",
          open: 1,
          high: 1,
          low: 1,
          close: 1,
          volume: 1,
          quoteVolume: 1,
          trades: 1
        }
      }

    ]).allowDiskUse(false);

    res.json({
      symbol,
      range,
      interval,
      from,
      to,
      count: candles.length,
      candles
    });

  } catch (error) {
    console.error(
      "History API error:",
      error.message
    );

    res.status(500).json({
      error: "Unable to load candle history."
    });
  }
});


// -------------------------------------
// ALERT HISTORY API
// -------------------------------------

app.get("/api/alerts", async (req, res) => {
  try {
    const filter = {
      timestamp: {
        $gte: new Date(Date.now() - RETENTION_MS)
      }
    };

    if (req.query.symbol) {
      filter.symbol = String(
        req.query.symbol
      ).toUpperCase();
    }

    if (req.query.type) {
      filter.type = String(
        req.query.type
      ).toUpperCase();
    }

    if (req.query.severity) {
      filter.severity = String(
        req.query.severity
      ).toUpperCase();
    }

    const limit = Math.min(
      Math.max(
        Number.parseInt(req.query.limit, 10) || 50,
        1
      ),
      200
    );

    const page = Math.max(
      Number.parseInt(req.query.page, 10) || 1,
      1
    );

    const [alerts, total] = await Promise.all([
      Alert.find(filter)
        .sort({ timestamp: -1 })
        .skip((page - 1) * limit)
        .limit(limit)
        .lean(),

      Alert.countDocuments(filter)
    ]);

    res.json({
      alerts,

      pagination: {
        total,
        page,
        limit,
        pages: Math.ceil(total / limit)
      }
    });

  } catch (error) {
    console.error(
      "Alerts API error:",
      error.message
    );

    res.status(500).json({
      error: "Unable to load alerts."
    });
  }
});


// -------------------------------------
// SOCKET.IO CONNECTIONS
// -------------------------------------

io.on("connection", socket => {
  console.log(
    "Frontend connected:",
    socket.id
  );

  socket.emit(
    "market:snapshot",
    marketSnapshot()
  );

  Alert.find({
    timestamp: {
      $gte: new Date(Date.now() - RETENTION_MS)
    }
  })
    .sort({ timestamp: -1 })
    .limit(30)
    .lean()

    .then(rows => {
      socket.emit(
        "market:alerts:history",
        rows
      );
    })

    .catch(error => {
      console.error(
        "Alert history socket error:",
        error.message
      );
    });
});


// -------------------------------------
// START APPLICATION
// -------------------------------------

async function start() {
  try {
    if (!process.env.MONGODB_URI) {
      throw new Error(
        "MONGODB_URI is missing from the root .env file."
      );
    }

    await connectDB();

    await ensureThreeHourTTL(Candle, "candleTime");
    await ensureThreeHourTTL(Alert, "timestamp");

    await Promise.all([
      Candle.init(),
      Alert.init()
    ]);

    console.log(
      "MongoDB candle and alert indexes are ready."
    );

    console.log(
      "Retention: 3 hours. Raw individual trades are not persisted."
    );

    snapshotTimer = setInterval(() => {
      io.emit(
        "market:snapshot",
        marketSnapshot()
      );
    }, 2000);

    liveStatsTimer = setInterval(() => {
      const cutoff = Date.now() - 10000;

      for (const state of Object.values(market)) {
        while (
          state.recentTrades.length &&
          state.recentTrades[0].time < cutoff
        ) {
          state.recentTrades.shift();
        }

        state.trades10s = state.recentTrades.length;

        state.volume10s = state.recentTrades.reduce(
          (sum, item) => sum + item.quantity,
          0
        );
      }
    }, 1000);

    writeFlushTimer = setInterval(() => {
      void flushAllWrites();
    }, WRITE_FLUSH_INTERVAL_MS);

    server.listen(PORT, () => {
      console.log(
        `MarketPulse API running at http://localhost:${PORT}`
      );

      connectBinance();
    });

  } catch (error) {
    console.error(
      "MarketPulse startup failed:",
      error.message
    );

    process.exit(1);
  }
}


// -------------------------------------
// GRACEFUL SHUTDOWN
// -------------------------------------

async function shutdown() {
  if (isShuttingDown) return;

  isShuttingDown = true;

  console.log("Stopping MarketPulse...");

  clearInterval(snapshotTimer);
  clearInterval(liveStatsTimer);
  clearInterval(writeFlushTimer);

  if (reconnectTimer) {
    clearTimeout(reconnectTimer);
  }

  if (binanceSocket) {
    try {
      binanceSocket.close();
    } catch (_) {}
  }

  // Drain queued batches before closing MongoDB.
  // The cap prevents shutdown from hanging forever
  // if Atlas is temporarily rejecting writes.

  for (let attempt = 0; attempt < 5; attempt++) {
    if (
      pendingCandles.size === 0 &&
      pendingAlerts.length === 0
    ) {
      break;
    }

    await Promise.all([
      flushCandleWrites(),
      flushAlertWrites()
    ]);
  }

  if (
    pendingCandles.size ||
    pendingAlerts.length
  ) {
    console.error(
      `Shutdown with ${pendingCandles.size} pending candles and ${pendingAlerts.length} pending alerts. ` +
      "Atlas may have rejected writes; check logs before restarting."
    );
  }

  io.close();

  await new Promise(resolve => server.close(resolve));

  await mongoose.disconnect();

  process.exit(0);
}

process.on("SIGINT", () => {
  void shutdown();
});

process.on("SIGTERM", () => {
  void shutdown();
});

void start();