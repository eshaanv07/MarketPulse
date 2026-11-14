const cors = require("cors");
require("dotenv").config();

const WebSocket = require("ws");
const mongoose = require("mongoose");
const express = require("express");
const http = require("http");
const { Server } = require("socket.io");
const { performance } = require("perf_hooks");

const connectDB = require("./db");
const Trade = require("./models/Trade");
const Alert = require("./models/Alert");
const createMarketAnalytics = require("./analytics");

// -------------------------------------
// CONFIGURATION
// -------------------------------------

const PORT = 5000;

// -------------------------------------
// MARKET INTELLIGENCE ENGINE
// -------------------------------------

const marketAnalytics = createMarketAnalytics({
  priceWindowMs: 60000,
  priceThresholdPct: 0.5,

  volumeBucketMs: 10000,
  volumeMultiplier: 2.5,
  volumeStdDevMultiplier: 3,

  minVolumeBaselineBuckets: 12,
  cooldownMs: 60000
});

console.log("Market Intelligence Engine initialized.");

const symbols = [
  "BTCUSDT",
  "ETHUSDT",
  "SOLUSDT",
  "XRPUSDT"
];

const streams = symbols.map(
  symbol => `${symbol.toLowerCase()}@trade`
);

const BINANCE_URL =
  `wss://stream.binance.com:9443/stream?streams=${streams.join("/")}`;

const BATCH_SIZE = 500;
const MAX_BUFFER_SIZE = 10000;
const FLUSH_INTERVAL = 1000;
const BROADCAST_INTERVAL = 250;
const ALERT_BATCH_SIZE = 100;
const MAX_ALERT_BUFFER_SIZE = 5000;

// Storage-pressure thresholds (MongoDB db.stats estimate, not an exact Atlas quota meter).
const STORAGE_THRESHOLDS_MB = { monitor: 400, cleanup: 440, aggressive: 460, critical: 480 };
const STORAGE_CHECK_INTERVAL_MS = 60 * 1000;
const STORAGE_CLEANUP_BATCH_PER_SYMBOL = 250;
const STORAGE_MAX_ROUNDS_PER_CHECK = 3;
const STORAGE_MIN_PROGRESS_MB = 1;
const STORAGE_RETRY_COOLDOWN_MS = 5 * 60 * 1000;
const STORAGE_CLEANER_ENABLED = true;


// -------------------------------------
// EXPRESS + HTTP + SOCKET.IO
// -------------------------------------

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


// -------------------------------------
// HISTORICAL MARKET DATA API
// -------------------------------------
// Example: /api/market/history?symbol=BTCUSDT&range=24h&interval=5m
app.get("/api/market/history", async (req, res) => {
  try {
    const symbol = String(req.query.symbol || "BTCUSDT").toUpperCase();
    const ranges = {
      "1h": 60 * 60 * 1000,
      "24h": 24 * 60 * 60 * 1000
    };
    const intervals = {
      "1m": { unit: "minute", binSize: 1 },
      "5m": { unit: "minute", binSize: 5 },
      "15m": { unit: "minute", binSize: 15 },
      "1h": { unit: "hour", binSize: 1 }
    };

    const range = String(req.query.range || "1h");
    const interval = String(req.query.interval || "1m");

    if (!symbols.includes(symbol)) {
      return res.status(400).json({ error: "Unsupported market symbol." });
    }
    if (!ranges[range]) {
      return res.status(400).json({ error: "Unsupported time range. Use 1h or 24h." });
    }
    if (!intervals[interval]) {
      return res.status(400).json({ error: "Unsupported candle interval. Use 1m, 5m, 15m, or 1h." });
    }

    const to = new Date();
    const from = new Date(to.getTime() - ranges[range]);
    const bucket = intervals[interval];

    const candles = await Trade.aggregate([
      {
        $match: {
          symbol,
          tradeTime: { $gte: from, $lte: to }
        }
      },
      // Use the existing { symbol: 1, tradeTime: -1 } index.
      // Sorting ascending by both tradeTime and tradeId required a large
      // in-memory sort for BTC and exceeded MongoDB's 32 MB limit.
      { $sort: { tradeTime: -1 } },
      {
        $group: {
          _id: {
            $dateTrunc: {
              date: "$tradeTime",
              unit: bucket.unit,
              binSize: bucket.binSize,
              timezone: "UTC"
            }
          },
          // Input is newest-first, so first=close and last=open.
          open: { $last: "$price" },
          high: { $max: "$price" },
          low: { $min: "$price" },
          close: { $first: "$price" },
          volume: { $sum: "$quantity" },
          quoteVolume: { $sum: { $multiply: ["$price", "$quantity"] } },
          trades: { $sum: 1 }
        }
      },
      { $sort: { _id: 1 } },
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
    ]).allowDiskUse(true);

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
    console.error("Historical market data API error:", error.message);
    res.status(500).json({ error: "Unable to retrieve historical market data." });
  }
});

// -------------------------------------
// ALERT HISTORY API
// -------------------------------------

// Supports: /api/alerts?symbol=BTCUSDT&type=VOLUME_SPIKE&severity=HIGH&limit=50&page=1
app.get("/api/alerts", async (req, res) => {
  try {
    const filter = {};

    if (req.query.symbol) {
      filter.symbol = String(req.query.symbol).toUpperCase();
    }

    if (req.query.type) {
      filter.type = String(req.query.type).toUpperCase();
    }

    if (req.query.severity) {
      filter.severity = String(req.query.severity).toUpperCase();
    }

    if (req.query.from || req.query.to) {
      filter.timestamp = {};
      if (req.query.from) {
        const from = new Date(req.query.from);
        if (Number.isNaN(from.getTime())) {
          return res.status(400).json({ error: "Invalid 'from' date." });
        }
        filter.timestamp.$gte = from;
      }
      if (req.query.to) {
        const to = new Date(req.query.to);
        if (Number.isNaN(to.getTime())) {
          return res.status(400).json({ error: "Invalid 'to' date." });
        }
        filter.timestamp.$lte = to;
      }
    }

    const requestedLimit = Number.parseInt(req.query.limit, 10);
    const requestedPage = Number.parseInt(req.query.page, 10);
    const limit = Number.isFinite(requestedLimit)
      ? Math.min(Math.max(requestedLimit, 1), 200)
      : 50;
    const page = Number.isFinite(requestedPage)
      ? Math.max(requestedPage, 1)
      : 1;

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
    console.error("Alert history API error:", error.message);
    res.status(500).json({ error: "Unable to retrieve alert history." });
  }
});

// Health check API
app.get("/health", (req, res) => {
  res.json({
    status: "running",
    database: mongoose.connection.readyState === 1 ? "connected" : "disconnected",
    connectedClients: io.engine.clientsCount,
    pendingAlerts: alertBuffer.length,
    timestamp: new Date()
  });
});

// Detailed system status, including estimated MongoDB storage usage.
app.get("/api/system/status", async (req, res) => {
  try {
    const storage = await getStorageStats();
    res.json({
      status: "running",
      database: mongoose.connection.readyState === 1 ? "connected" : "disconnected",
      symbols,
      storage,
      cleaner: {
        enabled: STORAGE_CLEANER_ENABLED,
        running: storageCleanupRunning,
        modelsReady,
        lastCleanupAt: lastStorageCleanupAt,
        lastCleanupDeleted: lastStorageCleanupDeleted,
        lastCleanupReason: lastStorageCleanupReason,
        thresholdsMB: STORAGE_THRESHOLDS_MB
      },
      buffers: {
        trades: writeBuffer.length,
        alerts: alertBuffer.length,
        broadcasts: outboundBatch.length
      },
      connectedClients: io.engine.clientsCount,
      timestamp: new Date()
    });
  } catch (error) {
    res.status(500).json({ error: "Unable to retrieve system status." });
  }
});

// MARKET STATE
// -------------------------------------

const marketState = {};

symbols.forEach(symbol => {
  marketState[symbol] = {
    totalEvents: 0,
    currentPrice: 0,
    recentTrades: [],
    volume10s: 0,
    trades10s: 0
  };
});

let totalReceived = 0;
let totalProcessed = 0;
let totalPersisted = 0;
let invalidEvents = 0;
let duplicateEvents = 0;
let dbErrors = 0;
let droppedEvents = 0;

let intervalEvents = 0;
let writeBuffer = [];
let alertBuffer = [];
let outboundBatch = [];

let isFlushing = false;
let isFlushingAlerts = false;
let socket;

let totalProcessingTime = 0;
let processingSamples = 0;
let maxProcessingTime = 0;

let statsTimer;
let flushTimer;
let broadcastTimer;
let storageTimer;
let lastStorageCleanupAt = null;
let lastStorageCleanupDeleted = 0;
let lastStorageCleanupReason = "Not run yet";
let lastStorageStats = null;
let storageCleanupRunning = false;
let modelsReady = false;
let snapshotTimer;

// -------------------------------------
// STORAGE MONITOR + CONTROLLED CLEANER
// -------------------------------------

function mb(bytes) {
  return Number((bytes / (1024 * 1024)).toFixed(2));
}

function getStorageLevel(usedMB) {
  if (usedMB >= STORAGE_THRESHOLDS_MB.critical) return "CRITICAL";
  if (usedMB >= STORAGE_THRESHOLDS_MB.aggressive) return "AGGRESSIVE";
  if (usedMB >= STORAGE_THRESHOLDS_MB.cleanup) return "CLEANUP";
  if (usedMB >= STORAGE_THRESHOLDS_MB.monitor) return "MONITOR";
  return "NORMAL";
}

async function getStorageStats() {
  const db = mongoose.connection.db;
  if (!db) throw new Error("MongoDB database handle is not ready.");

  // This is the MarketPulseDB logical footprint, not the Atlas cluster-wide
  // quota meter. Atlas may include other databases and replica-set overhead.
  // db.stats(1) returns byte counts. Convert every byte metric to MB
  // before comparing it with the 400/440/460/480 MB thresholds.
  const stats = await db.stats(1);
  const dataBytes = Number(stats.dataSize || 0);
  const indexBytes = Number(stats.indexSize || 0);
  const allocatedBytes = Number(stats.storageSize || 0);
  const logicalBytes = dataBytes + indexBytes;

  const result = {
    database: db.databaseName,
    usedMB: mb(logicalBytes),
    dataMB: mb(dataBytes),
    indexMB: mb(indexBytes),
    storageAllocatedMB: mb(allocatedBytes),
    collections: Number(stats.collections || 0),
    objects: Number(stats.objects || 0),
    level: getStorageLevel(mb(logicalBytes)),
    thresholdsMB: STORAGE_THRESHOLDS_MB,
    source: "MongoDB db.stats(1) byte values converted to MB: (dataSize + indexSize) / 1048576 for MarketPulseDB",
    limitation: "This is a database-level estimate, not Atlas cluster-wide Data Size. Compare it with Atlas Metrics."
  };

  lastStorageStats = result;
  return result;
}

function cleanupRoundsForLevel(level) {
  if (level === "CRITICAL") return STORAGE_MAX_ROUNDS_PER_CHECK;
  if (level === "AGGRESSIVE") return 2;
  if (level === "CLEANUP") return 1;
  return 0;
}

async function deleteOldestTradesForSymbol(symbol, limit) {
  const oldest = await Trade.find({ symbol })
    .sort({ tradeTime: 1, tradeId: 1 })
    .limit(limit)
    .select({ _id: 1 })
    .lean();

  if (oldest.length === 0) return 0;

  const result = await Trade.deleteMany({
    _id: { $in: oldest.map(item => item._id) }
  });

  return Number(result.deletedCount || 0);
}

async function runStorageCheck() {
  if (!STORAGE_CLEANER_ENABLED || storageCleanupRunning ||
      mongoose.connection.readyState !== 1) return;

  storageCleanupRunning = true;

  try {
    let storage = await getStorageStats();
    console.log(
      `[Storage] MarketPulseDB logical footprint: ${storage.usedMB} MB ` +
      `(${storage.level}); data=${storage.dataMB} MB, indexes=${storage.indexMB} MB.`
    );

    const rounds = cleanupRoundsForLevel(storage.level);
    if (rounds === 0) {
      lastStorageCleanupReason = `No cleanup required (${storage.level}).`;
      return;
    }

    if (lastStorageCleanupAt &&
        Date.now() - lastStorageCleanupAt.getTime() < STORAGE_RETRY_COOLDOWN_MS) {
      lastStorageCleanupReason = "Cleanup cooldown active; monitoring continues.";
      return;
    }

    let totalDeleted = 0;
    for (let round = 1; round <= rounds; round++) {
      let roundDeleted = 0;

      // Identical per-symbol allowance: no coin receives preferential treatment.
      for (const symbol of symbols) {
        roundDeleted += await deleteOldestTradesForSymbol(
          symbol,
          STORAGE_CLEANUP_BATCH_PER_SYMBOL
        );
      }

      totalDeleted += roundDeleted;
      if (roundDeleted === 0) break;

      await new Promise(resolve => setTimeout(resolve, 1000));
      const after = await getStorageStats();

      console.log(
        `[Storage] Round ${round}/${rounds}: deleted ${roundDeleted} trades; ` +
        `database logical footprint now ${after.usedMB} MB.`
      );

      // Compare the database logical footprint, not allocated WiredTiger bytes.
      // If it does not improve, stop rather than deleting repeatedly without evidence.
      if (after.usedMB >= storage.usedMB - STORAGE_MIN_PROGRESS_MB) {
        storage = after;
        console.warn(
          "[Storage] Logical database size did not fall by at least " +
          `${STORAGE_MIN_PROGRESS_MB} MB. Stopping cleanup; check Atlas Metrics separately.`
        );
        break;
      }

      storage = after;
      if (storage.usedMB < STORAGE_THRESHOLDS_MB.cleanup) break;
    }

    lastStorageCleanupAt = new Date();
    lastStorageCleanupDeleted = totalDeleted;
    lastStorageCleanupReason =
      `Pressure cleanup completed; deleted ${totalDeleted} oldest trades ` +
      "with equal per-symbol batch limits.";

    if (totalDeleted > 0) {
      console.warn(
        `[Storage] Deleted ${totalDeleted} trades. Under pressure, retained raw history ` +
        "may be shorter than 24 hours."
      );
    }
  } catch (error) {
    lastStorageCleanupReason = `Storage check failed: ${error.message}`;
    // Avoid hammering Atlas when quota restrictions or permissions block cleanup.
    lastStorageCleanupAt = new Date();
    console.error("[Storage] Monitor/cleaner error:", error.message);
  } finally {
    storageCleanupRunning = false;
  }
}

// -------------------------------------
// SOCKET.IO CONNECTIONS
// -------------------------------------

function getMarketSnapshot() {
  const snapshot = {};

  symbols.forEach(symbol => {
    const state = marketState[symbol];

    snapshot[symbol] = {
      symbol,
      price: state.currentPrice,
      volume10s: state.volume10s,
      trades10s: state.trades10s,
      totalEvents: state.totalEvents
    };
  });

  return snapshot;
}

io.on("connection", client => {
  console.log("Frontend client connected:", client.id);

  // Send current market state to newly connected clients.
  client.emit("market:snapshot", getMarketSnapshot());

  // Give newly connected clients a recent persisted alert history.
  Alert.find()
    .sort({ timestamp: -1 })
    .limit(20)
    .lean()
    .then(alerts => client.emit("market:alerts:history", alerts))
    .catch(error => {
      console.error("Unable to load alert history for client:", error.message);
    });

  client.on("disconnect", reason => {
    console.log("Frontend client disconnected:", reason);
  });
});

// -------------------------------------
// EVENT NORMALIZATION
// -------------------------------------

function normalizeEvent(trade) {
  const price = Number(trade.p);
  const quantity = Number(trade.q);

  if (
    !symbols.includes(trade.s) ||
    !Number.isSafeInteger(trade.t) ||
    !Number.isFinite(price) ||
    !Number.isFinite(quantity) ||
    price <= 0 ||
    quantity <= 0 ||
    !Number.isFinite(trade.T)
  ) {
    return null;
  }

  return {
    source: "BINANCE",
    symbol: trade.s,
    tradeId: trade.t,
    price,
    quantity,
    tradeTime: new Date(trade.T),
    eventTime: Number.isFinite(trade.E)
      ? new Date(trade.E)
      : undefined,
    receivedAt: new Date(),
    isBuyerMaker: trade.m
  };
}

// -------------------------------------
// ROLLING MARKET ANALYTICS
// -------------------------------------

function updateAnalytics(event) {
  const state = marketState[event.symbol];

  state.currentPrice = event.price;
  state.totalEvents++;

  state.recentTrades.push({
    time: event.tradeTime.getTime(),
    price: event.price,
    quantity: event.quantity
  });

  state.volume10s += event.quantity;

  const cutoff = Date.now() - 10000;

  while (
    state.recentTrades.length > 0 &&
    state.recentTrades[0].time < cutoff
  ) {
    const expired = state.recentTrades.shift();
    state.volume10s -= expired.quantity;
  }

  state.trades10s = state.recentTrades.length;
}

// -------------------------------------
// PROCESS EVENTS
// -------------------------------------

function processEvent(rawTrade) {
  const start = performance.now();

  totalReceived++;

  const event = normalizeEvent(rawTrade);

  if (!event) {
    invalidEvents++;
    return;
  }

  if (writeBuffer.length >= MAX_BUFFER_SIZE) {
    droppedEvents++;
    return;
  }

// -------------------------------------
// MARKET INTELLIGENCE ANALYSIS
// -------------------------------------

    const alerts = marketAnalytics.processTrade(event);

// Broadcast detected anomalies.

    if (alerts.length > 0) {

        alerts.forEach(alert => {
            if (alertBuffer.length < MAX_ALERT_BUFFER_SIZE) {
              alertBuffer.push(alert);
            } else {
              console.error("Alert persistence buffer is full; alert was not queued:", alert.id);
            }

            console.log("\n========== MARKET ALERT ==========");

            console.log("Type:", alert.type);
            console.log("Asset:", alert.symbol);
            console.log("Severity:", alert.severity);
            console.log("Message:", alert.message);

            console.log("==================================\n");

        });

        io.emit("market:alerts", alerts);

    }

// -------------------------------------
// EXISTING DATA PIPELINE
// -------------------------------------

    updateAnalytics(event);

    writeBuffer.push(event);
    outboundBatch.push(event);

    totalProcessed++;
    intervalEvents++;

  const elapsed = performance.now() - start;

  totalProcessingTime += elapsed;
  processingSamples++;

  maxProcessingTime = Math.max(
    maxProcessingTime,
    elapsed
  );

  if (writeBuffer.length >= BATCH_SIZE) {
    void flushTrades();
  }
}

// -------------------------------------
// MONGODB BATCH WRITER
// -------------------------------------

async function flushTrades() {
  if (isFlushing || writeBuffer.length === 0) {
    return;
  }

  isFlushing = true;

  const batch = writeBuffer.splice(0, BATCH_SIZE);

  try {
    const operations = batch.map(event => ({
      updateOne: {
        filter: {
          symbol: event.symbol,
          tradeId: event.tradeId
        },
        update: {
          $setOnInsert: event
        },
        upsert: true
      }
    }));

    const result = await Trade.bulkWrite(
      operations,
      { ordered: false }
    );

    totalPersisted += result.upsertedCount;
    duplicateEvents += result.matchedCount;

  } catch (error) {
    dbErrors++;

    console.error("MongoDB write error:", error.message);

    const available = MAX_BUFFER_SIZE - writeBuffer.length;
    const retryBatch = batch.slice(0, available);

    writeBuffer = retryBatch.concat(writeBuffer);

    droppedEvents += batch.length - retryBatch.length;

  } finally {
    isFlushing = false;
  }
}

// -------------------------------------
// MONGODB ALERT WRITER
// -------------------------------------

async function flushAlerts() {
  if (isFlushingAlerts || alertBuffer.length === 0) {
    return;
  }

  isFlushingAlerts = true;
  const batch = alertBuffer.splice(0, ALERT_BATCH_SIZE);

  try {
    const operations = batch.map(alert => ({
      updateOne: {
        filter: { alertId: alert.id },
        update: {
          $setOnInsert: {
            alertId: alert.id,
            type: alert.type,
            symbol: alert.symbol,
            message: alert.message,
            severity: alert.severity,
            observedValue: alert.observedValue,
            baselineValue: alert.baselineValue,
            changePct: alert.changePct,
            timestamp: alert.timestamp
          }
        },
        upsert: true
      }
    }));

    await Alert.bulkWrite(operations, { ordered: false });
  } catch (error) {
    console.error("MongoDB alert write error:", error.message);
    const available = MAX_ALERT_BUFFER_SIZE - alertBuffer.length;
    const retryBatch = batch.slice(0, available);
    alertBuffer = retryBatch.concat(alertBuffer);

    if (retryBatch.length < batch.length) {
      console.error("Some alerts could not be re-queued because the alert buffer is full.");
    }
  } finally {
    isFlushingAlerts = false;
  }
}

// -------------------------------------
// SOCKET.IO MARKET BROADCASTING
// -------------------------------------

function broadcastMarketUpdates() {
  if (outboundBatch.length === 0) {
    return;
  }

  // Send a batch of processed trades to connected clients.
  const batch = outboundBatch.splice(0);

  io.emit("market:trades", batch);
}

function broadcastMarketSnapshot() {
  io.emit("market:snapshot", getMarketSnapshot());
}

// -------------------------------------
// BINANCE CONNECTION
// -------------------------------------

function startBinanceStream() {
  socket = new WebSocket(BINANCE_URL);

  socket.on("open", () => {
    console.log("Binance WebSocket connected.");
    console.log("Monitoring:", symbols.join(", "));
  });

  socket.on("message", rawData => {
    try {
      const message = JSON.parse(rawData.toString());

      if (message.data) {
        processEvent(message.data);
      }

    } catch (error) {
      console.error("Event parsing error:", error.message);
    }
  });

  socket.on("error", error => {
    console.error("Binance connection error:", error.message);
  });

  socket.on("close", code => {
    console.log("Binance connection closed:", code);
  });
}

// -------------------------------------
// PERFORMANCE MONITORING
// -------------------------------------

function startMonitoring() {
  statsTimer = setInterval(() => {
    console.clear();

    console.log("========== MARKETPULSE SERVER ==========\n");

    console.log("Received:", totalReceived);
    console.log("Processed:", totalProcessed);
    console.log("Persisted:", totalPersisted);
    console.log("Invalid:", invalidEvents);
    console.log("Duplicates:", duplicateEvents);
    console.log("Database errors:", dbErrors);
    console.log("Dropped:", droppedEvents);

    console.log(
      "Throughput:",
      (intervalEvents / 5).toFixed(2),
      "events/sec"
    );

    console.log(
      "Average processing time:",
      processingSamples
        ? (totalProcessingTime / processingSamples).toFixed(4)
        : 0,
      "ms"
    );

    console.log(
      "Maximum processing time:",
      maxProcessingTime.toFixed(4),
      "ms"
    );

    console.log("Pending DB buffer:", writeBuffer.length);
    console.log("Pending broadcast buffer:", outboundBatch.length);

    console.log("Connected clients:", io.engine.clientsCount);

    console.log("\n----------- MARKET STATE -----------");

    symbols.forEach(symbol => {
      const state = marketState[symbol];

      console.log("\n" + symbol);
      console.log("Price:", state.currentPrice);
      console.log("10-sec volume:", state.volume10s.toFixed(6));
      console.log("10-sec trades:", state.trades10s);
    });

    console.log("\n======================================");

    intervalEvents = 0;

  }, 5000);

  flushTimer = setInterval(() => {
    void flushTrades();
    void flushAlerts();
  }, FLUSH_INTERVAL);

  broadcastTimer = setInterval(() => {
    broadcastMarketUpdates();
  }, BROADCAST_INTERVAL);

  // Periodically refresh market snapshots for clients.
  snapshotTimer = setInterval(broadcastMarketSnapshot, 2000);

  // Storage monitoring is independent of the market-data write pipeline.
  void runStorageCheck();
  storageTimer = setInterval(() => {
    void runStorageCheck();
  }, STORAGE_CHECK_INTERVAL_MS);
}

// -------------------------------------
// SHUTDOWN
// -------------------------------------

let shuttingDown = false;

async function shutdown() {
  if (shuttingDown) return;

  shuttingDown = true;

  console.log("\nStopping MarketPulse...");

  clearInterval(statsTimer);
  clearInterval(flushTimer);
  clearInterval(broadcastTimer);
  clearInterval(storageTimer);
  clearInterval(snapshotTimer);

  if (socket) {
    socket.close();
  }

  broadcastMarketUpdates();

  // Wait for an active database write.
  while (isFlushing || isFlushingAlerts) {
    await new Promise(resolve => setTimeout(resolve, 100));
  }

  // Flush remaining records.
  let attempts = 0;

  while (writeBuffer.length > 0 && attempts < 3) {
    await flushTrades();

    if (writeBuffer.length > 0) {
      attempts++;
      await new Promise(resolve => setTimeout(resolve, 1000));
    }
  }

  if (writeBuffer.length > 0) {
    console.error("Warning: some database records remain unpersisted.");
  }

  let alertAttempts = 0;
  while (alertBuffer.length > 0 && alertAttempts < 3) {
    await flushAlerts();
    if (alertBuffer.length > 0) {
      alertAttempts++;
      await new Promise(resolve => setTimeout(resolve, 1000));
    }
  }

  if (alertBuffer.length > 0) {
    console.error("Warning: some alerts remain unpersisted.");
  }

  io.close();

  await mongoose.disconnect();

  console.log("MongoDB disconnected.");
  console.log("MarketPulse stopped.");

  process.exit(0);
}

process.on("SIGINT", shutdown);

// -------------------------------------
// APPLICATION STARTUP
// -------------------------------------

async function startApplication() {
  console.log("Connecting to MongoDB...");

  try {
    await connectDB();
    console.log("MongoDB connected. Starting HTTP service and storage monitor.");

    // Start health/status endpoints even if model index initialization fails.
    server.listen(PORT, async () => {
      console.log(`MarketPulse server running on port ${PORT}`);
      console.log(`Health check: http://localhost:${PORT}/health`);
      console.log(`System status: http://localhost:${PORT}/api/system/status`);

      // Start the monitor before attempting model initialization.
      void getStorageStats()
        .then(stats => console.log(`[Storage] Initial database footprint: ${stats.usedMB} MB.`))
        .catch(error => console.error("[Storage] Initial check failed:", error.message));

      // Try a controlled cleanup before index initialization. This can help
      // when Atlas quota pressure is preventing index creation. If collections
      // are absent or deletes are blocked, the attempt logs the failure safely.
      void runStorageCheck();

      try {
        await Trade.init();
        await Alert.init();
        modelsReady = true;
        console.log("MongoDB models and indexes ready.");

        // Run a pressure check immediately after models become usable.
        void runStorageCheck();

        startBinanceStream();
        startMonitoring();
      } catch (error) {
        modelsReady = false;
        console.error("Model initialization failed; HTTP status service remains available:", error.message);
        console.error("Binance collection has NOT been started. Resolve the model/index issue, then restart.");
        // Keep the server alive so health and storage status can still be inspected.
        storageTimer = setInterval(() => {
          void runStorageCheck();
        }, STORAGE_CHECK_INTERVAL_MS);
      }
    });
  } catch (error) {
    console.error("MongoDB connection failed. HTTP service cannot start:", error.message);
    process.exitCode = 1;
  }
}
startApplication();