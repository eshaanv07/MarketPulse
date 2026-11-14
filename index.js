
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
  "BNBUSDT",
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

// -------------------------------------
// EXPRESS + HTTP + SOCKET.IO
// -------------------------------------

const app = express();

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
    database: mongoose.connection.readyState === 1
      ? "connected"
      : "disconnected",
    connectedClients: io.engine.clientsCount,
    pendingAlerts: alertBuffer.length,
    timestamp: new Date()
  });
});

// -------------------------------------
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
  setInterval(broadcastMarketSnapshot, 2000);
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
  try {
    console.log("Connecting to MongoDB...");

    await connectDB();
    await Trade.init();
    await Alert.init();

    console.log("MongoDB ready.");

    server.listen(PORT, () => {
      console.log(`MarketPulse server running on port ${PORT}`);
      console.log(`Health check: http://localhost:${PORT}/health`);

      startBinanceStream();
      startMonitoring();
    });

  } catch (error) {
    console.error("Startup failed:", error.message);
    process.exit(1);
  }
}

startApplication();
