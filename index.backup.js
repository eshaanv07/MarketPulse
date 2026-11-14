
require("dotenv").config();

const WebSocket = require("ws");
const mongoose = require("mongoose");
const { performance } = require("perf_hooks");

const connectDB = require("./db");
const Trade = require("./models/Trade");

// -------------------------------------
// CONFIGURATION
// -------------------------------------

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
const FLUSH_INTERVAL = 1000;
const MAX_BUFFER_SIZE = 10000;

// -------------------------------------
// MARKET STATE
// -------------------------------------

const marketState = {};

symbols.forEach(symbol => {
  marketState[symbol] = {
    totalEvents: 0,
    currentPrice: 0,
    recentTrades: [],
    volume10s: 0
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
let isFlushing = false;

let totalProcessingTime = 0;
let processingSamples = 0;
let maxProcessingTime = 0;

let socket;
let statsTimer;
let flushTimer;

// -------------------------------------
// NORMALIZE EVENTS
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
// UPDATE MARKET ANALYTICS
// -------------------------------------

function updateAnalytics(event) {
  const state = marketState[event.symbol];

  state.currentPrice = event.price;

  const cutoff = event.tradeTime.getTime() - 10000;

  const recent = state.recentTrades;

  recent.push({
    time: event.tradeTime.getTime(),
    price: event.price,
    quantity: event.quantity
  });

  state.volume10s += event.quantity;

  while (recent.length > 0 && recent[0].time < cutoff) {
    const expired = recent.shift();
    state.volume10s -= expired.quantity;
  }

  state.totalEvents++;
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

  const symbol = event.symbol;

  if (writeBuffer.length >= MAX_BUFFER_SIZE) {
    droppedEvents++;
    return;
  }

  updateAnalytics(event);

  writeBuffer.push(event);

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
    flushTrades();
  }
}

// -------------------------------------
// BATCHED MONGODB WRITES
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

    console.error(
      "MongoDB batch write failed:",
      error.message
    );

    // Retry failed batch later, without exceeding buffer capacity.
    const available =
      MAX_BUFFER_SIZE - writeBuffer.length;

    const retryBatch = batch.slice(0, available);

    writeBuffer = retryBatch.concat(writeBuffer);

    droppedEvents += batch.length - retryBatch.length;

  } finally {
    isFlushing = false;
  }
}

// -------------------------------------
// START BINANCE CONNECTION
// -------------------------------------

function startStream() {
  socket = new WebSocket(BINANCE_URL);

  socket.on("open", () => {
    console.log("\nBinance connected successfully!");
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
    console.error("Binance WebSocket error:", error.message);
  });

  socket.on("close", code => {
    console.log("Binance connection closed:", code);
  });
}

// -------------------------------------
// PERFORMANCE DASHBOARD
// -------------------------------------

function startMonitoring() {
  statsTimer = setInterval(() => {
    console.clear();

    console.log("========== MARKETPULSE ENGINE ==========\n");

    console.log("Received:", totalReceived);
    console.log("Processed:", totalProcessed);
    console.log("Persisted:", totalPersisted);
    console.log("Invalid:", invalidEvents);
    console.log("Duplicate:", duplicateEvents);
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
      "Max processing time:",
      maxProcessingTime.toFixed(4),
      "ms"
    );

    console.log(
      "Pending DB buffer:",
      writeBuffer.length
    );

    console.log("\n----------- MARKET ANALYTICS -----------");

    symbols.forEach(symbol => {
      const state = marketState[symbol];

      console.log("\n" + symbol);
      console.log("Current price:", state.currentPrice);
      console.log("Total trades:", state.totalEvents);
      console.log(
        "10-sec volume:",
        state.volume10s.toFixed(6)
      );
    });

    console.log("\n========================================");

    intervalEvents = 0;

  }, 5000);

  flushTimer = setInterval(() => {
    flushTrades();
  }, FLUSH_INTERVAL);
}

// -------------------------------------
// GRACEFUL SHUTDOWN
// -------------------------------------

async function shutdown() {
  console.log("\nShutting down MarketPulse...");

  clearInterval(statsTimer);
  clearInterval(flushTimer);

  if (socket) {
    socket.close();
  }

  // Allow an active database operation to finish.
  while (isFlushing) {
    await new Promise(resolve => setTimeout(resolve, 100));
  }

  // Persist remaining buffered trades.
  while (writeBuffer.length > 0) {
    await flushTrades();

    if (writeBuffer.length > 0) {
      await new Promise(resolve => setTimeout(resolve, 1000));
    }
  }

  await mongoose.disconnect();

  console.log("Database disconnected.");
  console.log("MarketPulse stopped.");

  process.exit(0);
}

// -------------------------------------
// APPLICATION ENTRY POINT
// -------------------------------------

async function startApplication() {
  try {
    console.log("Connecting to MongoDB...");

    await connectDB();

    // Ensure database indexes are ready.
    await Trade.init();

    console.log("Trade collection ready.");

    startStream();
    startMonitoring();

  } catch (error) {
    console.error("Application startup failed:", error.message);
    process.exit(1);
  }
}

process.on("SIGINT", shutdown);

startApplication();
