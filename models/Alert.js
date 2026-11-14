
const mongoose = require("mongoose");

const alertSchema = new mongoose.Schema({
  alertId: {
    type: String,
    required: true,
    unique: true
  },

  type: {
    type: String,
    required: true,
    enum: ["PRICE_MOVEMENT", "VOLUME_SPIKE"],
    index: true
  },

  symbol: {
    type: String,
    required: true,
    index: true
  },

  message: {
    type: String,
    required: true
  },

  severity: {
    type: String,
    required: true,
    enum: ["LOW", "MEDIUM", "HIGH"],
    index: true
  },

  observedValue: {
    type: Number,
    required: true
  },

  baselineValue: {
    type: Number,
    required: true
  },

  changePct: {
    type: Number,
    default: null
  },

  timestamp: {
    type: Date,
    required: true
    // Do not add index: true here.
    // The TTL index below creates the timestamp_1 index with expiry options.
  }
}, {
  versionKey: false
});

// Optimizes newest-first alert history queries.
alertSchema.index({ timestamp: -1 });

// Supports common filtered history queries.
alertSchema.index({ symbol: 1, timestamp: -1 });
alertSchema.index({ severity: 1, timestamp: -1 });

// Automatically expire alerts after 24 hours.
// This is the only ascending { timestamp: 1 } index.
alertSchema.index(
  { timestamp: 1 },
  { expireAfterSeconds: 86400 }
);

module.exports = mongoose.model("Alert", alertSchema);