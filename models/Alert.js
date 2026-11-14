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
    required: true,
    index: true
  }
}, {
  versionKey: false
});

// Optimizes the dashboard's newest-first alert history queries.
alertSchema.index({ timestamp: -1 });

// Supports common filtered history queries.
alertSchema.index({ symbol: 1, timestamp: -1 });
alertSchema.index({ severity: 1, timestamp: -1 });

module.exports = mongoose.model("Alert", alertSchema);
