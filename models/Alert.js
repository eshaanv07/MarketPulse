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
  }

}, {
  versionKey: false
});

alertSchema.index({
  timestamp: -1
});

alertSchema.index({
  symbol: 1,
  timestamp: -1
});

alertSchema.index({
  severity: 1,
  timestamp: -1
});


// Automatically expire alerts after 3 hours.
alertSchema.index(
  {
    timestamp: 1
  },
  {
    expireAfterSeconds: 10800
  }
);

module.exports = mongoose.model(
  "Alert",
  alertSchema
);