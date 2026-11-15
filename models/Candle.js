const mongoose = require("mongoose");

const candleSchema = new mongoose.Schema({
  symbol: {
    type: String,
    required: true
  },

  interval: {
    type: String,
    required: true,
    default: "1m"
  },

  candleTime: {
    type: Date,
    required: true
  },

  open: {
    type: Number,
    required: true
  },

  high: {
    type: Number,
    required: true
  },

  low: {
    type: Number,
    required: true
  },

  close: {
    type: Number,
    required: true
  },

  volume: {
    type: Number,
    required: true
  },

  quoteVolume: {
    type: Number,
    default: 0
  },

  trades: {
    type: Number,
    default: 0
  },

  updatedAt: {
    type: Date,
    default: Date.now
  }

}, {
  versionKey: false
});


// One candle per symbol per minute.
candleSchema.index(
  {
    symbol: 1,
    interval: 1,
    candleTime: 1
  },
  {
    unique: true
  }
);


// Automatically expire candles after 3 hours.
candleSchema.index(
  {
    candleTime: 1
  },
  {
    expireAfterSeconds: 10800
  }
);

module.exports = mongoose.model(
  "Candle",
  candleSchema
);