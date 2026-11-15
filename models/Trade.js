const mongoose = require("mongoose");

const tradeSchema = new mongoose.Schema({
  source: {
    type: String,
    default: "BINANCE"
  },

  symbol: {
    type: String,
    required: true
  },

  tradeId: {
    type: Number,
    required: true
  },

  price: {
    type: Number,
    required: true
  },

  quantity: {
    type: Number,
    required: true
  },

  tradeTime: {
    type: Date,
    required: true
  },

  eventTime: Date,

  receivedAt: {
    type: Date,
    required: true
  },

  isBuyerMaker: Boolean

}, {
  versionKey: false
});

// Prevent duplicate trades for the same symbol.
tradeSchema.index(
  { symbol: 1, tradeId: 1 },
  { unique: true }
);

// Support historical queries by symbol and trade time.
tradeSchema.index(
  { symbol: 1, tradeTime: -1 }
);

// Automatically expire raw trades after 3 hours (10,800 seconds).
tradeSchema.index(
  { tradeTime: 1 },
  { expireAfterSeconds: 10800 }
);

module.exports = mongoose.model("Trade", tradeSchema);