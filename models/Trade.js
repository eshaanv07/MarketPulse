
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

// Make historical queries faster.
tradeSchema.index(
  { symbol: 1, tradeTime: -1 }
);

module.exports = mongoose.model("Trade", tradeSchema);
