
const { io } = require("socket.io-client");

const socket = io("http://localhost:5000");

socket.on("connect", () => {
  console.log("Connected to MarketPulse!");
  console.log("Client ID:", socket.id);
});

socket.on("market:snapshot", (snapshot) => {
  console.log("\nLIVE MARKET SNAPSHOT");

  Object.values(snapshot).forEach(asset => {
    console.log(
      asset.symbol,
      "Price:", asset.price,
      "Volume:", asset.volume10s
    );
  });
});

socket.on("market:trades", (trades) => {
  console.log(
    "\nReceived",
    trades.length,
    "live trade events"
  );

  const latest = trades[trades.length - 1];

  if (latest) {
    console.log(
      latest.symbol,
      "Price:", latest.price,
      "Quantity:", latest.quantity
    );
  }
});

socket.on("disconnect", (reason) => {
  console.log("Disconnected:", reason);
});
