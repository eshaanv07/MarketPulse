
const connectDB = require("./db");

async function test() {
  await connectDB();
  process.exit(0);
}

test();
