require("dotenv").config();
const mongoose = require("mongoose");

async function check() {
  try {
    await mongoose.connect(process.env.MONGODB_URI);

    const db = mongoose.connection.db;

    console.log("\nDATABASE COLLECTIONS:");
    console.log(await db.listCollections().toArray());

    console.log("\nDATABASE STORAGE:");
    const stats = await db.stats(1024 * 1024);

    console.log({
      collections: stats.collections,
      objects: stats.objects,
      dataSizeMB: stats.dataSize,
      storageSizeMB: stats.storageSize,
      indexSizeMB: stats.indexSize
    });

  } catch (error) {
    console.error(error.message);
  } finally {
    await mongoose.disconnect();
  }
}

check();