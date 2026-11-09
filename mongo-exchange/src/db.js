// Single shared MongoClient. Always connect through mongos, never to a shard directly.
import { MongoClient } from 'mongodb';

export const MONGO_URI = process.env.MONGO_URI ?? 'mongodb://localhost:27017';
export const DB_NAME = process.env.DB_NAME ?? 'exchange';

export const client = new MongoClient(MONGO_URI, {
  // Transactions need majority write concern to be durable across shard failover.
  writeConcern: { w: 'majority' },
  readPreference: 'primary',
  maxPoolSize: 50,
});

let db;
export async function connect() {
  if (!db) {
    await client.connect();
    db = client.db(DB_NAME);
  }
  return db;
}

export const col = (name) => db.collection(name);

export const SYMBOLS = ['ASHOKLEY', 'BHEL', 'INFY', 'ITC', 'MRF', 'NTPC', 'RELIANCE', 'TCS', 'WIPRO', 'ZOMATO'];
export const QUOTE = 'INR'; // every symbol trades against INR; prices are stored in PAISE (integers)

export async function close() {
  await client.close();
  db = undefined;
}
