// Phase 1: schema, validators, indexes, sharding, zones, seed data.
// Idempotent — safe to run again.   `npm run setup`
import { connect, close, client, DB_NAME, SYMBOLS, QUOTE } from './db.js';
import { MinKey as MinKeyCls, MaxKey as MaxKeyCls } from 'mongodb';
const MinKey = new MinKeyCls(), MaxKey = new MaxKeyCls();

const admin = () => client.db('admin');
const ns = (c) => `${DB_NAME}.${c}`;

async function ignore(codeNames, fn) {
  try { return await fn(); }
  catch (e) { if (!codeNames.includes(e.codeName)) throw e; return null; }
}

// ───────────────────────────── 1. collections + validators ─────────────────────────────
// Integers only (paise / units). 'number' accepts int32|int64|double; multipleOf:1 forbids fractions.
const INT0 = { bsonType: 'number', multipleOf: 1, minimum: 0 };
const INT1 = { bsonType: 'number', multipleOf: 1, minimum: 1 };
const money = { bsonType: 'object', required: ['available', 'locked'],
  properties: { available: INT0, locked: INT0 } };

const SCHEMAS = {
  users: {
    bsonType: 'object', required: ['name', 'balances'],
    properties: {
      name: { bsonType: 'string' },
      // balances.INR / balances.<SYMBOL> → { available, locked } all INTEGER (paise / units)
      balances: { bsonType: 'object', additionalProperties: money },
    },
  },
  orders: {
    bsonType: 'object',
    required: ['symbol', 'userId', 'side', 'type', 'qty', 'remaining', 'status', 'createdAt'],
    properties: {
      symbol:    { enum: SYMBOLS },
      userId:    { bsonType: 'objectId' },
      side:      { enum: ['BUY', 'SELL'] },
      type:      { enum: ['LIMIT', 'MARKET'] },
      price:     { bsonType: ['number', 'null'], multipleOf: 1, minimum: 1 },   // paise; null for MARKET
      qty:       INT1,
      remaining: INT0,
      status:    { enum: ['OPEN', 'FILLED', 'CANCELLED'] },
      createdAt: { bsonType: 'date' },
    },
  },
  trades: {
    bsonType: 'object',
    required: ['symbol', 'price', 'qty', 'buyOrderId', 'sellOrderId', 'buyerId', 'sellerId', 'takerSide', 'executedAt'],
    properties: {
      symbol: { enum: SYMBOLS }, price: INT1, qty: INT1,
      takerSide: { enum: ['BUY', 'SELL'] }, executedAt: { bsonType: 'date' },
    },
  },
};

async function createCollections(db) {
  const existing = new Set((await db.listCollections({}, { nameOnly: true }).toArray()).map(c => c.name));
  for (const [name, schema] of Object.entries(SCHEMAS)) {
    if (existing.has(name)) await db.command({ collMod: name, validator: { $jsonSchema: schema }, validationLevel: 'strict' });
    else await db.createCollection(name, { validator: { $jsonSchema: schema } });
  }
  if (!existing.has('symbols')) await db.createCollection('symbols');
  if (!existing.has('candles_1m')) await db.createCollection('candles_1m');
  if (!existing.has('_meta')) await db.createCollection('_meta');          // change-stream resume tokens

  // TIME-SERIES collection: one bucket per symbol, columnar storage, automatic bucketing by time.
  if (!existing.has('ticks')) {
    await db.createCollection('ticks', {
      timeseries: { timeField: 'ts', metaField: 'meta', granularity: 'seconds' },
      expireAfterSeconds: 60 * 60 * 24 * 7,      // TTL: raw ticks live 7 days; candles_1m keeps history
    });
  }
  console.log('✔ collections + validators');
}

// ───────────────────────────── 2. indexes ─────────────────────────────
async function createIndexes(db) {
  // Order-book matching reads: {symbol, side, status:OPEN} sorted by price-time priority.
  // Partial indexes keep only live orders → the index stays tiny no matter how many filled orders exist.
  await db.collection('orders').createIndexes([
    { key: { symbol: 1, side: 1, price: 1, createdAt: 1 }, name: 'book_asks',
      partialFilterExpression: { status: 'OPEN', side: 'SELL' } },
    { key: { symbol: 1, side: 1, price: -1, createdAt: 1 }, name: 'book_bids',
      partialFilterExpression: { status: 'OPEN', side: 'BUY' } },
    { key: { userId: 1, createdAt: -1 }, name: 'user_orders' },
  ]);
  await db.collection('trades').createIndexes([
    { key: { symbol: 1, executedAt: -1 }, name: 'tape' },
    { key: { buyerId: 1, executedAt: -1 }, name: 'buyer_hist' },
    { key: { sellerId: 1, executedAt: -1 }, name: 'seller_hist' },
  ]);
  await db.collection('candles_1m').createIndex({ symbol: 1, bucket: 1 }, { unique: true });
  console.log('✔ indexes');
}

// ───────────────────────────── 3. sharding + zones ─────────────────────────────
//
//  orders / trades : RANGE sharded on { symbol:1, _id:1 }
//      → all orders of one symbol live on ONE shard (matching = single-shard reads)
//      → _id in the key lets a hot symbol still split into many chunks
//      → zones pin A–M symbols to rs1 and N–Z to rs2 (data locality we can *show*)
//  users           : HASHED sharded on _id → even spread, no hotspot
//
async function shardEverything(db) {
  const shards = (await admin().command({ listShards: 1 })).shards.map(s => s._id).sort();
  if (shards.length < 2) throw new Error(`need ≥2 shards, found ${shards}`);
  const [zoneA, zoneN] = ['A-M', 'N-Z'];

  await ignore(['AlreadyInitialized', 'NamespaceExists'], () => admin().command({ enableSharding: DB_NAME }));
  await admin().command({ addShardToZone: shards[0], zone: zoneA });
  await admin().command({ addShardToZone: shards[1], zone: zoneN });

  for (const c of ['orders', 'trades']) {
    // zone ranges first → initial chunks are created straight into the right shard
    await ignore(['RangeOverlapConflict', 'ZoneStillInUse'], () => admin().command({
      updateZoneKeyRange: ns(c), min: { symbol: MinKey, _id: MinKey }, max: { symbol: 'N', _id: MinKey }, zone: zoneA }));
    await ignore(['RangeOverlapConflict', 'ZoneStillInUse'], () => admin().command({
      updateZoneKeyRange: ns(c), min: { symbol: 'N', _id: MinKey }, max: { symbol: MaxKey, _id: MaxKey }, zone: zoneN }));
    await ignore(['AlreadyInitialized', 'NamespaceExists'], () => admin().command({
      shardCollection: ns(c), key: { symbol: 1, _id: 1 } }));
  }
  await ignore(['AlreadyInitialized', 'NamespaceExists'], () => admin().command({
    shardCollection: ns('users'), key: { _id: 'hashed' } }));
  console.log(`✔ sharding: orders/trades range {symbol,_id} zones ${zoneA}→${shards[0]} ${zoneN}→${shards[1]}; users hashed`);
}

// ───────────────────────────── 4. seed ─────────────────────────────
const L = (n) => n; // plain JS numbers; all amounts are integers (paise / units) so no float drift
const REF_PRICE = { ASHOKLEY: 22000, BHEL: 25000, INFY: 180000, ITC: 45000, MRF: 12500000,
                    NTPC: 36000, RELIANCE: 290000, TCS: 410000, WIPRO: 55000, ZOMATO: 26000 }; // paise

async function seed(db) {
  await db.collection('symbols').bulkWrite(SYMBOLS.map(s => ({
    updateOne: { filter: { _id: s }, update: { $setOnInsert: { tickSize: 5, lotSize: 1, refPrice: L(REF_PRICE[s]) } }, upsert: true } })));

  const users = db.collection('users');
  if (await users.estimatedDocumentCount() === 0) {
    const docs = [];
    for (let i = 1; i <= 20; i++) {
      const balances = { [QUOTE]: { available: L(100_000_000), locked: L(0) } };   // ₹10,00,000
      for (const s of SYMBOLS) balances[s] = { available: L(1000), locked: L(0) };
      docs.push({ name: `trader${String(i).padStart(2, '0')}`, balances });
    }
    await users.insertMany(docs);
  }
  await db.collection('_meta').updateOne({ _id: 'bootstrap' }, { $set: { at: new Date() } }, { upsert: true });
  console.log(`✔ seed: ${SYMBOLS.length} symbols, ${await users.countDocuments()} users`);
}

export async function setup() {
  const db = await connect();
  await createCollections(db);
  await createIndexes(db);
  await shardEverything(db);
  await seed(db);
}

if (process.argv[1].endsWith('setup.js')) {
  setup().then(() => close()).catch(e => { console.error(e); process.exit(1); });
}
