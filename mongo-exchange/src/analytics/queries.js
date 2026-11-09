// Phase 4: AGGREGATION PIPELINES on the time-series collection + order book + shard metadata.
import { client, col, DB_NAME, SYMBOLS } from '../db.js';

const UNIT = { '1m': ['minute', 1], '5m': ['minute', 5], '15m': ['minute', 15], '1h': ['hour', 1], '1d': ['day', 1] };

/** Live order book: price levels aggregated from OPEN orders (reads one shard thanks to the shard key). */
export async function orderBook(symbol, depth = 10) {
  const [r] = await col('orders').aggregate([
    { $match: { symbol, status: 'OPEN', type: 'LIMIT' } },
    { $group: { _id: { side: '$side', price: '$price' }, qty: { $sum: '$remaining' }, orders: { $sum: 1 } } },
    { $project: { _id: 0, side: '$_id.side', price: '$_id.price', qty: 1, orders: 1 } },
    { $facet: {
        bids: [{ $match: { side: 'BUY' } },  { $sort: { price: -1 } }, { $limit: depth }],
        asks: [{ $match: { side: 'SELL' } }, { $sort: { price: 1 } },  { $limit: depth }],
    } },
    { $addFields: {
        spread: { $subtract: [{ $first: '$asks.price' }, { $first: '$bids.price' }] },
        bidVolume: { $sum: '$bids.qty' }, askVolume: { $sum: '$asks.qty' },
    } },
  ]).toArray();
  return { symbol, ...r };
}

export function recentTrades(symbol, limit = 50) {
  return col('trades').find({ symbol }, { projection: { symbol: 0 } }).sort({ executedAt: -1 }).limit(limit).toArray();
}

/** OHLCV + VWAP candles straight from the time-series collection using $dateTrunc bucketing. */
export async function candles(symbol, interval = '1m', limit = 120) {
  const [unit, binSize] = UNIT[interval] ?? UNIT['1m'];
  return col('ticks').aggregate([
    { $match: { 'meta.symbol': symbol } },
    { $sort: { ts: 1 } },
    { $group: {
        _id: { $dateTrunc: { date: '$ts', unit, binSize } },
        open: { $first: '$price' }, high: { $max: '$price' }, low: { $min: '$price' }, close: { $last: '$price' },
        volume: { $sum: '$qty' }, notional: { $sum: { $multiply: ['$price', '$qty'] } }, trades: { $sum: 1 },
    } },
    { $project: { _id: 0, bucket: '$_id', open: 1, high: 1, low: 1, close: 1, volume: 1, trades: 1,
                  vwap: { $round: [{ $divide: ['$notional', '$volume'] }, 0] } } },
    { $sort: { bucket: -1 } }, { $limit: limit }, { $sort: { bucket: 1 } },
  ]).toArray();
}

/** 24h market summary for every symbol: last, change %, high, low, volume. */
export async function marketSummary(hours = 24) {
  const since = new Date(Date.now() - hours * 3600e3);
  const rows = await col('ticks').aggregate([
    { $match: { ts: { $gte: since } } },
    { $sort: { ts: 1 } },
    { $group: { _id: '$meta.symbol', open: { $first: '$price' }, last: { $last: '$price' },
                high: { $max: '$price' }, low: { $min: '$price' }, volume: { $sum: '$qty' }, trades: { $sum: 1 },
                lastAt: { $last: '$ts' } } },
    { $project: { _id: 0, symbol: '$_id', open: 1, last: 1, high: 1, low: 1, volume: 1, trades: 1, lastAt: 1,
                  changePct: { $round: [{ $multiply: [{ $divide: [{ $subtract: ['$last', '$open'] }, '$open'] }, 100] }, 2] } } },
    { $sort: { changePct: -1 } },
  ]).toArray();
  const seen = new Set(rows.map(r => r.symbol));
  const refs = await col('symbols').find({ _id: { $nin: [...seen] } }).toArray();
  return [...rows, ...refs.map(r => ({ symbol: r._id, last: r.refPrice, open: r.refPrice, changePct: 0, volume: 0, trades: 0 }))];
}

/** $setWindowFields demo: 20-trade simple moving average + cumulative volume for one symbol. */
export function movingAverage(symbol, window = 20, limit = 200) {
  return col('ticks').aggregate([
    { $match: { 'meta.symbol': symbol } },
    { $sort: { ts: -1 } }, { $limit: limit }, { $sort: { ts: 1 } },
    { $setWindowFields: {
        sortBy: { ts: 1 },
        output: {
          sma: { $avg: '$price', window: { documents: [-(window - 1), 0] } },
          cumVolume: { $sum: '$qty', window: { documents: ['unbounded', 'current'] } },
        } } },
    { $project: { _id: 0, ts: 1, price: 1, qty: 1, sma: { $round: ['$sma', 0] }, cumVolume: 1 } },
  ]).toArray();
}

/** Materialised view: roll the last `minutes` of ticks into candles_1m ($merge = incremental upsert). */
export async function materializeCandles(minutes = 10) {
  const since = new Date(Date.now() - minutes * 60e3);
  await col('ticks').aggregate([
    { $match: { ts: { $gte: since } } },
    { $sort: { ts: 1 } },
    { $group: { _id: { symbol: '$meta.symbol', bucket: { $dateTrunc: { date: '$ts', unit: 'minute' } } },
                open: { $first: '$price' }, high: { $max: '$price' }, low: { $min: '$price' }, close: { $last: '$price' },
                volume: { $sum: '$qty' }, trades: { $sum: 1 } } },
    { $project: { _id: 0, symbol: '$_id.symbol', bucket: '$_id.bucket', open: 1, high: 1, low: 1, close: 1, volume: 1, trades: 1 } },
    { $merge: { into: 'candles_1m', on: ['symbol', 'bucket'], whenMatched: 'replace', whenNotMatched: 'insert' } },
  ]).toArray();
  return col('candles_1m').estimatedDocumentCount();
}

/** Per-user portfolio valued at last price (uses $lookup against symbols for a fallback price). */
export async function portfolio(userId) {
  const user = await col('users').findOne({ _id: userId });
  if (!user) return null;
  const summary = await marketSummary();
  const px = Object.fromEntries(summary.map(s => [s.symbol, s.last]));
  const positions = Object.entries(user.balances).map(([asset, b]) => ({
    asset, ...b, total: b.available + b.locked,
    valueInr: asset === 'INR' ? b.available + b.locked : (b.available + b.locked) * (px[asset] ?? 0),
  }));
  return { _id: user._id, name: user.name, positions, netWorthInr: positions.reduce((s, p) => s + p.valueInr, 0) };
}

/** Shard report: where does the data physically live? (chunks from config DB + live doc counts) */
export async function shardReport() {
  const admin = client.db('admin'), config = client.db('config');
  const shards = (await admin.command({ listShards: 1 })).shards.map(s => ({ id: s._id, host: s.host }));
  const zones = await config.collection('shards').find({}, { projection: { tags: 1 } }).toArray();
  for (const s of shards) s.zones = zones.find(z => z._id === s.id)?.tags ?? [];

  const collections = {};
  for (const name of ['orders', 'trades', 'users']) {
    const meta = await config.collection('collections').findOne({ _id: `${DB_NAME}.${name}` });
    if (!meta) continue;
    const chunks = await config.collection('chunks').aggregate([
      { $match: { uuid: meta.uuid } }, { $group: { _id: '$shard', chunks: { $sum: 1 } } },
    ]).toArray();
    // $shardedDataDistribution gives per-shard doc counts without touching user data
    let dist = [];
    try {
      dist = await admin.aggregate([{ $shardedDataDistribution: {} }, { $match: { ns: `${DB_NAME}.${name}` } }]).toArray();
    } catch { /* needs 6.0.3+ */ }
    collections[name] = {
      shardKey: meta.key,
      perShard: shards.map(s => ({
        shard: s.id,
        chunks: chunks.find(c => c._id === s.id)?.chunks ?? 0,
        docs: dist[0]?.shards?.find(d => d.shardName === s.id)?.numOwnedDocuments ?? null,
      })),
    };
  }
  // which symbols are on which shard (resolved through zone ranges)
  const symbolShard = Object.fromEntries(SYMBOLS.map(s => [s, shards.find(sh => sh.zones.includes(s < 'N' ? 'A-M' : 'N-Z'))?.id]));
  return { shards, collections, symbolShard };
}
