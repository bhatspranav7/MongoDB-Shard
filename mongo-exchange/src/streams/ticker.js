// Phase 3: CHANGE STREAMS — the "real-time" in real-time exchange.
//
//   trades (sharded, 2 shards) ──watch──► mongos merges both shards' oplogs in
//                                         cluster-time order ──► this process
//                                              ├─► ticks (time-series) insert
//                                              └─► WebSocket broadcast {type:'trade'}
//   orders (sharded)           ──watch──► WebSocket broadcast {type:'book'} per symbol
//
// Why not write ticks inside the trade transaction?  Time-series collections cannot be
// written inside multi-document transactions, so we derive them asynchronously from the
// committed trades — exactly what change streams are for (and it keeps the hot path short).
//
// Resume tokens are persisted in _meta so a restart replays nothing twice and misses nothing.
import { col } from '../db.js';

const RESUME_KEY = (name) => `resume:${name}`;

async function loadToken(name) {
  const d = await col('_meta').findOne({ _id: RESUME_KEY(name) });
  return d?.token;
}
async function saveToken(name, token) {
  await col('_meta').updateOne({ _id: RESUME_KEY(name) }, { $set: { token, at: new Date() } }, { upsert: true });
}

/**
 * Start both watchers. `broadcast(msg)` fans a JSON-able message out to WS clients.
 * Returns a stop() function.
 */
export async function startTicker(broadcast) {
  const streams = [];

  // ── trades → ticks + 'trade' events ───────────────────────────────────────
  const tradeToken = await loadToken('trades');
  const tradeStream = col('trades').watch(
    [{ $match: { operationType: 'insert' } }],
    tradeToken ? { resumeAfter: tradeToken } : {});
  streams.push(tradeStream);

  let n = 0;
  tradeStream.on('change', async (ev) => {
    const t = ev.fullDocument;
    try {
      await col('ticks').insertOne({ ts: t.executedAt, meta: { symbol: t.symbol }, price: t.price, qty: t.qty, takerSide: t.takerSide });
      broadcast({ type: 'trade', symbol: t.symbol, price: t.price, qty: t.qty, takerSide: t.takerSide, at: t.executedAt });
      if (++n % 50 === 0) await saveToken('trades', ev._id);        // checkpoint every 50 events
    } catch (e) { console.error('ticker: trade handler failed', e.message); }
  });

  // ── orders → 'book' events (coalesced: one message per symbol per 100 ms) ──
  const orderToken = await loadToken('orders');
  const orderStream = col('orders').watch(
    [{ $match: { operationType: { $in: ['insert', 'update'] } } },
     { $project: { 'fullDocument.symbol': 1, 'fullDocument.status': 1, operationType: 1 } }],
    { fullDocument: 'updateLookup', ...(orderToken ? { resumeAfter: orderToken } : {}) });
  streams.push(orderStream);

  const dirty = new Set();
  let m = 0;
  orderStream.on('change', async (ev) => {
    if (ev.fullDocument?.symbol) dirty.add(ev.fullDocument.symbol);
    if (++m % 50 === 0) await saveToken('orders', ev._id).catch(() => {});
  });
  const flush = setInterval(() => {
    for (const s of dirty) broadcast({ type: 'book', symbol: s });
    dirty.clear();
  }, 100);

  for (const s of streams) s.on('error', (e) => console.error('change stream error', e.message));
  console.log(`✔ ticker: watching trades${tradeToken ? ' (resumed)' : ''} + orders`);

  return async () => { clearInterval(flush); await Promise.all(streams.map(s => s.close())); };
}
