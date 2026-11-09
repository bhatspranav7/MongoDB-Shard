// Phase 5: REST API + WebSocket push.   `npm start`  → http://localhost:3000
import express from 'express';
import { createServer } from 'http';
import { WebSocketServer } from 'ws';
import { ObjectId } from 'mongodb';
import path from 'path';
import { fileURLToPath } from 'url';
import { connect, col, SYMBOLS } from './db.js';
import { placeOrder, cancelOrder, ExchangeError, stats } from './engine/orders.js';
import { startTicker } from './streams/ticker.js';
import * as q from './analytics/queries.js';

const PORT = process.env.PORT ?? 3000;
const __dirname = path.dirname(fileURLToPath(import.meta.url));

const app = express();
app.use(express.json());
app.use(express.static(path.join(__dirname, '..', 'public')));

const wrap = (fn) => (req, res, next) => fn(req, res).then(r => res.json(r)).catch(next);
const oid = (s) => { try { return new ObjectId(s); } catch { throw new ExchangeError('BAD_ID', 'invalid ObjectId'); } };

// ── reference data ─────────────────────────────────────────────────────────
app.get('/api/symbols', wrap(async () => col('symbols').find().toArray()));
app.get('/api/users', wrap(async () => col('users').find({}, { projection: { name: 1 } }).sort({ name: 1 }).toArray()));
app.get('/api/users/:id', wrap(async (req) => q.portfolio(oid(req.params.id))));
app.get('/api/users/:id/orders', wrap(async (req) =>
  col('orders').find({ userId: oid(req.params.id) }).sort({ createdAt: -1 }).limit(100).toArray()));

// ── trading ────────────────────────────────────────────────────────────────
app.post('/api/orders', wrap(async (req) => {
  const { userId, symbol, side, type = 'LIMIT', price, qty } = req.body;
  return placeOrder(oid(userId), { symbol, side, type, price: price == null ? undefined : Number(price), qty: Number(qty) });
}));
app.delete('/api/orders/:id', wrap(async (req) => cancelOrder(oid(req.body.userId ?? req.query.userId), oid(req.params.id))));

// ── market data ────────────────────────────────────────────────────────────
app.get('/api/book/:symbol', wrap(async (req) => q.orderBook(req.params.symbol, Number(req.query.depth ?? 10))));
app.get('/api/trades/:symbol', wrap(async (req) => q.recentTrades(req.params.symbol, Number(req.query.limit ?? 50))));
app.get('/api/candles/:symbol', wrap(async (req) => q.candles(req.params.symbol, req.query.interval ?? '1m', Number(req.query.limit ?? 120))));
app.get('/api/sma/:symbol', wrap(async (req) => q.movingAverage(req.params.symbol, Number(req.query.window ?? 20))));
app.get('/api/summary', wrap(async () => q.marketSummary()));
app.post('/api/admin/materialize', wrap(async (req) => ({ candles_1m: await q.materializeCandles(Number(req.query.minutes ?? 10)) })));

// ── cluster introspection (the "show the sharding" endpoints) ──────────────
app.get('/api/admin/shards', wrap(async () => q.shardReport()));
app.get('/api/admin/stats', wrap(async () => ({ ...stats, wsClients: wss.clients.size })));
app.get('/api/admin/explain/:symbol', wrap(async (req) => {
  // proves the order-book query targets ONE shard and uses the partial index
  const plan = await col('orders').find({ symbol: req.params.symbol, side: 'SELL', status: 'OPEN' })
    .sort({ price: 1, createdAt: 1 }).limit(1).explain('queryPlanner');
  const qp = plan.queryPlanner;
  const shards = qp.winningPlan?.shards ?? [];
  return {
    shardsTouched: shards.map(s => s.shardName),
    indexUsed: JSON.stringify(shards[0]?.winningPlan ?? qp.winningPlan).match(/"indexName":"([^"]+)"/)?.[1] ?? null,
  };
}));

app.use((err, req, res, _next) => {
  if (err instanceof ExchangeError) return res.status(err.status).json({ error: err.code, message: err.message });
  console.error(err);
  res.status(500).json({ error: 'INTERNAL', message: err.message });
});

// ── WebSocket fan-out, fed by change streams ───────────────────────────────
const server = createServer(app);
const wss = new WebSocketServer({ server, path: '/ws' });
const broadcast = (msg) => { const s = JSON.stringify(msg); for (const c of wss.clients) if (c.readyState === 1) c.send(s); };

await connect();
await startTicker(broadcast);
setInterval(() => q.materializeCandles(2).catch(() => {}), 30_000);   // keep candles_1m fresh
server.listen(PORT, () => console.log(`✔ exchange API http://localhost:${PORT}   symbols: ${SYMBOLS.join(' ')}`));
