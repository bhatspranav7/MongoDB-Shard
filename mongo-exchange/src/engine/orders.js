// Phase 2: order placement / cancellation as MULTI-DOCUMENT, MULTI-SHARD ACID transactions.
//
//  placeOrder() in ONE transaction:
//     users(rs?)   lock funds        ─┐
//     orders(rsX)  insert taker       │ snapshot isolation: every read in here sees one
//     orders(rsX)  decrement makers   │ consistent point in time; if another txn touched a
//     trades(rsX)  insert fills       │ doc we read → WriteConflict → whole txn retried
//     users(rs?)   settle balances   ─┘ (mongos runs 2-phase commit across the shards)
//
import { ObjectId } from 'mongodb';
import { client, col, SYMBOLS } from '../db.js';
import { crosses, fill, lockFor, settlement, unlockFor, QUOTE } from './matcher.js';

export class ExchangeError extends Error {
  constructor(code, msg) { super(msg ?? code); this.code = code; this.status = 400; }
}

// Observability counters — the dashboard shows these to prove retries actually happen under load.
export const stats = { placed: 0, trades: 0, cancelled: 0, txnAttempts: 0, txnRetries: 0, rejected: 0 };

const TXN_OPTS = {
  readConcern: { level: 'snapshot' },
  writeConcern: { w: 'majority' },
  readPreference: 'primary',
  maxCommitTimeMS: 5000,
};

function validate(o) {
  if (!SYMBOLS.includes(o.symbol)) throw new ExchangeError('BAD_SYMBOL', `unknown symbol ${o.symbol}`);
  if (!['BUY', 'SELL'].includes(o.side)) throw new ExchangeError('BAD_SIDE');
  if (!['LIMIT', 'MARKET'].includes(o.type)) throw new ExchangeError('BAD_TYPE');
  if (!Number.isInteger(o.qty) || o.qty < 1) throw new ExchangeError('BAD_QTY', 'qty must be a positive integer');
  if (o.type === 'LIMIT' && (!Number.isInteger(o.price) || o.price < 1)) throw new ExchangeError('BAD_PRICE', 'price (paise) must be a positive integer');
}

/** Conditional, atomic balance move. Fails (matchedCount 0) instead of ever going negative. */
async function moveBalance(session, userId, asset, dAvailable, dLocked) {
  const filter = { _id: userId };
  if (dAvailable < 0) filter[`balances.${asset}.available`] = { $gte: -dAvailable };
  if (dLocked < 0)    filter[`balances.${asset}.locked`]    = { $gte: -dLocked };
  const r = await col('users').updateOne(filter,
    { $inc: { [`balances.${asset}.available`]: dAvailable, [`balances.${asset}.locked`]: dLocked } }, { session });
  if (r.matchedCount === 0) throw new ExchangeError('INSUFFICIENT_FUNDS', `not enough ${asset}`);
}

/**
 * Place an order. Returns { order, trades } after the transaction COMMITS.
 * @param {ObjectId|string} userId
 * @param {{symbol:string, side:'BUY'|'SELL', type:'LIMIT'|'MARKET', price?:number, qty:number}} req
 */
export async function placeOrder(userId, req) {
  userId = new ObjectId(userId);
  validate(req);
  const session = client.startSession();
  let attempts = 0;
  try {
    const out = await session.withTransaction(async () => {
      attempts++; stats.txnAttempts++;
      if (attempts > 1) stats.txnRetries++;

      const orders = col('orders'), trades = col('trades');
      const now = new Date();
      const order = {
        _id: new ObjectId(), symbol: req.symbol, userId, side: req.side, type: req.type,
        price: req.type === 'LIMIT' ? req.price : null, qty: req.qty, remaining: req.qty,
        status: 'OPEN', createdAt: now, updatedAt: now,
      };

      // 1️⃣ reserve funds for LIMIT orders (BUY → INR, SELL → units)
      const lock = lockFor(order);
      if (lock) await moveBalance(session, userId, lock.asset, -lock.amount, +lock.amount);

      // 2️⃣ the taker enters the book
      await orders.insertOne(order, { session });

      // 3️⃣ match against resting orders, best price first, then oldest (price-time priority)
      const opposite = order.side === 'BUY' ? 'SELL' : 'BUY';
      const sort = order.side === 'BUY' ? { price: 1, createdAt: 1 } : { price: -1, createdAt: 1 };
      const fills = [];
      while (order.remaining > 0) {
        const filter = { symbol: order.symbol, side: opposite, status: 'OPEN', userId: { $ne: userId } }; // no self-trade
        if (order.type === 'LIMIT') filter.price = order.side === 'BUY' ? { $lte: order.price } : { $gte: order.price };
        const maker = await orders.findOne(filter, { sort, session });
        if (!maker || !crosses(order, maker)) break;

        const { qty, price } = fill(order, maker);
        const makerLeft = maker.remaining - qty;
        await orders.updateOne({ _id: maker._id, symbol: maker.symbol },
          { $set: { remaining: makerLeft, status: makerLeft === 0 ? 'FILLED' : 'OPEN', updatedAt: now } }, { session });
        order.remaining -= qty;

        const trade = {
          _id: new ObjectId(), symbol: order.symbol, price, qty,
          buyOrderId:  order.side === 'BUY' ? order._id : maker._id,
          sellOrderId: order.side === 'BUY' ? maker._id : order._id,
          buyerId:  order.side === 'BUY' ? userId : maker.userId,
          sellerId: order.side === 'BUY' ? maker.userId : userId,
          takerSide: order.side, executedAt: now,
        };
        await trades.insertOne(trade, { session });
        fills.push(trade);

        // 4️⃣ settle both parties (money/units are conserved — see matcher.settlement)
        for (const d of settlement({ taker: order, maker, qty, price }))
          await moveBalance(session, d.userId, d.asset, d.available, d.locked);
      }

      // 5️⃣ finalise taker: MARKET orders never rest on the book
      if (order.remaining === 0) order.status = 'FILLED';
      else if (order.type === 'MARKET') order.status = 'CANCELLED';
      await orders.updateOne({ _id: order._id, symbol: order.symbol },
        { $set: { remaining: order.remaining, status: order.status, updatedAt: now } }, { session });

      return { order, trades: fills };
    }, TXN_OPTS);

    stats.placed++; stats.trades += out.trades.length;
    return { ...out, attempts };
  } catch (e) {
    if (e instanceof ExchangeError) stats.rejected++;
    throw e;
  } finally {
    await session.endSession();
  }
}

/** Cancel an OPEN order and release its locked funds — one transaction. */
export async function cancelOrder(userId, orderId) {
  userId = new ObjectId(userId); orderId = new ObjectId(orderId);
  const session = client.startSession();
  try {
    return await session.withTransaction(async () => {
      stats.txnAttempts++;
      // find first (read by _id may fan out), then write with the FULL shard key so the
      // update targets one shard — MongoDB 7.0 requires the shard key in findAndModify filters.
      const o = await col('orders').findOne({ _id: orderId, userId }, { session, projection: { symbol: 1 } });
      if (!o) throw new ExchangeError('NOT_OPEN', 'order not found / not yours');
      const before = await col('orders').findOneAndUpdate(
        { _id: orderId, symbol: o.symbol, userId, status: 'OPEN' },
        { $set: { status: 'CANCELLED', updatedAt: new Date() } },
        { session, returnDocument: 'before' });
      if (!before) throw new ExchangeError('NOT_OPEN', 'order not found / not open / not yours');
      const unlock = unlockFor(before);
      if (unlock) await moveBalance(session, userId, unlock.asset, +unlock.amount, -unlock.amount);
      stats.cancelled++;
      return { ...before, status: 'CANCELLED' };
    }, TXN_OPTS);
  } finally {
    await session.endSession();
  }
}

export { QUOTE };
