// Pure matching logic — no DB here, so it is unit-testable.
// Prices are integer PAISE, quantities integer units.

export const QUOTE = 'INR';

/** Can `taker` trade against resting order `maker`? (price-time priority, maker price wins) */
export function crosses(taker, maker) {
  if (taker.symbol !== maker.symbol || taker.side === maker.side) return false;
  if (taker.type === 'MARKET') return true;
  return taker.side === 'BUY' ? taker.price >= maker.price : taker.price <= maker.price;
}

/** Fill size & price for one match. Execution happens at the MAKER's (resting) price. */
export function fill(taker, maker) {
  const qty = Math.min(taker.remaining, maker.remaining);
  return { qty, price: maker.price };
}

/**
 * Funds a LIMIT order must lock when it enters the book.
 * BUY  → lock INR = price × qty     SELL → lock <symbol> units = qty
 * MARKET orders lock nothing up-front; each fill is paid from `available` directly.
 */
export function lockFor(order) {
  if (order.type === 'MARKET') return null;
  return order.side === 'BUY'
    ? { asset: QUOTE, amount: order.price * order.qty }
    : { asset: order.symbol, amount: order.qty };
}

/**
 * Balance deltas for one trade, per participant.  Each entry is { userId, asset, available, locked }.
 * Invariant: Σ(available+locked) per asset across both parties is unchanged (money is conserved).
 */
export function settlement({ taker, maker, qty, price }) {
  const notional = qty * price;
  const buy = taker.side === 'BUY' ? taker : maker;
  const sell = taker.side === 'BUY' ? maker : taker;
  const d = [];

  // Buyer pays INR, receives units.
  if (buy.type === 'MARKET') d.push({ userId: buy.userId, asset: QUOTE, available: -notional, locked: 0 });
  else {
    // locked at buy.price, executed at maker price (≤ buy.price) → refund the difference
    const refund = (buy.price - price) * qty;
    d.push({ userId: buy.userId, asset: QUOTE, available: refund, locked: -(notional + refund) });
  }
  d.push({ userId: buy.userId, asset: buy.symbol, available: qty, locked: 0 });

  // Seller gives units, receives INR.
  if (sell.type === 'MARKET') d.push({ userId: sell.userId, asset: sell.symbol, available: -qty, locked: 0 });
  else d.push({ userId: sell.userId, asset: sell.symbol, available: 0, locked: -qty });
  d.push({ userId: sell.userId, asset: QUOTE, available: notional, locked: 0 });

  return d.filter(x => x.available !== 0 || x.locked !== 0);
}

/** Funds returned when an OPEN limit order is cancelled (or a limit order's unfilled tail expires). */
export function unlockFor(order) {
  if (order.type === 'MARKET' || order.remaining === 0) return null;
  return order.side === 'BUY'
    ? { asset: QUOTE, amount: order.price * order.remaining }
    : { asset: order.symbol, amount: order.remaining };
}
