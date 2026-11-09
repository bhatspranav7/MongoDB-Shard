// Unit tests for the pure matching logic (no MongoDB needed):  npm test
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { crosses, fill, lockFor, settlement, unlockFor } from '../src/engine/matcher.js';

const o = (over) => ({ symbol: 'TCS', userId: 'u', side: 'BUY', type: 'LIMIT', price: 410000, qty: 10, remaining: 10, ...over });

test('limit buy crosses an ask at or below its price only', () => {
  assert.equal(crosses(o(), o({ side: 'SELL', price: 410000, userId: 'v' })), true);
  assert.equal(crosses(o(), o({ side: 'SELL', price: 409000, userId: 'v' })), true);
  assert.equal(crosses(o(), o({ side: 'SELL', price: 410005, userId: 'v' })), false);
  assert.equal(crosses(o(), o({ side: 'BUY', userId: 'v' })), false, 'same side never crosses');
});

test('market order crosses anything on the other side', () => {
  assert.equal(crosses(o({ type: 'MARKET', price: null }), o({ side: 'SELL', price: 999999999, userId: 'v' })), true);
});

test('fill takes min remaining at MAKER price (price-time priority)', () => {
  const taker = o({ remaining: 7 }), maker = o({ side: 'SELL', price: 405000, remaining: 20, userId: 'v' });
  assert.deepEqual(fill(taker, maker), { qty: 7, price: 405000 });
});

test('limit orders lock the right asset; market orders lock nothing', () => {
  assert.deepEqual(lockFor(o()), { asset: 'INR', amount: 410000 * 10 });
  assert.deepEqual(lockFor(o({ side: 'SELL' })), { asset: 'TCS', amount: 10 });
  assert.equal(lockFor(o({ type: 'MARKET', price: null })), null);
});

function conserved(deltas) {
  const byAsset = {};
  for (const d of deltas) byAsset[d.asset] = (byAsset[d.asset] ?? 0) + d.available + d.locked;
  return Object.values(byAsset).every(v => v === 0);
}

test('settlement conserves money and units, refunds price improvement to limit buyer', () => {
  const taker = o({ userId: 'buyer', price: 410000 });                       // willing to pay 4100.00
  const maker = o({ userId: 'seller', side: 'SELL', price: 405000, remaining: 10 }); // resting ask 4050.00
  const d = settlement({ taker, maker, qty: 10, price: 405000 });
  assert.ok(conserved(d), 'Σ(available+locked) per asset must be 0');
  const buyerInr = d.find(x => x.userId === 'buyer' && x.asset === 'INR');
  assert.equal(buyerInr.available, 50000, 'refund (4100-4050)×10 = ₹500 = 50000 paise');
  assert.equal(buyerInr.locked, -(405000 * 10 + 50000), 'entire lock released');
  assert.deepEqual(d.find(x => x.userId === 'seller' && x.asset === 'INR'), { userId: 'seller', asset: 'INR', available: 4050000, locked: 0 });
  assert.deepEqual(d.find(x => x.userId === 'seller' && x.asset === 'TCS'), { userId: 'seller', asset: 'TCS', available: 0, locked: -10 });
});

test('market taker pays from available directly', () => {
  const taker = o({ userId: 'buyer', type: 'MARKET', price: null, remaining: 3 });
  const maker = o({ userId: 'seller', side: 'SELL', price: 400000, remaining: 5 });
  const d = settlement({ taker, maker, qty: 3, price: 400000 });
  assert.ok(conserved(d));
  assert.deepEqual(d.find(x => x.userId === 'buyer' && x.asset === 'INR'), { userId: 'buyer', asset: 'INR', available: -1200000, locked: 0 });
});

test('cancel unlocks only the unfilled remainder', () => {
  assert.deepEqual(unlockFor(o({ remaining: 4 })), { asset: 'INR', amount: 410000 * 4 });
  assert.deepEqual(unlockFor(o({ side: 'SELL', remaining: 4 })), { asset: 'TCS', amount: 4 });
  assert.equal(unlockFor(o({ remaining: 0 })), null);
});
