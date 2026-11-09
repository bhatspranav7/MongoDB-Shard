// Phase 6: load generator.  N concurrent bots fire random orders straight at the engine
// (no HTTP) so you can watch throughput, fills and — importantly — transaction retries.
//   npm run bots                    (defaults: 8 bots, 60 s)
//   BOTS=20 SECONDS=120 npm run bots
import { connect, close, col, SYMBOLS } from '../src/db.js';
import { placeOrder, stats, ExchangeError } from '../src/engine/orders.js';

const BOTS = Number(process.env.BOTS ?? 8), SECONDS = Number(process.env.SECONDS ?? 60);
const rnd = (a, b) => a + Math.floor(Math.random() * (b - a + 1));
const pick = (xs) => xs[rnd(0, xs.length - 1)];

async function bot(id, users, refs, until) {
  let n = 0, errs = 0, conflicts = 0;
  while (Date.now() < until) {
    const symbol = pick(SYMBOLS), ref = refs[symbol];
    const side = Math.random() < .5 ? 'BUY' : 'SELL';
    const type = Math.random() < .15 ? 'MARKET' : 'LIMIT';
    // limit prices cluster ±1% around reference so the book actually crosses
    const price = Math.round(ref * (1 + (Math.random() - .5) * .02) / 5) * 5;
    try {
      const r = await placeOrder(pick(users)._id, { symbol, side, type, price, qty: rnd(1, 50) });
      n++; if (r.attempts > 1) conflicts++;
    } catch (e) {
      if (e instanceof ExchangeError) errs++;            // e.g. INSUFFICIENT_FUNDS — expected sometimes
      else { console.error(`bot${id}`, e.codeName ?? e.message); await new Promise(r => setTimeout(r, 200)); }
    }
  }
  return { n, errs, conflicts };
}

const db = await connect();
const users = await col('users').find({}, { projection: { _id: 1 } }).toArray();
const refs = Object.fromEntries((await col('symbols').find().toArray()).map(s => [s._id, s.refPrice]));
console.log(`▶ ${BOTS} bots × ${SECONDS}s against ${SYMBOLS.length} symbols…`);
const t0 = Date.now(), until = t0 + SECONDS * 1000;
const tick = setInterval(() => {
  const s = (Date.now() - t0) / 1000;
  console.log(`  ${s.toFixed(0).padStart(3)}s  orders ${stats.placed}  trades ${stats.trades}  rejected ${stats.rejected}  txnRetries ${stats.txnRetries}  (${(stats.placed / s).toFixed(1)} orders/s)`);
}, 5000);
const res = await Promise.all(Array.from({ length: BOTS }, (_, i) => bot(i, users, refs, until)));
clearInterval(tick);
const secs = (Date.now() - t0) / 1000;
console.log('\n■ done', { seconds: secs.toFixed(1), ordersPerSec: (stats.placed / secs).toFixed(1), ...stats,
  perBot: res.map(r => `${r.n}/${r.errs}e/${r.conflicts}c`) });

// invariant check: money & units are conserved (sum of available+locked never changes)
const [tot] = await col('users').aggregate([
  { $project: { b: { $objectToArray: '$balances' } } }, { $unwind: '$b' },
  { $group: { _id: '$b.k', total: { $sum: { $add: ['$b.v.available', '$b.v.locked'] } } } },
  { $group: { _id: null, assets: { $push: { k: '$_id', v: '$total' } } } },
]).toArray();
const expected = { INR: users.length * 100_000_000 };
for (const s of SYMBOLS) expected[s] = users.length * 1000;
const bad = tot.assets.filter(a => a.v !== expected[a.k]);
console.log(bad.length ? `✖ CONSERVATION BROKEN: ${JSON.stringify(bad)}` : `✔ conservation holds: ${tot.assets.length} assets, totals unchanged (ACID works)`);
await close();
