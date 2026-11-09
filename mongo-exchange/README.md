# MongoExchange — Sharded Real-Time Trading Exchange on MongoDB

NoSQL mini project · Team 1 (Pranav S Bhat, Raghav Bhat, S Shreyas Kumar) · VVCE ISE

A stock-exchange backend where **every hard problem is solved with a MongoDB feature**:

| Problem | MongoDB feature | Where |
|---|---|---|
| Order book for 1 symbol must live on 1 node, but 10 000 symbols must scale out | **Range sharding** on `{symbol, _id}` + **zone sharding** (A–M → rs1, N–Z → rs2) | `src/setup.js` |
| User docs must spread evenly, no hotspot | **Hashed sharding** on `_id` | `src/setup.js` |
| Matching touches orders + trades + 2 users across 2 shards, atomically | **Distributed multi-document transactions** (snapshot isolation, 2-phase commit via mongos) | `src/engine/orders.js` |
| Two takers hit the same resting order at once | **Write-conflict detection** → automatic transaction **retry** | `src/engine/orders.js` |
| Push every trade to screens in real time | **Change streams** (resumable, merged across shards) → WebSocket | `src/streams/ticker.js` |
| Millions of ticks, candles, VWAP | **Time-series collection** + `$dateTrunc`, `$setWindowFields`, `$merge` | `src/analytics/queries.js` |
| Bad data must never enter | **`$jsonSchema` validators**, **partial indexes** | `src/setup.js` |

---

## 1. Architecture

```
                 browser (public/index.html)
                 │  REST          ▲ WebSocket (trades, book updates)
                 ▼                │
          ┌──────────────────────────────┐
          │  Node API  src/server.js     │
          │  ├─ engine/orders.js  (txns) │
          │  ├─ streams/ticker.js (watch)│
          │  └─ analytics/queries.js     │
          └──────────────┬───────────────┘
                         │ mongodb://localhost:27017
                   ┌─────▼─────┐
                   │  mongos   │  query router: targets shard by shard key,
                   └──┬─────┬──┘  coordinates 2-phase commit for cross-shard txns
         ┌────────────┘     └────────────┐
   ┌─────▼──────┐                 ┌──────▼─────┐        ┌────────────┐
   │ rs1 shard  │ zone A-M        │ rs2 shard  │ zone   │ cfgrs      │
   │ ASHOKLEY…  │                 │ NTPC … ZOM │ N-Z    │ config svr │
   │ ITC, INFY  │                 │ RELIANCE   │        │ chunk map, │
   │ MRF, BHEL  │                 │ TCS, WIPRO │        │ zones      │
   └────────────┘                 └────────────┘        └────────────┘
```

### Collections

```
users      {_id, name, balances: { INR:{available,locked}, TCS:{available,locked}, … }}   sharded: {_id:'hashed'}
orders     {_id, symbol, userId, side, type, price, qty, remaining, status, createdAt}    sharded: {symbol:1,_id:1}
trades     {_id, symbol, price, qty, buyOrderId, sellOrderId, buyerId, sellerId, takerSide, executedAt}  sharded: {symbol:1,_id:1}
ticks      TIME-SERIES  {ts, meta:{symbol}, price, qty, takerSide}         timeField ts, metaField meta, TTL 7d
candles_1m materialised OHLCV via $merge                                    unique {symbol, bucket}
symbols    {_id:'TCS', tickSize, lotSize, refPrice}
_meta      change-stream resume tokens
```

All money is **integer paise**, all quantities **integer units** → no floating-point drift, and `$inc` stays exact.

---

## 2. Run it (5 commands)

```bash
npm install
npm run cluster:up      # docker compose up + rs.initiate ×3 + sh.addShard ×2   (~40 s first time)
npm run setup           # collections, validators, indexes, shardCollection, zones, 20 users seeded
npm start               # API + change-stream ticker → http://localhost:3000
npm run bots            # in a 2nd terminal: 8 bots × 60 s of random orders; watch the dashboard move
```

Useful:
```bash
npm test                # unit tests for the pure matching logic (no DB needed)
npm run report          # chunks / docs per shard, symbol → shard map
BOTS=20 SECONDS=120 npm run bots
docker compose exec mongos mongosh --eval 'sh.status()'
npm run cluster:down    # wipes volumes
```

---

## 3. Phase-by-phase (what to show the examiner, in order)

### Phase 0 — cluster (`docker-compose.yml`, `scripts/init-cluster.sh`)
3 replica sets (1 config, 2 shards) + 1 mongos. Single-member RSs are fine: transactions and
change streams need a **replica set**, not multiple members.

### Phase 1 — schema & sharding (`src/setup.js`)
Show `sh.status()` after setup: `exchange.orders` has chunks tagged `A-M` on rs1 and `N-Z` on rs2.
Then `GET /api/admin/explain/TCS` → `shardsTouched: ["rs2"]`, `indexUsed: "book_asks"`: the order-book
read is **single-shard** and uses the **partial index** that only contains OPEN orders.

### Phase 2 — matching engine (`src/engine/orders.js`)
```
placeOrder(user, {TCS, BUY, LIMIT, 4100.00, qty 10})
┌─ withTransaction (readConcern snapshot, writeConcern majority) ─────────────┐
│ 1 users.updateOne  {_id, INR.available ≥ 41 000 00}  $inc avail -, locked + │  ← conditional = never negative
│ 2 orders.insertOne taker (OPEN, remaining 10)                               │
│ 3 loop: orders.findOne best ask  (price ↑, createdAt ↑)  = price-time       │
│      orders.updateOne maker remaining-=q                                    │
│      trades.insertOne                                                       │
│      users.updateOne ×4  (buyer INR↓ units↑, seller units↓ INR↑)            │
│ 4 taker: FILLED | OPEN (rests) | CANCELLED (unfilled MARKET)                 │
└─ commit: mongos runs 2-phase commit across rs1 (users) and rs2 (TCS orders) ┘
```
If another transaction modified a document this one read → `WriteConflict` → `withTransaction`
retries the callback from scratch. `GET /api/admin/stats` shows `txnRetries` climbing under `npm run bots`.

### Phase 3 — change streams (`src/streams/ticker.js`)
`trades.watch()` through mongos merges both shards' oplogs in cluster-time order. Each insert →
(a) one row into the `ticks` time-series collection, (b) one WebSocket broadcast.
Kill `npm start`, run bots for 10 s, restart → it **resumes from the saved token** and backfills ticks.

### Phase 4 — analytics (`src/analytics/queries.js`)
* `/api/candles/TCS?interval=5m` — `$dateTrunc` + `$group` with `$first/$last/$max/$min` → OHLCV + VWAP
* `/api/sma/TCS` — `$setWindowFields` moving average
* `/api/summary` — 24 h change % for every symbol
* `POST /api/admin/materialize` — `$merge` into `candles_1m` (incremental materialised view)
* `/api/book/TCS` — `$group` by price level + `$facet` bids/asks

### Phase 5 — API + dashboard (`src/server.js`, `public/index.html`)
Live order book, tape, candles, portfolio, order entry, and a **cluster panel** showing chunks/docs per
shard and transaction counters — the sharding is visible, not just claimed.

### Phase 6 — load + proof (`scripts/bots.js`)
Ends with a conservation check: Σ(available + locked) of INR and of every symbol across all users is
**exactly** what was seeded. Thousands of concurrent, retried, cross-shard transactions — not one paisa lost.

---

## 4. Pitfalls we hit (and the fix)

1. ❌ Shard key `{symbol:1}` alone → one chunk per symbol can never split (jumbo chunk for a hot stock)
   ✅ `{symbol:1, _id:1}`: still symbol-local, but splittable.
2. ❌ Shard key `{_id:'hashed'}` on orders → matching must scatter-gather to every shard
   ✅ Range key on symbol → matching reads are single-shard.
3. ❌ Storing price as a `double` (4100.05) → `$inc` drift, ledger never balances
   ✅ Integer paise + `multipleOf: 1` in the validator.
4. ❌ Writing ticks inside the trade transaction → time-series collections don't support multi-doc txns
   ✅ Derive ticks from the committed `trades` via a change stream.
5. ❌ `updateOne({_id})` on a sharded collection → broadcast to all shards
   ✅ Always include the shard key in the filter: `{_id, symbol}`.
6. ❌ Checking balance with `findOne` then `updateOne` → two concurrent orders both pass the check
   ✅ Conditional update `{_id, 'balances.INR.available': {$gte: cost}}` + snapshot isolation; `matchedCount 0` ⇒ reject.
7. ❌ Change stream without a saved resume token → restart loses trades
   ✅ Persist `ev._id` every 50 events in `_meta`, `resumeAfter` on boot.
8. ❌ Index on `{symbol, side, status, price}` for all orders → grows forever
   ✅ Partial indexes `partialFilterExpression: {status:'OPEN'}` → tiny, always in RAM.

---

## 5. Viva quick-fire

* **Why not a relational DB?** We could, but we'd hand-roll sharding, change-feeds and time-series buckets. Here each is one command.
* **Isolation level?** Transactions read at `snapshot` (point-in-time, cluster-wide via cluster time); conflicting writes abort the later transaction (optimistic concurrency, first-writer-wins), the driver retries.
* **What if mongos dies mid-commit?** The coordinator shard owns the commit decision; participants recover the decision from it — no half-committed trade.
* **Why zones?** Data locality you can *show* (`sh.status()`), and in real life: put NSE symbols in the Mumbai region shard, NYSE symbols in Virginia.
* **How does a change stream span two shards?** mongos opens a cursor on each shard and merges by cluster-time; it waits until every shard has advanced so ordering is total.
* **Time-series vs normal collection?** Buckets of ~1 h per symbol stored columnar; 10–20× smaller, `$dateTrunc` group runs over far fewer documents; TTL via `expireAfterSeconds`.
* **Self-trade?** Filter `userId: {$ne: taker}` in the maker query.
* **Market order with no liquidity?** Fills what it can, remaining is `CANCELLED` (IOC semantics), nothing stays locked.

## 6. Self-test (can you answer without looking?)

1. A BUY LIMIT at ₹4100 meets a resting SELL at ₹4050. What price trades, and who gets the ₹50 × qty difference?
2. Two bots place BUY orders for TCS simultaneously and both read the same best ask. What happens in each transaction, step by step?
3. Which collections of one `placeOrder` live on rs1 and which on rs2 when the symbol is TCS and the user hashes to rs1?
4. Why is `ticks` not sharded, and what would its shard key be if you did?
5. Why is the order-book partial index on `status:'OPEN'` safe even though `status` isn't in the index key?

<details><summary>Answers</summary>

1. ₹4050 (maker price). The buyer: ₹50 × qty moves from `locked` back to `available` (see `settlement()`).
2. Both read the ask under snapshot isolation. First to write the maker doc wins; the second's write hits `WriteConflict`, the whole callback aborts and `withTransaction` re-runs it — now it sees the updated/filled maker and picks the next ask.
3. `users` (hashed) → rs1; `orders` and `trades` for TCS (zone N-Z) → rs2. Commit is 2-phase across both.
4. Time-series collections can't be written in multi-doc transactions and ours is derived + TTL'd, so it's small; if sharded, key would be `{'meta.symbol':1, ts:1}` (metaField + timeField).
5. A partial index stores only docs matching the filter; the query includes `status:'OPEN'` so the planner knows every candidate is in the index. Filled orders simply aren't there.
</details>
