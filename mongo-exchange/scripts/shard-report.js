// Prints the shard report as text:  npm run report
import { connect, close } from '../src/db.js';
import { shardReport } from '../src/analytics/queries.js';
await connect();
const r = await shardReport();
console.log('shards:'); for (const s of r.shards) console.log(`  ${s.id.padEnd(5)} ${s.host}  zones=[${s.zones}]`);
for (const [c, v] of Object.entries(r.collections)) {
  console.log(`\n${c}  shardKey=${JSON.stringify(v.shardKey)}`);
  for (const p of v.perShard) console.log(`  ${p.shard.padEnd(5)} chunks=${p.chunks}  docs=${p.docs ?? '?'}`);
}
console.log('\nsymbol → shard:', r.symbolShard);
await close();
