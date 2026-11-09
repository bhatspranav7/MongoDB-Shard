#!/usr/bin/env bash
# Phase 0: turn the 4 containers into ONE sharded cluster.
# Run once after `docker compose up -d`.  Idempotent (re-running is harmless).
set -euo pipefail
cd "$(dirname "$0")/.."

ex() { docker compose exec -T "$1" mongosh --quiet --port "$2" --eval "$3"; }

echo "▶ 1/4 initiating config server replica set (cfgrs)"
ex cfg1 27019 'try { rs.initiate({_id:"cfgrs", configsvr:true, members:[{_id:0, host:"cfg1:27019"}]}) } catch(e) { print(e.codeName) }'

echo "▶ 2/4 initiating shard replica sets (rs1, rs2)"
ex shard1 27018 'try { rs.initiate({_id:"rs1", members:[{_id:0, host:"shard1:27018"}]}) } catch(e) { print(e.codeName) }'
ex shard2 27018 'try { rs.initiate({_id:"rs2", members:[{_id:0, host:"shard2:27018"}]}) } catch(e) { print(e.codeName) }'

echo "▶ 3/4 waiting for primaries"
for s in "cfg1 27019" "shard1 27018" "shard2 27018"; do
  set -- $s
  until ex "$1" "$2" 'db.hello().isWritablePrimary' | grep -q true; do sleep 1; done
done
# mongos only accepts connections once the config RS has a primary
until ex mongos 27017 'db.adminCommand("ping").ok' 2>/dev/null | grep -q 1; do sleep 1; done

echo "▶ 4/4 registering shards with mongos"
ex mongos 27017 'try { sh.addShard("rs1/shard1:27018") } catch(e) { print(e.codeName) }'
ex mongos 27017 'try { sh.addShard("rs2/shard2:27018") } catch(e) { print(e.codeName) }'

echo
ex mongos 27017 'printjson(db.adminCommand({listShards:1}).shards.map(s => s._id + " -> " + s.host))'
echo "✅ cluster ready at mongodb://localhost:27017  — next: npm run setup"
