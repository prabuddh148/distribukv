#!/usr/bin/env bash
# Smoke test for the all-in-one container: full stack up, reads/writes work, public guard active.
#   deploy/huggingface/smoke-test.sh http://localhost:7860
set -euo pipefail

BASE="${1:-http://localhost:7860}"
echo "waiting for the stack at $BASE"
S=""
# Nodes start out optimistic ("UP" before the first heartbeat), so wait until every node reports
# its own stats and failure detection has switched to Redis.
for _ in $(seq 1 100); do
  S=$(curl -s --max-time 5 "$BASE/cluster/status" || true)
  [[ "$(grep -o '"stats":{' <<< "$S" | wc -l)" -eq 5 && "$(grep -o '"status":"UP"' <<< "$S" | wc -l)" -eq 5 ]] \
    && grep -q '"membership":"redis"' <<< "$S" && break
  sleep 3
done
echo "${S:0:700}"
grep -q '"storage":"mysql"' <<< "$S" || { echo "expected MySQL storage"; exit 1; }
grep -q '"replicationLog":"kafka:kv-replication"' <<< "$S" || { echo "expected Kafka replication log"; exit 1; }
grep -q '"membership":"redis"' <<< "$S" || { echo "expected Redis heartbeats"; exit 1; }
[[ "$(grep -o '"status":"UP"' <<< "$S" | wc -l)" -eq 5 ]] || { echo "not all 5 nodes are UP"; exit 1; }

KEY="smoke:$(date +%s)"
curl -sf -X PUT -H 'Content-Type: text/plain' --data-raw v1 "$BASE/kv/$KEY?consistency=STRONG"; echo
EVENTUAL=$(curl -s -X PUT -H 'Content-Type: text/plain' --data-raw v2 "$BASE/kv/$KEY?consistency=EVENTUAL")
echo "$EVENTUAL"
grep -q '"replicationLog":"kafka:' <<< "$EVENTUAL" || { echo "EVENTUAL write was not acknowledged by Kafka"; exit 1; }
for _ in $(seq 1 20); do
  curl -s "$BASE/kv/$KEY?consistency=STRONG" | grep -q '"value":"v2"' && break
  sleep 1
done
curl -s "$BASE/kv/$KEY?consistency=STRONG" | grep -q '"value":"v2"' || { echo "STRONG read did not return v2"; exit 1; }

[[ "$(curl -s -o /dev/null -w '%{http_code}' "$BASE/")" == 200 ]] || { echo "dashboard not served"; exit 1; }
GUARD=$(curl -s -o /dev/null -w '%{http_code}' -H 'X-Forwarded-For: 203.0.113.9' "$BASE/internal/ping")
[[ "$GUARD" == 403 ]] || { echo "public guard not active for proxied traffic (got HTTP $GUARD)"; exit 1; }

echo "ALL-IN-ONE SMOKE TEST PASSED"
