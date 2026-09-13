#!/usr/bin/env bash
# End-to-end failure scenarios against a running 5-node cluster (ports 8081..8085).
#
#   scripts/chaos-test.sh local     nodes started with scripts/cluster.sh
#   scripts/chaos-test.sh compose   nodes started with docker compose
set -euo pipefail

MODE="${1:-local}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
NODES=5
KEY="chaos:$(date +%s)"
START=$(date +%s)

url()  { echo "http://localhost:$((8080 + $1))"; }
log()  { printf '[%3ss] %s\n' "$(( $(date +%s) - START ))" "$*"; }
fail() { log "FAIL: $*"; exit 1; }

stop_node() {
  if [[ "$MODE" == compose ]]; then docker compose kill "node$1" > /dev/null; else scripts/cluster.sh stop-node "$1" > /dev/null; fi
  log "killed node$1"
}
start_node() {
  if [[ "$MODE" == compose ]]; then docker compose start "node$1" > /dev/null; else scripts/cluster.sh start-node "$1" > /dev/null; fi
  log "restarted node$1"
}

wait_healthy() {
  for _ in $(seq 1 120); do
    curl -sf "$(url "$1")/actuator/health" > /dev/null && return 0
    sleep 1
  done
  fail "node$1 did not become healthy"
}

# request METHOD NODE PATH [BODY] -> prints "STATUS BODY"
request() {
  local out
  out=$(curl -s -o /tmp/kv-body -w '%{http_code}' -X "$1" -H 'Content-Type: text/plain' ${4:+--data-raw "$4"} "$(url "$2")$3" || true)
  echo "$out $(cat /tmp/kv-body 2>/dev/null)"
}

expect() { # expect STATUS "STATUS BODY" description
  [[ "${2%% *}" == "$1" ]] || fail "$3: expected HTTP $1, got: $2"
  log "ok  - $3 (HTTP $1)"
}

expect_eventually() { # expect_eventually STATUS METHOD NODE PATH description  (eventual reads may lag briefly)
  local out
  for _ in $(seq 1 20); do
    out=$(request "$2" "$3" "$4")
    [[ "${out%% *}" == "$1" ]] && break
    sleep 0.5
  done
  expect "$1" "$out" "$5"
}

value_on() { # value stored locally on a node (bypasses coordination)
  curl -s "$(url "$1")/internal/kv/$KEY" | sed -n 's/.*"value":"\([^"]*\)".*/\1/p'
}

wait_for_value() {
  for _ in $(seq 1 60); do
    [[ "$(value_on "$1")" == "$2" ]] && { log "ok  - node$1 converged to '$2'"; return 0; }
    sleep 1
  done
  fail "node$1 never converged to '$2' (has '$(value_on "$1")')"
}

for i in $(seq 1 $NODES); do wait_healthy "$i"; done
sleep 3 # let failure detectors see every peer
log "cluster healthy - stack: $(curl -s "$(url 1)/cluster/status" | grep -o '"stack":{[^}]*}')"

replicas=$(curl -s "$(url 1)/cluster/ring?key=$KEY" | grep -o 'node[0-9]*' | sed 's/node//' | tr '\n' ' ')
read -r R1 R2 R3 <<< "$replicas"
C=""
for i in $(seq 1 $NODES); do
  if [[ " $R1 $R2 $R3 " != *" $i "* ]]; then C=$i; break; fi
done
log "key $KEY -> replicas node$R1 node$R2 node$R3, coordinator node$C"

log "--- scenario 1: one replica crashes"
expect 200 "$(request PUT "$C" "/kv/$KEY?consistency=STRONG" v1)" "strong PUT v1"
stop_node "$R1"
expect 200 "$(request GET "$C" "/kv/$KEY?consistency=STRONG")" "strong GET with 2/3 replicas"
expect 200 "$(request PUT "$C" "/kv/$KEY?consistency=STRONG" v2)" "strong PUT v2 with 2/3 replicas"

log "--- scenario 2: quorum lost (two replicas down)"
stop_node "$R2"
expect 503 "$(request PUT "$C" "/kv/$KEY?consistency=STRONG" v3)" "strong PUT rejected without quorum"
expect 200 "$(request PUT "$C" "/kv/$KEY?consistency=EVENTUAL" v3)" "eventual PUT accepted with 1/3 replicas"
expect_eventually 200 GET "$C" "/kv/$KEY?consistency=EVENTUAL" "eventual GET with 1/3 replicas"

log "--- scenario 3: recovery via hinted handoff"
start_node "$R1"; start_node "$R2"
wait_healthy "$R1"; wait_healthy "$R2"
wait_for_value "$R1" v3
wait_for_value "$R2" v3

log "--- scenario 4: network partition around a replica"
expect 200 "$(request POST "$C" "/cluster/nodes/node$R3/isolate?enabled=true")" "isolate node$R3"
expect 503 "$(request GET "$R3" "/kv/$KEY")" "isolated node refuses traffic"
expect 200 "$(request PUT "$C" "/kv/$KEY?consistency=STRONG" v4)" "strong PUT during partition"
expect 200 "$(request POST "$C" "/cluster/nodes/node$R3/isolate?enabled=false")" "heal node$R3"
wait_for_value "$R3" v4

if [[ "$MODE" == compose ]]; then
  log "--- evidence from the infrastructure"
  row=$(docker compose exec -T mysql mysql -uroot -pdistribukv -N -e "SELECT v FROM kv_node$R3.kv_entries WHERE k='$KEY'" 2>/dev/null | tr -d '\r')
  [[ "$row" == v4 ]] || fail "expected v4 in node$R3's MySQL database, found '$row'"
  log "ok  - node$R3's own MySQL database (kv_node$R3) holds '$row'"
  log "Kafka consumer group of node$R1 (replication log offsets and lag):"
  docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
    --describe --group "kv-node$R1" 2>/dev/null | sed -n '1,9p'
fi

log "ALL FAILURE SCENARIOS PASSED"
