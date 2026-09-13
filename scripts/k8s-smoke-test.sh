#!/usr/bin/env bash
# Smoke test for the Kubernetes deployment: write, delete a replica pod, keep serving, verify recovery.
set -euo pipefail

NS=distribukv
KEY="k8s:$(date +%s)"
PIDS=()
cleanup() { for p in "${PIDS[@]}"; do kill "$p" 2>/dev/null || true; done; }
trap cleanup EXIT

forward() { # forward POD LOCAL_PORT
  kubectl -n "$NS" port-forward "pod/$1" "$2:8080" > /dev/null 2>&1 &
  PIDS+=($!)
  for _ in $(seq 1 60); do curl -sf "http://localhost:$2/actuator/health" > /dev/null && return 0; sleep 1; done
  echo "port-forward to $1 failed"; exit 1
}

forward distribukv-0 18080
API=http://localhost:18080

echo "waiting for every node to see a fully UP cluster"
# Nodes start out optimistic ("UP" before the first heartbeat), so also require every node's stats.
for _ in $(seq 1 90); do
  S=$(curl -s $API/cluster/status)
  [[ "$(grep -o '"status":"UP"' <<< "$S" | wc -l)" -eq 5 && "$(grep -o '"stats":{' <<< "$S" | wc -l)" -eq 5 ]] && break
  sleep 2
done
curl -s $API/cluster/status; echo
curl -s $API/cluster/status | grep -q '"storage":"mysql"' || { echo "expected MySQL storage"; exit 1; }
curl -s $API/cluster/status | grep -q '"replicationLog":"kafka:kv-replication"' || { echo "expected Kafka replication log"; exit 1; }
curl -s $API/cluster/status | grep -q '"membership":"redis"' || { echo "expected Redis heartbeats"; exit 1; }
echo "stack verified: MySQL storage, Kafka replication log, Redis heartbeats"

curl -sf -X PUT -H 'Content-Type: text/plain' --data-raw v1 "$API/kv/$KEY?consistency=STRONG"; echo
RING=$(curl -s "$API/cluster/ring?key=$KEY")
VICTIM=""
for pod in $(grep -o 'distribukv-[0-9]' <<< "$RING"); do
  if [[ "$pod" != distribukv-0 ]]; then VICTIM=$pod; break; fi
done
OLD_UID=$(kubectl -n "$NS" get pod "$VICTIM" -o jsonpath='{.metadata.uid}')
echo "deleting replica pod $VICTIM"
kubectl -n "$NS" delete pod "$VICTIM" --wait=false

curl -sf -X PUT -H 'Content-Type: text/plain' --data-raw v2 "$API/kv/$KEY?consistency=STRONG"; echo
[[ "$(curl -sf "$API/kv/$KEY?consistency=STRONG")" == *'"value":"v2"'* ]] || { echo "strong read failed"; exit 1; }
echo "strong reads and writes kept working while $VICTIM was down"

# Wait for the *replacement* pod: the old one keeps its Ready condition while terminating.
for _ in $(seq 1 180); do
  UID_NOW=$(kubectl -n "$NS" get pod "$VICTIM" -o jsonpath='{.metadata.uid}' 2>/dev/null || true)
  READY=$(kubectl -n "$NS" get pod "$VICTIM" -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null || true)
  [[ -n "$UID_NOW" && "$UID_NOW" != "$OLD_UID" && "$READY" == True ]] && break
  sleep 1
done
echo "replacement pod $VICTIM is ready"
forward "$VICTIM" 18081
for _ in $(seq 1 60); do
  if [[ "$(curl -s "http://localhost:18081/internal/kv/$KEY")" == *'"value":"v2"'* ]]; then
    echo "$VICTIM recovered the latest value (persistent volume + hinted handoff)"
    echo "KUBERNETES SMOKE TEST PASSED"
    exit 0
  fi
  sleep 1
done
echo "$VICTIM never converged"; exit 1
