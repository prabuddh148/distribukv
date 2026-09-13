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
for _ in $(seq 1 90); do
  [[ "$(curl -s $API/cluster/status | grep -o '"status":"UP"' | wc -l)" -eq 5 ]] && break
  sleep 2
done
curl -s $API/cluster/status; echo

curl -sf -X PUT -H 'Content-Type: text/plain' --data-raw v1 "$API/kv/$KEY?consistency=STRONG"; echo
VICTIM=$(curl -s "$API/cluster/ring?key=$KEY" | grep -o 'distribukv-[0-9]' | grep -v '^distribukv-0$' | head -1)
echo "deleting replica pod $VICTIM"
kubectl -n "$NS" delete pod "$VICTIM" --wait=false

curl -sf -X PUT -H 'Content-Type: text/plain' --data-raw v2 "$API/kv/$KEY?consistency=STRONG"; echo
[[ "$(curl -sf "$API/kv/$KEY?consistency=STRONG")" == *'"value":"v2"'* ]] || { echo "strong read failed"; exit 1; }
echo "strong reads and writes kept working while $VICTIM was down"

kubectl -n "$NS" wait --for=condition=Ready "pod/$VICTIM" --timeout=180s
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
