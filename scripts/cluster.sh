#!/usr/bin/env bash
# Run a local DistribuKV cluster as plain JVM processes (no Docker needed).
#
#   scripts/cluster.sh start            build if needed, start all nodes on ports 8081..808N
#   scripts/cluster.sh stop             stop all nodes
#   scripts/cluster.sh start-node 3     start one node
#   scripts/cluster.sh stop-node 3      kill one node (simulates a crash)
#   scripts/cluster.sh status
#
# NODES (default 5) controls the cluster size.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
NODES="${NODES:-5}"
JAR="target/distribukv.jar"

port() { echo $((8080 + $1)); }

cluster_spec() {
  local spec=""
  for i in $(seq 1 "$NODES"); do spec+="${spec:+,}node$i=http://localhost:$(port "$i")"; done
  echo "$spec"
}

start_node() {
  local i=$1
  mkdir -p .run logs data
  if [[ -f ".run/node$i.pid" ]] && kill -0 "$(cat ".run/node$i.pid")" 2>/dev/null; then
    echo "node$i already running"; return
  fi
  nohup java -Xmx256m -jar "$JAR" \
    --server.port="$(port "$i")" \
    --kv.node-id="node$i" \
    --kv.cluster="$(cluster_spec)" \
    --kv.data-dir=data \
    > "logs/node$i.log" 2>&1 &
  echo $! > ".run/node$i.pid"
  echo "started node$i on http://localhost:$(port "$i")"
}

stop_node() {
  local i=$1 pidfile=".run/node$1.pid"
  [[ -f "$pidfile" ]] || { echo "node$i not running"; return; }
  local pid; pid="$(cat "$pidfile")"
  if [[ -r "/proc/$pid/winpid" ]]; then
    taskkill //F //PID "$(cat "/proc/$pid/winpid")" > /dev/null 2>&1 || true   # Git Bash on Windows
  else
    kill -9 "$pid" 2>/dev/null || true
  fi
  rm -f "$pidfile"
  echo "killed node$i"
}

wait_healthy() {
  for i in $(seq 1 "$NODES"); do
    for _ in $(seq 1 120); do
      curl -sf "http://localhost:$(port "$i")/actuator/health" > /dev/null && break
      sleep 0.5
    done
  done
}

case "${1:-}" in
  start)
    [[ -f "$JAR" ]] || ./mvnw -B -q -DskipTests package
    for i in $(seq 1 "$NODES"); do start_node "$i"; done
    wait_healthy
    echo "cluster ready - dashboard: http://localhost:8081"
    ;;
  stop)
    for i in $(seq 1 "$NODES"); do stop_node "$i"; done
    ;;
  start-node) start_node "$2" ;;
  stop-node) stop_node "$2" ;;
  status)
    for i in $(seq 1 "$NODES"); do
      if curl -sf "http://localhost:$(port "$i")/actuator/health" > /dev/null; then echo "node$i UP"; else echo "node$i DOWN"; fi
    done
    ;;
  *) echo "usage: $0 start|stop|start-node <n>|stop-node <n>|status" >&2; exit 1 ;;
esac
