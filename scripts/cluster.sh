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
  local extra=()
  if [[ "${STACK:-}" == full ]]; then # Kafka on :9092, MySQL on :$MYSQL_PORT, Redis on :6379 (see scripts/local-infra.sh)
    extra+=(--kv.storage.type=mysql
            "--kv.storage.mysql-url=jdbc:mysql://127.0.0.1:${MYSQL_PORT:-3306}/?allowPublicKeyRetrieval=true&useSSL=false"
            --kv.storage.mysql-username=root "--kv.storage.mysql-password=${MYSQL_PASSWORD:-}"
            --kv.kafka.enabled=true --kv.kafka.bootstrap-servers=127.0.0.1:9092
            --kv.redis.enabled=true --kv.redis.url=redis://127.0.0.1:6379)
  fi
  if [[ "${PUBLIC_DEMO:-}" == true ]]; then
    extra+=(--kv.public-demo.enabled=true --kv.chaos-auto-heal-seconds=60
            "--kv.public-demo.allowed-origins=${DEMO_ORIGIN:-https://prabuddh148.github.io}")
  fi
  nohup java -Xmx200m -jar "$JAR" \
    --server.port="$(port "$i")" \
    --kv.node-id="node$i" \
    --kv.cluster="$(cluster_spec)" \
    --kv.data-dir=data \
    "${extra[@]}" \
    > "logs/node$i.log" 2>&1 &
  echo $! > ".run/node$i.pid"
  echo "started node$i on http://localhost:$(port "$i")"
}

stop_node() {
  local i=$1 pidfile=".run/node$1.pid"
  if command -v taskkill > /dev/null 2>&1; then
    # Git Bash on Windows: $! is a wrapper, so kill whatever JVM owns the node's port.
    local winpid
    winpid=$(netstat -ano | grep -E "[:.]$(port "$i") .*LISTENING" | awk '{print $NF}' | head -1)
    [[ -n "$winpid" ]] && taskkill //F //T //PID "$winpid" > /dev/null 2>&1 || true
  elif [[ -f "$pidfile" ]]; then
    kill -9 "$(cat "$pidfile")" 2>/dev/null || true
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
