#!/usr/bin/env bash
# Starts the whole DistribuKV stack in one container: Redis, MySQL, Kafka and 5 nodes.
# node1 serves the dashboard and API on $PORT (7860 on Hugging Face Spaces); the rest is internal.
set -euo pipefail

PORT="${PORT:-7860}"
DATA="${DATA_DIR:-/tmp/distribukv}"
LOGS="$DATA/logs"
export LOG_DIR="$LOGS" # Kafka scripts write their logs here (/opt/kafka is read-only for this user)
mkdir -p "$DATA"/mysql "$DATA"/kafka "$DATA"/redis "$DATA"/run "$DATA"/nodes "$LOGS"

log() { echo "[start] $*"; }
port_open() { (echo > "/dev/tcp/127.0.0.1/$1") 2>/dev/null; }
wait_port() {
  for _ in $(seq 1 120); do port_open "$1" && return 0; sleep 1; done
  echo "[start] $2 did not start on port $1" >&2
  tail -50 "$LOGS/$2.log" >&2 || true
  exit 1
}

log "starting Redis"
redis-server --port 6379 --bind 127.0.0.1 --save "" --appendonly no --dir "$DATA/redis" \
  --daemonize yes --logfile "$LOGS/redis.log"
wait_port 6379 redis

log "starting MySQL"
if [[ ! -d "$DATA/mysql/mysql" ]]; then
  mysqld --no-defaults --initialize-insecure --datadir="$DATA/mysql" --log-error="$LOGS/mysql-init.log"
fi
mysqld --no-defaults --datadir="$DATA/mysql" --socket="$DATA/run/mysqld.sock" --pid-file="$DATA/run/mysqld.pid" \
  --port=3306 --bind-address=127.0.0.1 --mysqlx=OFF --innodb-buffer-pool-size=128M \
  --log-error="$LOGS/mysql.log" &
wait_port 3306 mysql

log "starting Kafka"
sed -e "s#^log.dirs=.*#log.dirs=$DATA/kafka#" \
    -e "s/localhost/127.0.0.1/g" \
    -e "s#^listeners=PLAINTEXT://:9092,CONTROLLER://:9093#listeners=PLAINTEXT://127.0.0.1:9092,CONTROLLER://127.0.0.1:9093#" \
    /opt/kafka/config/kraft/server.properties > "$DATA/kafka.properties"
if [[ ! -f "$DATA/kafka/meta.properties" ]]; then
  /opt/kafka/bin/kafka-storage.sh format -t "$(/opt/kafka/bin/kafka-storage.sh random-uuid)" \
    -c "$DATA/kafka.properties" > "$LOGS/kafka-format.log" 2>&1
fi
KAFKA_HEAP_OPTS="-Xmx512m -Xms512m" /opt/kafka/bin/kafka-server-start.sh "$DATA/kafka.properties" > "$LOGS/kafka.log" 2>&1 &
wait_port 9092 kafka

CLUSTER="node1=http://127.0.0.1:$PORT"
for i in 2 3 4 5; do CLUSTER+=",node$i=http://127.0.0.1:$((8080 + i))"; done
node_port() { if [[ "$1" == 1 ]]; then echo "$PORT"; else echo $((8080 + $1)); fi; }

start_node() {
  local i=$1
  java -Xmx256m -XX:+UseSerialGC -jar /app/distribukv.jar \
    --server.port="$(node_port "$i")" \
    --kv.node-id="node$i" \
    --kv.cluster="$CLUSTER" \
    --kv.data-dir="$DATA/nodes" \
    --kv.storage.type=mysql \
    "--kv.storage.mysql-url=jdbc:mysql://127.0.0.1:3306/?allowPublicKeyRetrieval=true&useSSL=false" \
    --kv.storage.mysql-username=root \
    --kv.kafka.enabled=true --kv.kafka.bootstrap-servers=127.0.0.1:9092 \
    --kv.redis.enabled=true --kv.redis.url=redis://127.0.0.1:6379 \
    --kv.public-demo.enabled=true \
    --kv.public-demo.client-ip-header=X-Forwarded-For \
    "--kv.public-demo.allowed-origins=${ALLOWED_ORIGINS:-https://prabuddh148.github.io}" \
    --kv.chaos-auto-heal-seconds=60 \
    >> "$LOGS/node$i.log" 2>&1 &
  echo $! > "$DATA/run/node$i.pid"
}

log "starting 5 DistribuKV nodes"
for i in 1 2 3 4 5; do start_node "$i"; done
tail -n 0 -F "$LOGS/node1.log" &
log "stack is starting; dashboard on port $PORT"

# Supervise: restart a node that exits; stop the container if a backing service dies.
while true; do
  sleep 5
  for i in 1 2 3 4 5; do
    if ! kill -0 "$(cat "$DATA/run/node$i.pid")" 2>/dev/null; then
      log "node$i exited, restarting"
      start_node "$i"
    fi
  done
  for p in 3306 9092 6379; do
    port_open "$p" || { log "backing service on port $p is down"; exit 1; }
  done
done
