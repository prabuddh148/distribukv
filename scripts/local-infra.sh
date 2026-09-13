#!/usr/bin/env bash
# Kafka, MySQL and a Redis-compatible server (Microsoft Garnet) running natively on Windows,
# without Docker, for the full-stack local cluster and the public demo. Run from Git Bash.
# Binaries and data live in ~/.distribukv-infra.
#
#   scripts/local-infra.sh install    download + initialise everything (once)
#   scripts/local-infra.sh start      Kafka :9092, MySQL :$MYSQL_PORT (default 3316), Redis :6379
#   scripts/local-infra.sh stop
#   scripts/local-infra.sh status
#
# Then: STACK=full MYSQL_PORT=3316 scripts/cluster.sh start
set -euo pipefail

INFRA="${INFRA:-$HOME/.distribukv-infra}"
MYSQL_PORT="${MYSQL_PORT:-3316}"
KAFKA_VERSION=3.9.1
MYSQL_VERSION=8.4.11
GARNET_VERSION=2.1.7
KAFKA_DIR="$INFRA/kafka_2.13-$KAFKA_VERSION"
MYSQL_DIR="$INFRA/mysql-$MYSQL_VERSION-winx64"

win() { cygpath -m "$1"; }
listening() { netstat -ano | grep -qE "127\.0\.0\.1:$1 .*LISTENING"; }
kill_port() {
  local pid
  pid=$(netstat -ano | grep -E "127\.0\.0\.1:$1 .*LISTENING" | awk '{print $NF}' | head -1)
  [[ -n "$pid" ]] && taskkill //F //T //PID "$pid" > /dev/null 2>&1 || true
}
wait_port() {
  for _ in $(seq 1 60); do listening "$1" && return 0; sleep 1; done
  echo "$2 did not start on port $1 (see $INFRA/logs)" >&2; return 1
}
fetch() { [[ -f "$INFRA/$1" ]] || curl -sSL -o "$INFRA/$1" "$2"; }

install() {
  mkdir -p "$INFRA/logs"
  fetch kafka.tgz "https://archive.apache.org/dist/kafka/$KAFKA_VERSION/kafka_2.13-$KAFKA_VERSION.tgz"
  fetch mysql.zip "https://cdn.mysql.com/Downloads/MySQL-8.4/mysql-$MYSQL_VERSION-winx64.zip"
  fetch garnet.zip "https://github.com/microsoft/garnet/releases/download/v$GARNET_VERSION/win-x64-based-readytorun.zip"
  fetch cloudflared.exe "https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-windows-amd64.exe"
  cd "$INFRA"
  [[ -d "$KAFKA_DIR" ]] || tar -xzf kafka.tgz
  [[ -d "$MYSQL_DIR" ]] || unzip -q mysql.zip
  [[ -d garnet ]] || unzip -q garnet.zip -d garnet
  [[ -d dotnet ]] || powershell -NoProfile -ExecutionPolicy Bypass -Command \
    "iwr https://dot.net/v1/dotnet-install.ps1 -OutFile \$env:TEMP\\dotnet-install.ps1 -UseBasicParsing; & \$env:TEMP\\dotnet-install.ps1 -Runtime dotnet -Channel 8.0 -InstallDir '$(win "$INFRA/dotnet")' -NoPath"

  if [[ ! -d mysql-data ]]; then
    "$MYSQL_DIR/bin/mysqld.exe" --initialize-insecure --basedir="$(win "$MYSQL_DIR")" --datadir="$(win "$INFRA/mysql-data")"
  fi

  if [[ ! -f kafka-data/meta.properties ]]; then
    sed -e "s#^log.dirs=.*#log.dirs=$(win "$INFRA/kafka-data")#" \
        -e "s/localhost/127.0.0.1/g" \
        -e "s#^listeners=PLAINTEXT://:9092,CONTROLLER://:9093#listeners=PLAINTEXT://127.0.0.1:9092,CONTROLLER://127.0.0.1:9093#" \
        "$KAFKA_DIR/config/kraft/server.properties" > kafka-server.properties
    # Kafka's .bat launchers break on long classpaths, so call the JVM directly.
    local id
    id=$(cd "$KAFKA_DIR" && java -cp "libs/*" kafka.tools.StorageTool random-uuid 2>/dev/null | tr -d '\r' | tail -1)
    (cd "$KAFKA_DIR" && java -cp "libs/*" -Dlog4j.configuration=file:config/tools-log4j.properties \
      kafka.tools.StorageTool format -t "$id" -c "$(win "$INFRA/kafka-server.properties")")
  fi
  echo "installed into $INFRA"
}

start() {
  mkdir -p "$INFRA/logs"
  cd "$INFRA"
  if ! listening 9092; then
    (cd "$KAFKA_DIR" && nohup java -Xmx512m -Xms512m -cp "libs/*" \
      -Dlog4j.configuration=file:config/log4j.properties -Dkafka.logs.dir="$(win "$INFRA/logs")" \
      kafka.Kafka "$(win "$INFRA/kafka-server.properties")" > "$INFRA/logs/kafka-stdout.log" 2>&1 &)
  fi
  if ! listening "$MYSQL_PORT"; then
    nohup "$MYSQL_DIR/bin/mysqld.exe" --basedir="$(win "$MYSQL_DIR")" --datadir="$(win "$INFRA/mysql-data")" \
      --port="$MYSQL_PORT" --mysqlx=OFF --bind-address=127.0.0.1 --innodb-buffer-pool-size=128M --console \
      > "$INFRA/logs/mysql.log" 2>&1 &
  fi
  if ! listening 6379; then
    DOTNET_ROOT="$(cygpath -w "$INFRA/dotnet")" nohup ./garnet/net8.0/GarnetServer.exe --bind 127.0.0.1 --port 6379 \
      > "$INFRA/logs/garnet.log" 2>&1 &
  fi
  wait_port 9092 Kafka && wait_port "$MYSQL_PORT" MySQL && wait_port 6379 Garnet
  status
}

stop() {
  "$MYSQL_DIR/bin/mysqladmin.exe" -h127.0.0.1 -P"$MYSQL_PORT" -uroot shutdown 2>/dev/null || kill_port "$MYSQL_PORT"
  kill_port 9092
  kill_port 6379
  echo "stopped"
}

status() {
  for svc in "Kafka 9092" "MySQL $MYSQL_PORT" "Redis(Garnet) 6379"; do
    set -- $svc
    if listening "$2"; then echo "$1 UP on 127.0.0.1:$2"; else echo "$1 DOWN"; fi
  done
}

case "${1:-}" in
  install) install ;;
  start) start ;;
  stop) stop ;;
  status) status ;;
  *) echo "usage: $0 install|start|stop|status" >&2; exit 1 ;;
esac
