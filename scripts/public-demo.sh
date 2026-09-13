#!/usr/bin/env bash
# Expose the local dashboard (node1, port 8081) on a public https URL through a Cloudflare quick
# tunnel. No Cloudflare account is needed; the URL is random and lives as long as this process.
#
#   PUBLIC_DEMO=true scripts/cluster.sh start     # nodes with public guard rails (rate limit, auto-heal)
#   scripts/public-demo.sh start                  # prints the public URL
#   scripts/public-demo.sh stop
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
CLOUDFLARED="${CLOUDFLARED:-$HOME/.distribukv-infra/cloudflared.exe}"
command -v "$CLOUDFLARED" > /dev/null 2>&1 || CLOUDFLARED=cloudflared
# The static project page (https://prabuddh148.github.io/distribukv/) reads the current tunnel URL from this gist.
DEMO_GIST_ID="${DEMO_GIST_ID:-fe973feaf3679bb03996fd0fbb399bee}"
mkdir -p .run logs

publish() { # publish <url>  |  publish ""  (offline)
  command -v gh > /dev/null 2>&1 || { echo "gh CLI not found: project page not updated"; return 0; }
  local now body
  now=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  if [[ -n "$1" ]]; then
    body="{\"status\":\"online\",\"url\":\"$1\",\"updatedAt\":\"$now\"}"
  else
    body="{\"status\":\"offline\",\"url\":null,\"updatedAt\":\"$now\"}"
  fi
  if gh api -X PATCH "gists/$DEMO_GIST_ID" -f "files[distribukv-live.json][content]=$body" > /dev/null 2>&1; then
    echo "project page updated: ${1:-offline}"
  else
    echo "could not update the project page gist ($DEMO_GIST_ID)"
  fi
}

case "${1:-}" in
  start)
    : > logs/tunnel.log
    nohup "$CLOUDFLARED" tunnel --no-autoupdate --url http://localhost:8081 > logs/tunnel.log 2>&1 &
    echo $! > .run/tunnel.pid
    for _ in $(seq 1 60); do
      URL=$(grep -o 'https://[a-z0-9-]*\.trycloudflare\.com' logs/tunnel.log | head -1 || true)
      if [[ -n "$URL" ]]; then
        echo "$URL" > .run/public-url
        echo "Public demo: $URL"
        publish "$URL"
        exit 0
      fi
      sleep 1
    done
    echo "tunnel did not start, see logs/tunnel.log" >&2
    exit 1
    ;;
  stop)
    if command -v taskkill > /dev/null 2>&1; then
      taskkill //F //IM cloudflared.exe > /dev/null 2>&1 || true
    elif [[ -f .run/tunnel.pid ]]; then
      kill "$(cat .run/tunnel.pid)" 2>/dev/null || true
    fi
    rm -f .run/tunnel.pid .run/public-url
    echo "tunnel stopped"
    publish ""
    ;;
  url) cat .run/public-url 2>/dev/null || echo "no tunnel running" ;;
  *) echo "usage: $0 start|stop|url" >&2; exit 1 ;;
esac
