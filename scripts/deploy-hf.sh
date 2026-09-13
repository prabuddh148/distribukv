#!/usr/bin/env bash
# Deploy the all-in-one image (deploy/huggingface) to a Hugging Face Docker Space.
#
#   scripts/deploy-hf.sh          token from $HF_TOKEN or from the file ~/.hf_token
#
# Creates the Space if needed, then pushes the sources; Hugging Face builds and runs the image.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
HF_USER="${HF_USER:-PrabuddhAg}"
SPACE="${HF_SPACE:-distribukv}"
TOKEN="${HF_TOKEN:-$(cat "$HOME/.hf_token" 2>/dev/null || true)}"
TOKEN="$(printf '%s' "$TOKEN" | tr -d '\r\n ')"
[[ -n "$TOKEN" ]] || { echo "No token: set HF_TOKEN or save it to ~/.hf_token" >&2; exit 1; }
redact() { sed "s/$TOKEN/***/g"; }

WHOAMI=$(curl -s -H "Authorization: Bearer $TOKEN" https://huggingface.co/api/whoami-v2)
grep -q "\"name\":\"$HF_USER\"" <<< "$WHOAMI" || { echo "Token is not valid for user $HF_USER" >&2; exit 1; }

echo "Ensuring Space $HF_USER/$SPACE exists"
curl -s -X POST https://huggingface.co/api/repos/create \
  -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -d "{\"type\":\"space\",\"name\":\"$SPACE\",\"sdk\":\"docker\",\"private\":false}" | redact | head -c 300; echo

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT
AUTH="Authorization: Basic $(printf '%s:%s' "$HF_USER" "$TOKEN" | base64 | tr -d '\n')"
git -c http.extraHeader="$AUTH" clone -q "https://huggingface.co/spaces/$HF_USER/$SPACE" "$WORK/space" 2>&1 | redact

cd "$WORK/space"
find . -mindepth 1 -maxdepth 1 ! -name .git -exec rm -rf {} +
mkdir -p deploy/huggingface
cp "$ROOT/pom.xml" .
cp -r "$ROOT/src" "$ROOT/docs" .
cp "$ROOT/deploy/huggingface/start.sh" deploy/huggingface/
cp "$ROOT/deploy/huggingface/Dockerfile" Dockerfile
cp "$ROOT/deploy/huggingface/README.md" README.md
printf '* text=auto eol=lf\n' > .gitattributes

git add -A
if git diff --cached --quiet; then
  echo "Space is already up to date"
else
  git -c user.name="$HF_USER" -c user.email="$HF_USER@users.noreply.huggingface.co" \
    commit -q -m "Deploy DistribuKV from $(git -C "$ROOT" rev-parse --short HEAD)"
  git -c http.extraHeader="$AUTH" push -q origin HEAD:main 2>&1 | redact
  echo "Pushed. Hugging Face is building the image."
fi
echo "Space: https://huggingface.co/spaces/$HF_USER/$SPACE"
echo "App:   https://$(tr '[:upper:]' '[:lower:]' <<< "$HF_USER")-$SPACE.hf.space"
