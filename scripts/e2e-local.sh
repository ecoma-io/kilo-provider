#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
CONFIG_PATH="${CONFIG_PATH:-$ROOT/config.toml}"
PORT="${PORT:-18099}"

if [[ ! -f "$CONFIG_PATH" ]]; then
  echo "ERROR: config.toml is required for live E2E. Set CONFIG_PATH or place config.toml at $ROOT/config.toml." >&2
  echo "Do NOT use config.example.toml as a fallback." >&2
  exit 1
fi

export PORT CONFIG_PATH
echo "Declared proxies: $(python3 -c 'import tomllib,sys; d=tomllib.load(open(sys.argv[1],"rb")); print(len(d["proxy_pool"]["proxies"]))' "$CONFIG_PATH")"

# Probe each proxy (redacted output only)
declared=0; ok=0; fail=0
while IFS= read -r p; do
  declared=$((declared+1))
  code=$(curl -sS -m 10 -o /dev/null -w "%{http_code}" --proxy "$p" https://api.kilo.ai/api/gateway/v1/models 2>/dev/null || echo "000")
  if [[ "$code" == "200" ]]; then ok=$((ok+1)); else fail=$((fail+1)); echo "proxy probe failed (http=$code)"; fi
done < <(python3 -c 'import tomllib,sys; d=tomllib.load(open(sys.argv[1],"rb")); [print(x) for x in d["proxy_pool"]["proxies"]]' "$CONFIG_PATH")
echo "Proxy probe summary: declared=$declared success=$ok failed=$fail"

echo "Building release binary..."
cargo build --locked --release --quiet

BIN="$ROOT/target/release/kilo-provider"
"$BIN" &
SRV_PID=$!
cleanup() { kill "$SRV_PID" 2>/dev/null || true; wait "$SRV_PID" 2>/dev/null || true; }
trap cleanup EXIT

ready=0
for i in $(seq 1 60); do
  if curl -sf "http://127.0.0.1:$PORT/healthz" >/dev/null 2>&1; then ready=1; break; fi
  sleep 0.5
done
if [[ "$ready" != "1" ]]; then echo "ERROR: server did not become ready" >&2; exit 1; fi

echo "healthz: $(curl -sS "http://127.0.0.1:$PORT/healthz")"
echo "readyz: $(curl -sS "http://127.0.0.1:$PORT/readyz")"

curl -sS -m 30 "http://127.0.0.1:$PORT/v1/models" -o /tmp/kilo-models.json
python3 -c 'import json,sys; d=json.load(open("/tmp/kilo-models.json"))["data"]; print("catalog models:",len(d)); sys.exit(0 if len(d)>0 else 1)'

MODEL="$(python3 -c 'import json; print(json.load(open("/tmp/kilo-models.json"))["data"][0]["id"])')"
echo "Using model: $MODEL"

curl -sS -m 60 -X POST "http://127.0.0.1:$PORT/v1/chat/completions" \
  -H 'content-type: application/json' \
  -d "{\"model\":\"$MODEL\",\"messages\":[{\"role\":\"user\",\"content\":\"hi\"}],\"max_tokens\":8,\"stream\":false}" \
  -o /tmp/kilo-chat.json
echo "chat response: $(head -c 200 /tmp/kilo-chat.json)"

# SSE attempt: capture a bounded prefix without failing the script when the pipe closes.
tmp_sse="$(mktemp)"
curl -sS -m 60 -N -X POST "http://127.0.0.1:$PORT/v1/chat/completions" \
  -H 'content-type: application/json' \
  -d "{\"model\":\"$MODEL\",\"messages\":[{\"role\":\"user\",\"content\":\"hi\"}],\"max_tokens\":8,\"stream\":true}" \
  > "$tmp_sse" 2>/dev/null || true
if grep -q '^data:' "$tmp_sse"; then
  echo "SSE: received streamed chunks (first 120 bytes):"
  head -c 120 "$tmp_sse"; echo
else
  echo "SSE: no streamed chunks captured (upstream may not support streaming for this model)" >&2
fi
rm -f "$tmp_sse"
echo "E2E completed."
