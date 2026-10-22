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

# Load proxies redacted count
declared=$(python3 -c 'import tomllib,sys; d=tomllib.load(open(sys.argv[1],"rb")); print(len(d["proxy_pool"]["proxies"]))' "$CONFIG_PATH")
echo "Declared proxies: $declared"

export PORT CONFIG_PATH
echo "Declared proxies: $declared"

# Probe each proxy with bounded concurrency, redacted output
probe_one() {
  local p="$1"
  code=$(curl -sS -m 10 -o /dev/null -w "%{http_code}" --proxy "$p" https://api.kilo.ai/api/gateway/models 2>/dev/null || echo "000")
  if [[ "$code" == "200" ]]; then echo OK; else echo "FAIL:$code"; fi
}
export -f probe_one

ok=0; fail=0
while IFS= read -r p; do
  res=$(probe_one "$p")
  if [[ "$res" == "OK" ]]; then ok=$((ok+1)); else fail=$((fail+1)); echo "proxy probe failed (${res})"; fi
done < <(python3 -c 'import tomllib,sys; d=tomllib.load(open(sys.argv[1],"rb")); [print(x) for x in d["proxy_pool"]["proxies"]]' "$CONFIG_PATH")
echo "Proxy probe summary: declared=$declared success=$ok failed=$fail"
if [[ "$ok" -eq 0 ]]; then
  echo "ERROR: no usable proxy" >&2
  exit 1
fi

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
readyz_code=$(curl -sS -o /tmp/readyz.json -w "%{http_code}" "http://127.0.0.1:$PORT/readyz")
echo "readyz HTTP $readyz_code: $(cat /tmp/readyz.json)"
if [[ "$readyz_code" != "200" ]]; then echo "ERROR: readyz not ready" >&2; exit 1; fi

models_code=$(curl -sS -o /tmp/kilo-models.json -w "%{http_code}" -m 30 "http://127.0.0.1:$PORT/v1/models")
echo "models HTTP $models_code"
if [[ "$models_code" != "200" ]]; then echo "ERROR: models failed" >&2; exit 1; fi
python3 -c 'import json,sys; d=json.load(open("/tmp/kilo-models.json")); assert d.get("object")=="list"; data=d["data"]; assert len(data)>0; print("catalog models:",len(data)); sys.exit(0)'

MODEL="$(python3 -c 'import json; print(json.load(open("/tmp/kilo-models.json"))["data"][0]["id"])')"
echo "Using model: $MODEL"

# Rejected model before upstream
rej_code=$(curl -sS -o /tmp/rej.json -w "%{http_code}" -m 30 -X POST "http://127.0.0.1:$PORT/v1/chat/completions" -H 'content-type: application/json' -d '{"model":"not-in-catalog","messages":[{"role":"user","content":"hi"}],"max_tokens":4}')
echo "rejected-model HTTP $rej_code"
if [[ "$rej_code" != "400" ]]; then echo "ERROR: unknown model not rejected" >&2; exit 1; fi

# Inference
chat_code=$(curl -sS -o /tmp/kilo-chat.json -w "%{http_code}" -m 60 -X POST "http://127.0.0.1:$PORT/v1/chat/completions" -H 'content-type: application/json' -d "{\"model\":\"$MODEL\",\"messages\":[{\"role\":\"user\",\"content\":\"hi\"}],\"max_tokens\":8,\"stream\":false}")
echo "chat HTTP $chat_code"
if [[ "$chat_code" != "200" ]]; then echo "ERROR: chat failed: $(head -c 200 /tmp/kilo-chat.json)" >&2; exit 1; fi
python3 -c 'import json; d=json.load(open("/tmp/kilo-chat.json")); assert "choices" in d; print("chat ok")'

# SSE
tmp_sse="$(mktemp)"
sse_code=$(curl -sS -m 60 -N -o "$tmp_sse" -w "%{http_code}" -X POST "http://127.0.0.1:$PORT/v1/chat/completions" -H 'content-type: application/json' -d "{\"model\":\"$MODEL\",\"messages\":[{\"role\":\"user\",\"content\":\"hi\"}],\"max_tokens\":8,\"stream\":true}")
echo "SSE HTTP $sse_code"
if [[ "$sse_code" != "200" ]]; then echo "ERROR: SSE http $sse_code" >&2; exit 1; fi
if grep -q '^data:' "$tmp_sse"; then
  echo "SSE: received streamed chunks"
else
  echo "ERROR: SSE no chunks" >&2; exit 1
fi
rm -f "$tmp_sse"
echo "E2E completed."
