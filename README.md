# kilo-provider

Anonymous HTTP provider gateway in front of the Kilo upstream API
(`https://api.kilo.ai/api/gateway`). It exposes OpenAI-compatible inference
endpoints, filters the upstream model catalog to free models only, and routes
traffic through a configurable pool of outbound proxies with circuit breakers.

## Requirements

Two environment variables are **required**, with **no defaults anywhere**:

| Variable       | Meaning                                                                  |
| -------------- | ------------------------------------------------------------------------ |
| `PORT`         | Listening port; must be a valid integer in `1..=65535`.                   |
| `CONFIG_PATH`  | Path to the single TOML configuration file; must be readable.             |

Optional: `RUST_LOG`, `LOG_FORMAT=json`, `GRACEFUL_SHUTDOWN_TIMEOUT_SECS`.

## Configuration

Copy `config.example.toml` and point `CONFIG_PATH` at your copy. Full schema:

```toml
[proxy_pool]
reload_interval_secs = 1          # hot-reload polling interval
connect_timeout_secs = 10         # per-proxy connect timeout
max_concurrent_requests = 64      # admission control
request_body_timeout_secs = 30    # client body read timeout
response_header_timeout_secs = 30 # upstream headers timeout
stream_idle_timeout_secs = 60     # idle between SSE chunks
proxies = ["http://user:pass@host:port", ...]   # at least one, http:// only

[models]
refresh_interval_secs = 300       # periodic catalog refresh
stale_ttl_secs = 900             # snapshot validity window
max_entries = 2048               # cap on catalog size
recovery_initial_backoff_secs = 2 # fast recovery backoff
recovery_max_backoff_secs = 30

[circuit_breaker]
proxy_failure_cooldown_secs = 60
rate_limit_cooldown_secs = 120
model_not_found_cooldown_secs = 300
half_open_max_probes = 1
retry_after_max_secs = 300
```

Proxy pool must not be empty; there is no direct (non-proxy) fallback.

## HTTP endpoints

| Endpoint                  | Method | Purpose                                                        |
| ------------------------- | ------ | -------------------------------------------------------------- |
| `/healthz`                | GET    | Liveness — `{"status":"ok"}`                                   |
| `/readyz`                 | GET    | Readiness — ready once config loaded, proxy available, catalog ready |
| `/v1/models`              | GET    | OpenAI-compatible list of free models (`object: "list"`)         |
| `/v1/chat/completions`    | POST   | Inference forwarding                                           |
| `/v1/responses`           | POST   | Inference forwarding                                           |
| `/v1/embeddings`          | POST   | Inference forwarding                                           |
| `/v1/completions`         | POST   | Inference forwarding                                           |

Inference requires a `model` present in the current free catalog. Unknown
models are rejected with 400 before upstream. Anonymous access only: no
`Authorization` header is ever sent to Kilo.

## Logging

`tracing` + `tracing-subscriber`: `RUST_LOG` honored, defaults to `info`.
`LOG_FORMAT=json` selects JSON lines.

## Local development

```sh
cargo build --locked --release
CONFIG_PATH=config.example.toml PORT=8085 ./target/release/kilo-provider
```

Tests / lint / format:

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
```

## Local E2E

Requires a real `config.toml` (not committed). Run:

```sh
CONFIG_PATH=/path/to/config.toml PORT=18099 ./scripts/e2e-local.sh
```

Live E2E is not run in CI.

## Docker

```sh
docker build -t kilo-provider:verification .

docker run --rm -p 8085:8085 \
  -e PORT=8085 \
  -e CONFIG_PATH=/etc/kilo-provider/config.toml \
  -v ./config.toml:/etc/kilo-provider/config.toml:ro \
  kilo-provider:verification
```

Multi-stage build, CA certificates, non-root user (uid 10001). No config or
secrets are baked into the image.
