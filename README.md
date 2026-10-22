# kilo-provider

An anonymous HTTP provider gateway in front of the Kilo upstream API
(`https://api.kilo.ai/api/gateway`) — **bootstrap scaffold**: the plumbing
(axum HTTP server, strict env-var startup, single TOML config file, upstream
client, proxy-pool / catalog / circuit-breaker placeholders) is in place;
business behavior lands in follow-up PRs.

## Requirements

Two environment variables are **required**, with **no defaults anywhere**:

| Variable       | Meaning                                                                  |
| -------------- | ------------------------------------------------------------------------ |
| `PORT`         | Listening port; must be a valid integer in `1..=65535`.                   |
| `CONFIG_PATH`  | Path to the single TOML configuration file; must be readable.             |

If either is missing or invalid, the process prints a clear error to stderr
and exits with a non-zero status **before binding any listener**.

The listening port and the upstream location are **not** configurable in the
config file: the upstream base URL is a fixed code constant and access is
anonymous (no `Authorization` header is ever sent, and the HTTP client
ignores system/env proxy settings via `.no_proxy()`).

## Configuration

See [`config.example.toml`](config.example.toml) — copy it and point
`CONFIG_PATH` at your copy. It must contain a `[proxy_pool]` section with a
required `proxies` key (the list may be empty):

```toml
[proxy_pool]
proxies = []
```

Placeholder-only entries look like:

```toml
[proxy_pool]
proxies = [
  "http://proxy-a.example.internal:8080",
]
```

Unknown fields and missing required keys are rejected at startup with a
descriptive error.

## HTTP endpoints

| Endpoint      | Method | Purpose                                                        |
| ------------- | ------ | -------------------------------------------------------------- |
| `/healthz`    | GET    | Liveness — `{"status":"ok"}`                                    |
| `/readyz`     | GET    | Readiness — ready once config is loaded and clients are built.  |
| anything else | GET/…  | JSON 404 fallback `{"error": "..."}`                            |

## Logging

`tracing` + `tracing-subscriber`:

* `RUST_LOG` is honored when set; it defaults to `info`.
* `LOG_FORMAT=json` selects JSON-formatted log lines; anything else uses the
  default formatter.

Startup logs report the listening port, the config path (never the file
contents), and the initial placeholder counts.

## Local development

```sh
cargo build --locked --release
CONFIG_PATH=config.example.toml PORT=8085 ./target/release/kilo-provider

# tests / lint / format
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
cargo test
```

## Docker

```sh
docker build -t kilo-provider:bootstrap .

docker run --rm -p 8085:8085 \
  -e PORT=8085 \
  -e CONFIG_PATH=/etc/kilo-provider/config.example.toml \
  -v ./config.example.toml:/etc/kilo-provider/config.example.toml:ro \
  kilo-provider:bootstrap

curl http://127.0.0.1:8085/healthz   # {"status":"ok"}
curl http://127.0.0.1:8085/readyz    # {"status":"ready", ...}

# Missing PORT → fast, non-zero exit with a clear error:
docker run --rm \
  -e CONFIG_PATH=/etc/kilo-provider/config.example.toml \
  -v ./config.example.toml:/etc/kilo-provider/config.example.toml:ro \
  kilo-provider:bootstrap
```

The image is a multi-stage build (pinned `rust:1.98-slim` builder →
`debian:bookworm-slim` runtime) containing only the release binary and
`ca-certificates`; it runs as non-root (uid/gid 10001), bakes in no config
file, sets no default `PORT`, and exposes nothing.

## Bootstrap scope

Intentionally unimplemented placeholders (marked `TODO(bootstrap)` in code):

* **proxy_pool** — entries are loaded/validated, but rotation and hot reload
  are not implemented.
* **model catalog** — in-memory cache shape only; no upstream listing or
  filtering.
* **circuit breaker** — `Closed/Open/HalfOpen` states exist, `allow()`
  returns `true`; transitions are not implemented.
* **upstream client** — builds anonymous requests against the fixed base
  URL; no requests are made at startup.
