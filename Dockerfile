# ---------------------------------------------------------------------------
# Builder: pinned stable Rust, bookworm-based slim image.
# Installs nothing beyond what the build needs (ca-certificates for crates.io
# HTTPS access via the registry).
# ---------------------------------------------------------------------------
FROM rust:1.98-slim AS builder

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy the manifests first for good layer caching; Cargo.lock must be present
# for the --locked build.
COPY Cargo.toml Cargo.lock ./
COPY src ./src

# Fully reproducible, lockfile-verified release build.
RUN cargo build --locked --release

# ---------------------------------------------------------------------------
# Runtime: minimal Debian runtime with only the CA root store installed.
# The image carries NO config file and NO baked-in port: `PORT` and
# `CONFIG_PATH` are provided strictly via the environment at runtime, and a
# missing/invalid `PORT` makes the binary exit non-zero with a clear error.
# ---------------------------------------------------------------------------
FROM debian:bookworm-slim AS runtime

# ca-certificates only: needed by the rustls/webpki TLS stack for HTTPS
# verification. Nothing else is installed.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Non-root runtime user with explicit uid/gid >= 10000 and no login shell.
RUN groupadd --gid 10001 app \
    && useradd --uid 10001 --gid 10001 --no-create-home \
        --shell /usr/sbin/nologin appuser

# Copy only the final binary out of the builder stage (no target/, no
# registry caches, no sources, no credentials).
COPY --from=builder /app/target/release/kilo-provider /usr/local/bin/kilo-provider

USER 10001:10001

# No ENV PORT / no EXPOSE on purpose: the port is strictly runtime-provided.
ENTRYPOINT ["/usr/local/bin/kilo-provider"]
