# ============================================================
# NexusDB — Multi-stage Docker Build
# ============================================================
# Build:  docker build -t nexusdb .
# Run:    docker run -d -p 5431:5431 -p 5432:5432 \
#           -v nexusdb-data:/var/lib/nexusdb/data nexusdb
# ============================================================

# Stage 1: Build
FROM rust:1.82-bookworm AS builder

WORKDIR /build

# Cache dependency build: copy manifests first, build deps, then copy source
COPY Cargo.toml Cargo.lock ./
COPY crates/ crates/
COPY proto/ proto/

RUN cargo build --release --bin nexusd \
    && strip target/release/nexusd

# Stage 2: Runtime
FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        libssl3 \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd -r nexusdb \
    && useradd -r -g nexusdb -d /var/lib/nexusdb -s /sbin/nologin nexusdb \
    && mkdir -p /var/lib/nexusdb/data /docker-entrypoint-initdb.d \
    && chown -R nexusdb:nexusdb /var/lib/nexusdb

COPY --from=builder /build/target/release/nexusd /usr/local/bin/nexusd
COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

VOLUME ["/var/lib/nexusdb/data"]

# PG wire protocol + gRPC
EXPOSE 5431 5432

USER nexusdb

HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 \
    CMD ["nexusd", "--version"] || exit 1

ENTRYPOINT ["docker-entrypoint.sh"]
CMD ["nexusd", "--data-dir", "/var/lib/nexusdb/data", "-H", "0.0.0.0"]
