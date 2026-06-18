ARG HAPROXY_VERSION=3.2

# ============================================================
# Rust builder stage (statically linked with musl)
# ============================================================
FROM rust:1.83-alpine AS rust-builder

RUN apk add --no-cache musl-dev openssl-dev openssl-libs-static

WORKDIR /build

COPY Cargo.toml Cargo.lock ./
COPY common ./common
COPY redis-sentinel ./redis-sentinel
COPY haproxy ./haproxy

RUN cargo build --release -p haproxy-entrypoint

FROM scratch AS rust-builder-local-output
COPY --from=rust-builder /build/target/release/haproxy-entrypoint /haproxy-entrypoint

FROM scratch AS rust-binaries
COPY --from=rust-builder-local-output / /

# ============================================================
# Final stage: HAProxy with Rust entrypoint
# ============================================================
FROM haproxy:${HAPROXY_VERSION}-alpine

USER root

# redis-cli for Railway's Database UI SSH tunnel
RUN apk add --no-cache redis bash

COPY --from=rust-binaries --chmod=755 /haproxy-entrypoint /entrypoint

RUN chown -R haproxy:haproxy /usr/local/etc/haproxy

USER haproxy

# 6379 = write port (primary only)
# 6380 = read port (any healthy node)
# 8404 = stats page
EXPOSE 6379 6380 8404

HEALTHCHECK --interval=10s --timeout=5s --start-period=15s --retries=3 \
    CMD wget -qO- http://127.0.0.1:8404/stats || exit 1

ENTRYPOINT ["/entrypoint"]
