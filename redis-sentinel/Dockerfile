ARG REDIS_VERSION=7

# ============================================================
# Rust builder stage
# ============================================================
FROM rust:1.90-alpine AS rust-builder

RUN apk add --no-cache musl-dev openssl-dev openssl-libs-static

WORKDIR /build

COPY Cargo.toml Cargo.lock ./
COPY common ./common
COPY redis-sentinel ./redis-sentinel
COPY haproxy ./haproxy

RUN cargo build --release -p redis-sentinel

FROM scratch AS rust-builder-local-output
COPY --from=rust-builder /build/target/release/redis-wrapper /redis-wrapper

FROM scratch AS rust-binaries
COPY --from=rust-builder-local-output / /

# ============================================================
# Final stage: Redis with colocated Sentinel
# ============================================================
FROM redis:${REDIS_VERSION}-bookworm

USER root

# redis-cli is bundled with the redis image; no extra packages needed.
# Install bash for Railway's Database UI SSH tunnel compatibility, and wget for
# the HEALTHCHECK below (this base image, unlike the haproxy alpine one, doesn't
# bundle it).
RUN apt-get update && apt-get install -y --no-install-recommends bash wget && rm -rf /var/lib/apt/lists/*

# Copy the Rust entrypoint
COPY --from=rust-binaries --chmod=755 /redis-wrapper /usr/local/bin/redis-wrapper

RUN mkdir -p /data

# TODO(security): runs as root. Railway's runtime-mounted volume at /data has its
# own ownership, independent of a build-time `chown`, so a non-root USER here
# was unable to write redis.conf/sentinel.conf to the actual mounted volume.
# The proper fix — matching upstream redis's own docker-entrypoint.sh — is to
# start as root, chown /data to a non-root user at runtime, then drop privileges
# (e.g. via gosu) before exec'ing redis-server/sentinel. Deferred for now.

VOLUME ["/data"]

EXPOSE 6379 26379 8080

HEALTHCHECK --interval=5s --timeout=3s --start-period=30s --retries=3 \
    CMD wget -qO- http://127.0.0.1:8080/health || exit 1

ENTRYPOINT ["redis-wrapper"]
