# Redis High Availability Template for Railway

Self-healing Redis HA cluster with colocated Sentinel and automatic failover. Designed for Railway's single-click HA template.

## Features

- **3-node Redis cluster** with streaming replication
- **Automatic failover** in ~5–30 seconds via Sentinel majority vote
- **Colocated Sentinel** — no separate coordinator nodes
- **HAProxy entry point** with separate write and read endpoints
- **Hostname-based peer tracking** — survives Railway IP changes on redeploy
- **Split-brain write bound** — isolated master stops accepting writes when it loses quorum
- **AOF persistence** — prevents data wipe on master auto-restart

## Architecture

```
Application
    ↓
Redis HA (HAProxy)
    ├─ :6379 (write)  → Current master only
    └─ :6380 (read)   → Any healthy node (round-robin)
    ↓
Redis Cluster
    ├─ Redis-1 (master)  ← Writes + Reads
    ├─ Redis-2 (replica) ← Reads + Failover ready
    └─ Redis-3 (replica) ← Reads + Failover ready
```

Each Redis node runs a colocated Sentinel on port 26379. HAProxy probes each node's `/role` health endpoint and routes writes only to the node reporting `role:master`.

## Services

| Service | Image | Role |
|---|---|---|
| Redis-1 | `redis-sentinel` | Root — initial master |
| Redis-2 | `redis-sentinel` | Replica |
| Redis-3 | `redis-sentinel` | Replica |
| Redis HA | `haproxy` | Edge — client entry point |

**Minimum 3 Redis nodes**: Sentinel requires a majority to authorize failover. A 2-node cluster split-brains on a symmetric partition; 3 nodes (quorum=2) tolerate 1 node loss.

## Connecting

Use `REDIS_URL` (writes) or `REDIS_PUBLIC_URL` (public TCP) from the **Redis HA** (HAProxy) service. Do not connect directly to individual Redis nodes.

## Scaling

Scale from 2–5 replicas via the cluster overview. Sentinel uses gossip to discover new peers — the initial `SENTINEL_HOSTS` list bootstraps the cluster; scale-up nodes join automatically.

## Images

| Image | GHCR tag | Base |
|---|---|---|
| `redis-sentinel` | `ghcr.io/railwayapp-templates/redis-ha/redis-sentinel:8-bookworm` (latest) | `redis:8-bookworm` |
| `redis-sentinel` | `ghcr.io/railwayapp-templates/redis-ha/redis-sentinel:7-bookworm` | `redis:7-bookworm` |
| `haproxy` | `ghcr.io/railwayapp-templates/redis-ha/haproxy:3.2-alpine` | `haproxy:3.2-alpine` |

Both images are thin wrappers on official upstream images. The Rust entrypoints handle config rendering, process management, and health serving.

### `redis-sentinel` (`redis-wrapper`)

- Renders `redis.conf` and `sentinel.conf` from env vars at startup
- Manages `redis-server` + `redis-sentinel` as supervised child processes
- Serves `/health` (liveness) and `/role` (master check) on `HEALTH_PORT` (default 8080)

### `haproxy` (`haproxy-entrypoint`)

- Renders `haproxy.cfg` from `REDIS_NODES` env var at startup
- Routes `:6379` writes to the backend reporting `role:master`
- Routes `:6380` reads to all healthy backends (round-robin)
- Exposes HAProxy stats at `:8404/stats`

## Environment variables

Key variables on the Redis nodes (set on Redis-1, referenced by replicas):

| Variable | Default | Purpose |
|---|---|---|
| `REDIS_PASSWORD` | `${{secret(64)}}` | Auth — applied to requirepass, masterauth, sentinel auth-pass |
| `REDIS_MASTER_NAME` | `mymaster` | Sentinel master set name |
| `SENTINEL_QUORUM` | `2` | Votes needed to elect a new master |
| `SENTINEL_DOWN_AFTER_MS` | `5000` | MS before a node is considered down |
| `SENTINEL_FAILOVER_TIMEOUT_MS` | `30000` | Failover abort timeout |
| `REDIS_MIN_REPLICAS_TO_WRITE` | `1` | Master disables writes when fewer healthy replicas |
| `REDIS_MIN_REPLICAS_MAX_LAG` | `10` | Replica lag threshold (seconds) |
| `REDIS_APPENDONLY` | `yes` | AOF persistence (required — see notes) |

## Development

### Prerequisites

- Rust (stable)
- Docker + Docker Buildx

### Build locally

```bash
# Build redis-sentinel
docker build -f redis-sentinel/Dockerfile -t redis-sentinel:local .

# Build haproxy
docker build -f haproxy/Dockerfile -t redis-ha-haproxy:local .
```

### Publish

CI publishes on every push to `main` that touches a component. To add a new Redis major version, add it to the `redis_major` matrix and update `LATEST_REDIS_MAJOR` in `.github/workflows/build-and-push.yml`. To bump HAProxy, update `HAPROXY_VERSION`.
