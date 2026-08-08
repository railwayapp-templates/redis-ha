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
| `redis-sentinel` | `ghcr.io/railwayapp-templates/redis-ha/redis-sentinel:8-bookworm` | `redis:8-bookworm` |
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
| `SENTINEL_QUORUM` | `2` | Seed for the odown quorum on first boot. At runtime each node keeps its own Sentinel's quorum at a strict majority of the Sentinels it actually knows (see Self-healing), so this only matters until gossip discovery settles |
| `SENTINEL_DOWN_AFTER_MS` | `5000` | MS before a node is considered down |
| `SENTINEL_FAILOVER_TIMEOUT_MS` | `30000` | Failover abort timeout |
| `REDIS_MIN_REPLICAS_TO_WRITE` | `1` | Master disables writes when fewer healthy replicas |
| `REDIS_MIN_REPLICAS_MAX_LAG` | `10` | Replica lag threshold (seconds) |
| `REDIS_APPENDONLY` | `yes` | AOF persistence (required — see notes) |
| `BOOT_ROLE_FROM_SENTINEL_STATE` | `true` | Take the boot role from Sentinel's own `sentinel.conf` instead of `REPLICA_OF`. Set to `false` to pin every boot to the deploy-time topology |
| `BOOT_ROLE_FROM_PEER_SENTINELS` | `true` | On a first boot (no local Sentinel state), ask the peer Sentinels in `SENTINEL_HOSTS` who the master currently is before trusting `REPLICA_OF`. Set to `false` to disable the query |
| `QUORUM_SYNC_DISABLED` | unset | Set to `1` to stop the watcher that keeps the local Sentinel's quorum at a majority of the known Sentinels |
| `LINK_HEAL_DISABLED` | unset | Set to `1` to stop the replication-link self-heal watcher |

### Boot role

`REPLICA_OF` describes the topology at *deploy* time, so regenerating `redis.conf` from it alone re-imposes that topology on every restart — a promoted node would demote itself back onto the node it was promoted over, and a cold restart would recreate the pre-failover cluster wholesale.

Sentinel already records the current master on the same volume: it owns `sentinel.conf` after first boot and rewrites its `sentinel monitor` line after every failover. Each node reads that line at startup and boots into the role it names, falling back to `REPLICA_OF` when there is no readable local state (first boot, fresh volume). The decision is logged as a single `boot role:` line, which calls out an override of `REPLICA_OF` explicitly.

A node that was down for the whole failover never saw the switch, so its `sentinel.conf` still names itself and it comes back as a master — Sentinel demotes it within one failover timeout, as it does today.

A node with no local state at all — a scale-up addition, a replaced volume — asks the peer Sentinels in `SENTINEL_HOSTS` who the master currently is (`BOOT_ROLE_FROM_PEER_SENTINELS`) before falling back to `REPLICA_OF`. The env topology names whoever was master when the template was stamped; a cluster that failed over since would otherwise receive the new node as an invisible chained sub-replica of a demoted ex-master. The answer is only ever used as a replication target: a peer answer naming the booting node itself (the incumbent master coming back on a wiped volume) is refused, because self-promoting an empty dataset would make every data-bearing replica full-sync from it.

## Self-healing

Beyond boot-time role resolution, two watchers run on every Sentinel-managed node:

- **link-heal** repoints a replica whose replication link is durably broken (`REPLICAOF` reissued at Sentinel's answer), completes a promotion whose `REPLICAOF NO ONE` never landed, and — the case Sentinel structurally cannot fix — repoints a replica attached to the *wrong* master over a healthy link. Sentinel only learns replicas from the master's `INFO`, so a node chained behind a demoted ex-master is invisible to it forever; the watcher compares the replica's own attachment against `SENTINEL get-master-addr-by-name` and acts once the disagreement outlives `LINK_HEAL_WRONG_MASTER_DWELL_SECONDS` (default 300).
- **quorum-sync** keeps the local Sentinel's odown quorum at a strict majority of the Sentinels it actually gossips with (peers flagged `s_down` don't count, so scale-downs shrink it back). `sentinel.conf` otherwise freezes the first-boot quorum forever — after a 3→5 scale-up the original nodes would keep quorum 2 while the new ones write 3. Quorum only gates odown; failover authorization always needs a majority of all known Sentinels, so a transiently low value cannot enable a unilateral failover.

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

CI publishes on every push to `main` that touches a component. No image carries a floating `:latest` tag — every published tag is pinned to a Redis/HAProxy version, distro, or commit SHA, so a pull always names an exact build. To add a new Redis major version, add it to the `redis_major` matrix in `.github/workflows/build-and-push.yml`. To bump HAProxy, update `HAPROXY_VERSION`.
