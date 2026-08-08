#!/usr/bin/env bash
# test/e2e.sh — end-to-end harness for the redis-sentinel node image.
#
# Modeled on postgres-ssl's test/e2e.sh and postgres-ha's test/e2e-ha.sh:
# each assertion is a `t_*` function with its own volumes; containers carry a
# label so the exit trap can clean up whatever a failed run leaves behind.
# Final exit code is the count of failed tests.
#
# Run: ./test/e2e.sh
# Or:  ./test/e2e.sh t_rdb_adoption t_sentinel_failover   # subset
#
# The scenarios encode the conversion behaviors this image guarantees:
# adopting an RDB-only dataset (a standalone Railway redis being converted to
# HA), surviving the CONFIG SET → manifest-commit crash window, following the
# volume mount path, and Sentinel failover preserving adopted data.

set -uo pipefail

IMAGE="${IMAGE:-redis-sentinel-e2e:local}"
SEED_IMAGE="redis:8.2.1"
NET="redis-ha-test-net"
LABEL="redis-ha-e2e=1"
PW="e2e-password"

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"

PASS=0
FAIL=0
FAILED_TESTS=()

# ----- log helpers -----------------------------------------------------------
if [ -t 1 ]; then
  R=$'\033[31m'; G=$'\033[32m'; Y=$'\033[33m'; B=$'\033[36m'; N=$'\033[0m'
else
  R=""; G=""; Y=""; B=""; N=""
fi
log()  { echo "${B}==>${N} $*"; }
ok()   { echo "${G}PASS${N} $1"; PASS=$((PASS+1)); }
ko()   { echo "${R}FAIL${N} $1: $2"; FAIL=$((FAIL+1)); FAILED_TESTS+=("$1"); fail_dump "$1" "${@:3}"; }
note() { echo "  ${Y}note:${N} $*"; }

fail_dump() {
  local label="$1"; shift
  for c in "$@"; do
    if docker ps -a --format '{{.Names}}' | grep -q "^${c}$"; then
      local cstate
      cstate=$(docker inspect -f 'status={{.State.Status}} exit={{.State.ExitCode}}' "$c" 2>/dev/null)
      echo "${R}--- docker logs ${c} (${cstate}) (last 40) ---${N}" >&2
      docker logs --tail 40 "$c" 2>&1 | sed 's/^/    /' >&2
    fi
  done
}

# ----- infra -----------------------------------------------------------------
cleanup_test_resources() {
  docker ps -aq --filter "label=${LABEL}" | xargs -r docker rm -f >/dev/null 2>&1
  docker volume ls -q --filter "label=${LABEL}" | xargs -r docker volume rm -f >/dev/null 2>&1
}
trap 'cleanup_test_resources; docker network rm "$NET" >/dev/null 2>&1 || true' EXIT

setup() {
  docker network inspect "$NET" >/dev/null 2>&1 || docker network create "$NET" >/dev/null
  if ! docker image inspect "$IMAGE" >/dev/null 2>&1; then
    log "building ${IMAGE}"
    docker build -q -f "${REPO_ROOT}/redis-sentinel/Dockerfile" \
      --build-arg REDIS_VERSION=8 -t "$IMAGE" "$REPO_ROOT" >/dev/null || {
      echo "${R}image build failed${N}" >&2; exit 1
    }
  fi
  docker image inspect "$SEED_IMAGE" >/dev/null 2>&1 || docker pull -q "$SEED_IMAGE" >/dev/null
}

mkvol() { # mkvol NAME
  # `docker volume rm -f` fails SILENTLY while a container holds the volume,
  # and `docker volume create` is idempotent — together they can hand a test a
  # stale volume from an earlier run. Evict holders first, and abort loudly if
  # the volume still can't be recreated fresh.
  docker ps -aq --filter "volume=$1" | xargs -r docker rm -f >/dev/null 2>&1
  docker volume rm -f "$1" >/dev/null 2>&1
  if docker volume inspect "$1" >/dev/null 2>&1; then
    echo "${R}mkvol: stale volume $1 could not be removed${N}" >&2
    exit 1
  fi
  docker volume create --label "$LABEL" "$1" >/dev/null
}

# Seed a volume the way Railway's standalone redis template persists: RDB
# only (`--save 60 1`), no AOF. Extra args are key/value pairs to SET.
seed_rdb_volume() { # seed_rdb_volume VOLUME MOUNT KEY VALUE [redis-server extra args...]
  local vol="$1" mount="$2" key="$3" value="$4"; shift 4
  docker rm -f seeder >/dev/null 2>&1
  # The official image drops to uid 999; a volume mounted at a path the image
  # doesn't ship (e.g. /bitnami/redis/data) is created root-owned and BGSAVE
  # fails silently. Hand the mount to the redis user first.
  docker run --rm -v "${vol}:${mount}" alpine:latest chown 999:999 "$mount" >/dev/null 2>&1
  docker run -d --name seeder --label "$LABEL" -v "${vol}:${mount}" "$SEED_IMAGE" \
    redis-server --requirepass "$PW" --save 60 1 --dir "$mount" "$@" >/dev/null
  local i
  for i in $(seq 1 30); do
    docker exec seeder redis-cli -a "$PW" PING 2>/dev/null | grep -q PONG && break
    sleep 1
  done
  docker exec seeder redis-cli -a "$PW" SET "$key" "$value" >/dev/null 2>&1
  docker exec seeder redis-cli -a "$PW" BGSAVE >/dev/null 2>&1
  for i in $(seq 1 30); do
    docker exec seeder redis-cli -a "$PW" INFO persistence 2>/dev/null \
      | grep -q "rdb_bgsave_in_progress:0" && break
    sleep 1
  done
  docker rm -f seeder >/dev/null 2>&1
}

# start_node NAME VOLUME MOUNT [extra -e args...]
# Runs a node with the env the redis-ha template stamps, single-node sentinel
# topology unless SENTINEL_HOSTS is overridden via extra args.
start_node() {
  local name="$1" vol="$2" mount="$3"; shift 3
  docker run -d --name "$name" --label "$LABEL" --network "$NET" \
    --network-alias "$name" --hostname "$name" \
    -v "${vol}:${mount}" \
    -e RAILWAY_ENVIRONMENT=production \
    -e RAILWAY_VOLUME_MOUNT_PATH="$mount" \
    -e RAILWAY_PRIVATE_DOMAIN="$name" \
    -e REDIS_PASSWORD="$PW" \
    -e REDIS_PORT=6379 \
    -e HEALTH_PORT=8080 \
    -e REDIS_APPENDONLY=yes \
    -e REDIS_MASTER_NAME=mymaster \
    -e SENTINEL_ENABLED=true \
    -e SENTINEL_PORT=26379 \
    -e SENTINEL_QUORUM=2 \
    -e SENTINEL_DOWN_AFTER_MS=5000 \
    -e SENTINEL_HOSTS="${name}:26379" \
    -e SENTINEL_RESOLVE_HOSTNAMES=yes \
    -e SENTINEL_ANNOUNCE_HOSTNAMES=yes \
    -e REDIS_ANNOUNCE_HOSTNAME="$name" \
    -e REPLICA_OF= \
    "$@" \
    "$IMAGE" >/dev/null
}

rcli() { docker exec "$1" redis-cli -a "$PW" "${@:2}" 2>/dev/null; }

wait_for_ping() { # wait_for_ping NODE [timeout]
  local i
  for i in $(seq 1 "${2:-60}"); do
    rcli "$1" PING | grep -q PONG && return 0
    sleep 1
  done
  return 1
}

wait_for_log_line() { # wait_for_log_line NODE PATTERN [timeout]
  local i
  for i in $(seq 1 "${3:-60}"); do
    docker logs "$1" 2>&1 | grep -q "$2" && return 0
    sleep 1
  done
  return 1
}

wait_for_file_in_volume() { # wait_for_file_in_volume VOLUME PATH [timeout]
  local i
  for i in $(seq 1 "${3:-60}"); do
    docker run --rm -v "$1:/v" alpine:latest test -e "/v/$2" >/dev/null 2>&1 && return 0
    sleep 1
  done
  return 1
}

wait_for_sentinel_peers() { # wait_for_sentinel_peers NODE MIN_PEERS [timeout]
  # Sentinels discover each other through the master's pub/sub; killing the
  # master before discovery completes leaves the survivors unable to reach
  # quorum, so failover tests must wait for the mesh to form first.
  local i peers
  for i in $(seq 1 "${3:-60}"); do
    peers=$(docker exec "$1" redis-cli -p 26379 SENTINEL master mymaster 2>/dev/null       | grep -A1 "^num-other-sentinels$" | tail -1)
    [ -n "$peers" ] && [ "$peers" -ge "$2" ] 2>/dev/null && return 0
    sleep 1
  done
  return 1
}

# The sentinel's own links to the replicas must be live before a failover
# test: selection skips replicas whose instance link is disconnected or whose
# INFO is stale, and that state is invisible from the outside — the election
# succeeds and then aborts with -failover-abort-no-good-slave forever. When a
# view doesn't converge, SENTINEL RESET forces rediscovery from the master's
# INFO — the same remedy an operator would use.
wait_for_sentinel_slave_view() { # wait_for_sentinel_slave_view NODE MIN_SLAVES [timeout]
  local i good resets=0
  for i in $(seq 1 "${3:-60}"); do
    good=$(docker exec "$1" redis-cli -p 26379 SENTINEL slaves mymaster 2>/dev/null       | paste - - | awk '
        $1=="flags" && $2=="slave" { f=1 }
        $1=="master-link-status" && $2=="ok" { l=1 }
        $1=="info-refresh" && $2+0 < 10000 { r=1 }
        $1=="name" { if (f&&l&&r) n++; f=l=r=0 }
        END { if (f&&l&&r) n++; print n+0 }')
    [ "$good" -ge "$2" ] 2>/dev/null && return 0
    if [ $(( i % 20 )) -eq 0 ] && [ "$resets" -lt 2 ]; then
      docker exec "$1" redis-cli -p 26379 SENTINEL RESET mymaster >/dev/null 2>&1
      resets=$((resets + 1))
    fi
    sleep 1
  done
  return 1
}

dump_sentinel_view() { # dump_sentinel_view NODE...
  for n in "$@"; do
    echo "--- SENTINEL view (${n}) ---" >&2
    docker exec "$n" redis-cli -p 26379 SENTINEL slaves mymaster 2>/dev/null       | paste - - | grep -E "^name|^flags|master-link-status|info-refresh|last-ping-reply" >&2
  done
}

wait_for_role_master() { # wait_for_role_master NODE [timeout]
  local i
  for i in $(seq 1 "${2:-90}"); do
    docker exec "$1" sh -c 'wget -qO- http://127.0.0.1:8080/role' 2>/dev/null \
      | grep -q '"role":"master"' && return 0
    sleep 1
  done
  return 1
}

link_status() { # link_status NODE  ->  "up" | "down" | ""
  rcli "$1" INFO replication | tr -d '\r' | grep '^master_link_status:' | cut -d: -f2
}

wait_for_link_status() { # wait_for_link_status NODE up|down [timeout]
  local i
  for i in $(seq 1 "${3:-30}"); do
    [ "$(link_status "$1")" = "$2" ] && return 0
    sleep 1
  done
  return 1
}

wait_for_partition() { # wait_for_partition FROM_NODE UNREACHABLE_NODE [timeout]
  # `docker network disconnect` returning is not proof the partition already
  # took effect network-wide — confirm it from another node's point of view
  # (2 consecutive failures, not 1, to rule out a single dropped probe) before
  # relying on it. Skipping this check once produced a false pass: Sentinel
  # reconfigured the "partitioned" replica normally because it was, in fact,
  # still reachable when the master died.
  local i ok_count=0
  for i in $(seq 1 "${3:-30}"); do
    if docker exec "$1" sh -c "wget -qO- --timeout=2 http://$2:8080/health" >/dev/null 2>&1; then
      ok_count=0
    else
      ok_count=$((ok_count + 1))
      [ "$ok_count" -ge 2 ] && return 0
    fi
    sleep 1
  done
  return 1
}

redis_role() { # redis_role NODE  ->  "master" | "slave" | ""
  rcli "$1" INFO replication | tr -d '\r' | awk -F: '/^role:/{print $2}'
}

master_host_of() { # master_host_of NODE  ->  the host this replica points at
  rcli "$1" INFO replication | tr -d '\r' | awk -F: '/^master_host:/{print $2}'
}

# The host on the `sentinel monitor` line of a volume's sentinel.conf —
# Sentinel's own record of who is master, rewritten after every failover, and
# the state a node reads back at boot to pick its role.
sentinel_conf_master_host() { # sentinel_conf_master_host VOLUME
  docker run --rm -v "$1:/v" alpine:latest cat /v/sentinel.conf 2>/dev/null \
    | awk '$1=="sentinel" && $2=="monitor" && $3=="mymaster" {print $4; exit}'
}

wait_for_sentinel_conf_master() { # wait_for_sentinel_conf_master VOLUME HOST [timeout]
  local i
  for i in $(seq 1 "${3:-90}"); do
    [ "$(sentinel_conf_master_host "$1")" = "$2" ] && return 0
    sleep 1
  done
  return 1
}

write_key() { # write_key NODE KEY VALUE [timeout]
  # Retried, not fire-and-forget: a master whose replicas have not (re)attached
  # yet is fenced by min-replicas-to-write, and right after a promotion that
  # state is transient. A single attempt would read as a silent -NOREPLICAS.
  local i result
  for i in $(seq 1 "${4:-30}"); do
    result=$(rcli "$1" SET "$2" "$3")
    [ "$result" = "OK" ] && return 0
    sleep 1
  done
  return 1
}

wait_for_key() { # wait_for_key NODE KEY VALUE [timeout]
  local i
  for i in $(seq 1 "${4:-60}"); do
    [ "$(rcli "$1" GET "$2")" = "$3" ] && return 0
    sleep 1
  done
  return 1
}

# A 3-node cluster (PREFIX-1..3 on PREFIX-vol-1..3), PREFIX-1 deployed as the
# initial master, brought up to the point where a failover can actually
# succeed: every survivor sees both sentinel peers and has a live view of both
# replicas. Same sequence t_sentinel_failover performs inline.
start_ha_trio() { # start_ha_trio PREFIX [extra -e args...]
  local p="$1"; shift
  local hosts="${p}-1:26379,${p}-2:26379,${p}-3:26379"
  mkvol "${p}-vol-1"; mkvol "${p}-vol-2"; mkvol "${p}-vol-3"
  start_node "${p}-1" "${p}-vol-1" /data -e SENTINEL_HOSTS="$hosts" "$@"
  start_node "${p}-2" "${p}-vol-2" /data -e SENTINEL_HOSTS="$hosts" -e REPLICA_OF="${p}-1:6379" "$@"
  start_node "${p}-3" "${p}-vol-3" /data -e SENTINEL_HOSTS="$hosts" -e REPLICA_OF="${p}-1:6379" "$@"
  wait_for_role_master "${p}-1" || return 1
  local n
  for n in "${p}-2" "${p}-3"; do
    wait_for_sentinel_peers "$n" 2 || return 1
    wait_for_sentinel_slave_view "$n" 2 || return 1
  done
}

# Pause a master (see t_sentinel_failover on why pause and not kill) and wait
# for one of the candidates to take over. Echoes the promoted node's name.
promote_by_pausing() { # promote_by_pausing MASTER CANDIDATE...
  local master="$1"; shift
  docker pause "$master" >/dev/null 2>&1
  local i n
  # 180s: sdown at ~5s, but a tied first election only retries after the
  # template's 30s failover-timeout — room for two full retry cycles.
  for i in $(seq 1 180); do
    for n in "$@"; do
      docker exec "$n" sh -c 'wget -qO- http://127.0.0.1:8080/role' 2>/dev/null \
        | grep -q '"role":"master"' && { echo "$n"; return 0; }
    done
    sleep 1
  done
  return 1
}

wait_for_replica_repointed() { # wait_for_replica_repointed NODE EXPECTED_MASTER_HOST [timeout]
  # Polls the node's OWN INFO replication — the link-heal watcher's target
  # signal — rather than the health server, so this proves the watcher acted,
  # not just that the cluster looks healthy from outside.
  local i info host status
  for i in $(seq 1 "${3:-150}"); do
    info=$(rcli "$1" INFO replication)
    host=$(echo "$info" | grep "^master_host:" | tr -d '\r')
    status=$(echo "$info" | grep "^master_link_status:" | tr -d '\r')
    [ "$host" = "master_host:$2" ] && [ "$status" = "master_link_status:up" ] && return 0
    sleep 1
  done
  return 1
}

# ----- scenarios --------------------------------------------------------------

# A fresh volume boots straight into AOF with a Sentinel-confirmed master —
# and a master with no replicas refuses writes: the generated conf carries
# `min-replicas-to-write 1` as the split-brain fence, so a fully isolated
# master must reject writes rather than accept data its replicas never saw.
t_fresh_boot() {
  local t=t_fresh_boot n=fresh-1
  mkvol fresh-vol
  start_node "$n" fresh-vol /data
  wait_for_role_master "$n" || { ko "$t" "never became master" "$n"; return; }
  rcli "$n" SET k v | grep -q "NOREPLICAS" \
    || { ko "$t" "lone master must refuse writes (min-replicas-to-write fence)" "$n"; return; }
  [ "$(rcli "$n" CONFIG GET appendonly | tail -1)" = "yes" ] \
    || { ko "$t" "appendonly should be yes on a fresh volume" "$n"; return; }
  docker logs "$n" 2>&1 | grep -q "adopted dataset" \
    && { ko "$t" "migration must not trigger on a fresh volume" "$n"; return; }
  docker rm -f "$n" >/dev/null 2>&1
  ok "$t"
}

# The reverted-root case: an HA revert leaves the root on this image with
# SENTINEL_ENABLED stripped. A standalone boot has no replicas by definition,
# so the split-brain fence must not apply — writes go through and the data
# stays AOF-durable.
t_standalone_boot_accepts_writes() {
  local t=t_standalone_boot_accepts_writes n=solo-1
  mkvol solo-vol
  start_node "$n" solo-vol /data -e SENTINEL_ENABLED=
  wait_for_ping "$n" || { ko "$t" "redis never answered PING" "$n"; return; }
  [ "$(rcli "$n" SET k v)" = "OK" ] \
    || { ko "$t" "standalone boot must accept writes (no min-replicas fence)" "$n"; return; }
  [ "$(rcli "$n" GET k)" = "v" ] \
    || { ko "$t" "written key must read back" "$n"; return; }
  [ "$(rcli "$n" CONFIG GET appendonly | tail -1)" = "yes" ] \
    || { ko "$t" "appendonly should be yes on a standalone boot" "$n"; return; }
  docker rm -f "$n" >/dev/null 2>&1
  ok "$t"
}

# The conversion case: a standalone (RDB-only) dataset is adopted, AOF is
# enabled at runtime, and the manifest commit makes it durable.
t_rdb_adoption() {
  local t=t_rdb_adoption n=adopt-1
  mkvol adopt-vol
  seed_rdb_volume adopt-vol /data migkey migvalue
  start_node "$n" adopt-vol /data
  wait_for_log_line "$n" "enabled AOF after loading adopted RDB" \
    || { ko "$t" "AOF migration never completed" "$n"; return; }
  [ "$(rcli "$n" GET migkey)" = "migvalue" ] || { ko "$t" "adopted key lost" "$n"; return; }
  [ "$(rcli "$n" CONFIG GET appendonly | tail -1)" = "yes" ] \
    || { ko "$t" "appendonly not enabled after adoption" "$n"; return; }
  wait_for_file_in_volume adopt-vol appendonlydir/appendonly.aof.manifest 30 \
    || { ko "$t" "AOF manifest never committed" "$n"; return; }
  ok "$t"
  # container/volume intentionally left for t_adoption_survives_restart
}

# The adopted key must be in the committed AOF, not just in memory, and the
# migration must not re-trigger once a manifest exists.
t_adoption_survives_restart() {
  local t=t_adoption_survives_restart n=adopt-1
  docker ps --format '{{.Names}}' | grep -q "^${n}$" \
    || { ko "$t" "requires t_rdb_adoption to have run" "$n"; return; }
  docker restart "$n" >/dev/null 2>&1
  wait_for_ping "$n" || { ko "$t" "did not come back after restart" "$n"; return; }
  [ "$(rcli "$n" GET migkey)" = "migvalue" ] \
    || { ko "$t" "adopted key lost across restart" "$n"; return; }
  local count
  count=$(docker logs "$n" 2>&1 | grep -c "adopted dataset has an RDB")
  [ "$count" = "1" ] || { ko "$t" "migration fired ${count} times, expected 1" "$n"; return; }
  docker rm -f "$n" >/dev/null 2>&1
  ok "$t"
}

# A crash between CONFIG SET and the manifest commit leaves a manifest-less
# appendonlydir. The next boot must quarantine it (rename, never delete) and
# re-adopt the still-intact dump.rdb.
t_crash_window_recovery() {
  local t=t_crash_window_recovery n=crash-1
  mkvol crash-vol
  seed_rdb_volume crash-vol /data crashkey crashvalue
  docker run --rm -v crash-vol:/v alpine:latest sh -c \
    'mkdir -p /v/appendonlydir && echo orphan > /v/appendonlydir/appendonly.aof.1.incr.aof' \
    >/dev/null 2>&1
  start_node "$n" crash-vol /data
  wait_for_log_line "$n" "moved manifest-less appendonlydir aside" \
    || { ko "$t" "orphan dir was not quarantined" "$n"; return; }
  wait_for_log_line "$n" "enabled AOF after loading adopted RDB" \
    || { ko "$t" "AOF migration never completed" "$n"; return; }
  [ "$(rcli "$n" GET crashkey)" = "crashvalue" ] || { ko "$t" "adopted key lost" "$n"; return; }
  # Quarantined, not deleted: the orphan incr file must survive, contents intact.
  docker run --rm -v crash-vol:/v alpine:latest sh -c \
    'cat /v/appendonlydir.orphaned-*/appendonly.aof.1.incr.aof' 2>/dev/null \
    | grep -q orphan || { ko "$t" "orphan files were not preserved" "$n"; return; }
  docker rm -f "$n" >/dev/null 2>&1
  ok "$t"
}

# The data dir follows RAILWAY_VOLUME_MOUNT_PATH — a bitnami-lineage root
# mounted at /bitnami/redis/data adopts its data in place, no /data involved.
t_adoption_at_custom_mount() {
  local t=t_adoption_at_custom_mount n=custom-1 mount=/bitnami/redis/data
  mkvol custom-vol
  seed_rdb_volume custom-vol "$mount" bitkey bitvalue
  start_node "$n" custom-vol "$mount"
  wait_for_log_line "$n" "enabled AOF after loading adopted RDB" \
    || { ko "$t" "AOF migration never completed" "$n"; return; }
  [ "$(rcli "$n" GET bitkey)" = "bitvalue" ] || { ko "$t" "adopted key lost" "$n"; return; }
  wait_for_file_in_volume custom-vol appendonlydir/appendonly.aof.manifest 30 \
    || { ko "$t" "manifest not written to the custom mount" "$n"; return; }
  docker logs "$n" 2>&1 | grep -q "data directory is outside" \
    && { ko "$t" "false persistence warning at a followed mount" "$n"; return; }
  docker rm -f "$n" >/dev/null 2>&1
  ok "$t"
}

# An explicit DATA_DIR outside the volume must warn — not refuse to boot, and
# not silently pass.
t_data_dir_outside_volume_warns() {
  local t=t_data_dir_outside_volume_warns n=outside-1
  mkvol outside-vol
  start_node "$n" outside-vol /data -e DATA_DIR=/tmp/elsewhere
  wait_for_log_line "$n" "data directory is outside the mounted volume" 30 \
    || { ko "$t" "expected persistence warning never logged" "$n"; return; }
  wait_for_ping "$n" || { ko "$t" "node should boot despite the warning" "$n"; return; }
  docker rm -f "$n" >/dev/null 2>&1
  ok "$t"
}

# Redis answers -LOADING while reading a large RDB; the migration must wait it
# out instead of bailing, then adopt every key.
t_large_rdb_loading_retry() {
  local t=t_large_rdb_loading_retry n=big-1
  mkvol big-vol
  docker rm -f seeder >/dev/null 2>&1
  docker run -d --name seeder --label "$LABEL" -v big-vol:/data "$SEED_IMAGE" \
    redis-server --requirepass "$PW" --save 60 1 --dir /data --enable-debug-command yes >/dev/null
  wait_for_ping seeder || { ko "$t" "seeder never came up" seeder; return; }
  docker exec seeder redis-cli -a "$PW" DEBUG POPULATE 3000000 >/dev/null 2>&1
  docker exec seeder redis-cli -a "$PW" BGSAVE >/dev/null 2>&1
  local i
  for i in $(seq 1 60); do
    docker exec seeder redis-cli -a "$PW" INFO persistence 2>/dev/null \
      | grep -q "rdb_bgsave_in_progress:0" && break
    sleep 1
  done
  docker rm -f seeder >/dev/null 2>&1
  start_node "$n" big-vol /data
  wait_for_log_line "$n" "waiting for redis to finish loading" 30 \
    || note "load finished before the first probe — LOADING branch not exercised this run"
  wait_for_log_line "$n" '"keys_adopted":3000000' 120 \
    || { ko "$t" "3M keys were not adopted" "$n"; return; }
  [ "$(rcli "$n" GET key:42)" = "value:42" ] || { ko "$t" "sampled key missing" "$n"; return; }
  docker rm -f "$n" >/dev/null 2>&1
  ok "$t"
}

# When the AOF rewrite child DIES (here: ENOSPC on a size-capped volume), the
# node must keep serving the adopted data, keep retrying — CONFIG SET is a
# no-op once AOF is nominally on; only BGREWRITEAOF starts a new attempt — and
# commit the manifest as soon as the cause clears.
t_rewrite_failure_recovers() {
  local t=t_rewrite_failure_recovers n=fail-1
  docker ps -aq --filter "volume=fail-vol" | xargs -r docker rm -f >/dev/null 2>&1
  docker volume rm -f fail-vol >/dev/null 2>&1
  docker volume create --label "$LABEL" \
    --opt type=tmpfs --opt device=tmpfs --opt o=size=16m fail-vol >/dev/null

  # A tmpfs volume only lives while something has it mounted — contents written
  # by one container evaporate when it exits. The holder keeps the tmpfs alive
  # (and shared) across the ballast, seeder and node containers.
  docker run -d --name fail-holder --label "$LABEL" -v fail-vol:/hold \
    alpine:latest sleep 600 >/dev/null

  # 8MB of ballast so the ~5MB dataset fits but its rewrite (~5MB more) can't.
  docker run --rm -v fail-vol:/v alpine:latest sh -c \
    'dd if=/dev/zero of=/v/ballast bs=1M count=8 2>/dev/null && chown 999:999 /v' \
    >/dev/null 2>&1

  docker rm -f seeder >/dev/null 2>&1
  docker run -d --name seeder --label "$LABEL" -v fail-vol:/data "$SEED_IMAGE" \
    redis-server --requirepass "$PW" --save 60 1 --dir /data --enable-debug-command yes >/dev/null
  wait_for_ping seeder || { ko "$t" "seeder never came up" seeder; return; }
  docker exec seeder redis-cli -a "$PW" DEBUG POPULATE 200000 >/dev/null 2>&1
  docker exec seeder redis-cli -a "$PW" SET failkey failvalue >/dev/null 2>&1
  docker exec seeder redis-cli -a "$PW" BGSAVE >/dev/null 2>&1
  local i
  for i in $(seq 1 30); do
    docker exec seeder redis-cli -a "$PW" INFO persistence 2>/dev/null \
      | grep -q "rdb_bgsave_in_progress:0" && break
    sleep 1
  done
  docker rm -f seeder >/dev/null 2>&1
  docker run --rm -v fail-vol:/v alpine:latest test -s /v/dump.rdb \
    || { ko "$t" "seeding produced no dump.rdb"; return; }

  start_node "$n" fail-vol /data
  wait_for_log_line "$n" "background AOF rewrite did not commit; started a new attempt" 90 \
    || { ko "$t" "reconcile loop never detected the dead rewrite child" "$n"; return; }
  # The node must keep serving the adopted data while the rewrite is failing.
  [ "$(rcli "$n" GET failkey)" = "failvalue" ] \
    || { ko "$t" "adopted data unavailable while the rewrite fails" "$n"; return; }
  docker run --rm -v fail-vol:/v alpine:latest test -e /v/appendonlydir/appendonly.aof.manifest \
    >/dev/null 2>&1 && { ko "$t" "manifest exists while ENOSPC — test premise broken" "$n"; return; }

  # Clear the cause; the loop's next attempt (backoff-capped) must commit.
  docker run --rm -v fail-vol:/v alpine:latest rm -f /v/ballast >/dev/null 2>&1
  wait_for_log_line "$n" "enabled AOF after loading adopted RDB" 180 \
    || { ko "$t" "migration never recovered after space was freed" "$n"; return; }
  wait_for_file_in_volume fail-vol appendonlydir/appendonly.aof.manifest 30 \
    || { ko "$t" "manifest missing after recovery" "$n"; return; }
  [ "$(rcli "$n" GET failkey)" = "failvalue" ] || { ko "$t" "adopted key lost" "$n"; return; }
  [ "$(rcli "$n" CONFIG GET appendonly | tail -1)" = "yes" ] \
    || { ko "$t" "appendonly not on after recovery" "$n"; return; }
  docker rm -f "$n" fail-holder >/dev/null 2>&1
  ok "$t"
}

# A replica full-syncs the adopted dataset — the failover story depends on
# replicas carrying the past, independently of the AOF.
t_replication_of_adopted_data() {
  local t=t_replication_of_adopted_data
  mkvol repl-vol-1; mkvol repl-vol-2
  seed_rdb_volume repl-vol-1 /data replkey replvalue
  start_node repl-1 repl-vol-1 /data \
    -e SENTINEL_HOSTS="repl-1:26379,repl-2:26379"
  start_node repl-2 repl-vol-2 /data \
    -e SENTINEL_HOSTS="repl-1:26379,repl-2:26379" -e REPLICA_OF=repl-1:6379
  wait_for_log_line repl-1 "enabled AOF after loading adopted RDB" \
    || { ko "$t" "primary never finished adoption" repl-1; return; }
  local i
  for i in $(seq 1 60); do
    [ "$(rcli repl-2 GET replkey)" = "replvalue" ] && break
    sleep 1
  done
  [ "$(rcli repl-2 GET replkey)" = "replvalue" ] \
    || { ko "$t" "replica never received the adopted key" repl-1 repl-2; return; }
  # The counterpart of t_fresh_boot's fence: with a good replica online the
  # primary accepts writes again, and they propagate.
  rcli repl-1 SET newkey newvalue | grep -q OK \
    || { ko "$t" "primary with a synced replica must accept writes" repl-1; return; }
  for i in $(seq 1 30); do
    [ "$(rcli repl-2 GET newkey)" = "newvalue" ] && break
    sleep 1
  done
  [ "$(rcli repl-2 GET newkey)" = "newvalue" ] \
    || { ko "$t" "post-adoption write never reached the replica" repl-1 repl-2; return; }
  docker rm -f repl-1 repl-2 >/dev/null 2>&1
  ok "$t"
}

# The full conversion story: adopted primary + two replicas + Sentinel quorum.
# Kill the primary; a replica must be promoted and still serve the adopted key.
t_sentinel_failover() {
  local t=t_sentinel_failover
  local hosts="ha-1:26379,ha-2:26379,ha-3:26379"
  mkvol ha-vol-1; mkvol ha-vol-2; mkvol ha-vol-3
  seed_rdb_volume ha-vol-1 /data hakey havalue
  start_node ha-1 ha-vol-1 /data -e SENTINEL_HOSTS="$hosts"
  start_node ha-2 ha-vol-2 /data -e SENTINEL_HOSTS="$hosts" -e REPLICA_OF=ha-1:6379
  start_node ha-3 ha-vol-3 /data -e SENTINEL_HOSTS="$hosts" -e REPLICA_OF=ha-1:6379
  wait_for_role_master ha-1 || { ko "$t" "ha-1 never became master" ha-1 ha-2 ha-3; return; }
  local i
  for i in $(seq 1 60); do
    [ "$(rcli ha-2 GET hakey)" = "havalue" ] && [ "$(rcli ha-3 GET hakey)" = "havalue" ] && break
    sleep 1
  done
  [ "$(rcli ha-2 GET hakey)" = "havalue" ] \
    || { ko "$t" "replicas never synced before the kill" ha-1 ha-2; return; }
  # Each survivor must see BOTH other sentinels before the master dies, or the
  # 2-of-3 quorum can never form and no promotion happens.
  wait_for_sentinel_peers ha-2 2 || { ko "$t" "ha-2 sentinel never saw 2 peers" ha-2; return; }
  wait_for_sentinel_peers ha-3 2 || { ko "$t" "ha-3 sentinel never saw 2 peers" ha-3; return; }
  wait_for_sentinel_slave_view ha-2 2 \
    || { dump_sentinel_view ha-2; ko "$t" "ha-2 sentinel never got a live view of both replicas" ha-2; return; }
  wait_for_sentinel_slave_view ha-3 2 \
    || { dump_sentinel_view ha-3; ko "$t" "ha-3 sentinel never got a live view of both replicas" ha-3; return; }

  # Pause, not kill: on Railway a dead node's private domain keeps resolving
  # (connections fail); docker kill deregisters the name entirely (NXDOMAIN)
  # and Sentinel spins on resolution instead of failing over. Pause keeps the
  # DNS endpoint alive with a hung host behind it — the realistic failure.
  docker pause ha-1 >/dev/null 2>&1
  local promoted=""
  # 180s: sdown at ~5s, but a tied first election only retries after the
  # template's 30s failover-timeout — leave room for two full retry cycles.
  for i in $(seq 1 180); do
    for n in ha-2 ha-3; do
      docker exec "$n" sh -c 'wget -qO- http://127.0.0.1:8080/role' 2>/dev/null \
        | grep -q '"role":"master"' && { promoted="$n"; break 2; }
    done
    sleep 1
  done
  [ -n "$promoted" ] || {
    dump_sentinel_view ha-2 ha-3
    ko "$t" "no replica was promoted after killing the master" ha-2 ha-3
    return
  }
  [ "$(rcli "$promoted" GET hakey)" = "havalue" ] \
    || { ko "$t" "adopted key lost across failover (promoted=${promoted})" "$promoted"; return; }
  note "promoted: ${promoted}"
  docker unpause ha-1 >/dev/null 2>&1
  docker rm -f ha-1 ha-2 ha-3 >/dev/null 2>&1
  ok "$t"
}

# `REPLICA_OF` is the topology at DEPLOY time. Redeploying the node that was
# deployed as the initial master, after Sentinel has moved the master
# elsewhere, used to regenerate `replicaof` from that env var alone and bring
# it back declaring itself master — a second master for as long as Sentinel
# takes to demote it. Its own sentinel.conf, which Sentinel rewrote when it
# observed the switch, names the real master; that is what the boot role now
# comes from, so the node rejoins as a replica from its very first answer.
t_restart_old_master_rejoins_as_replica() {
  local t=t_restart_old_master_rejoins_as_replica
  local hosts="rejoin-1:26379,rejoin-2:26379,rejoin-3:26379"
  start_ha_trio rejoin \
    || { dump_sentinel_view rejoin-2 rejoin-3
         ko "$t" "cluster never became failover-ready" rejoin-1 rejoin-2 rejoin-3; return; }
  write_key rejoin-1 prekey prevalue \
    || { ko "$t" "master never accepted the pre-failover write" rejoin-1; return; }
  wait_for_key rejoin-2 prekey prevalue || { ko "$t" "rejoin-2 never synced" rejoin-1 rejoin-2; return; }
  wait_for_key rejoin-3 prekey prevalue || { ko "$t" "rejoin-3 never synced" rejoin-1 rejoin-3; return; }

  local promoted
  promoted=$(promote_by_pausing rejoin-1 rejoin-2 rejoin-3) || {
    dump_sentinel_view rejoin-2 rejoin-3
    ko "$t" "no replica was promoted after pausing the master" rejoin-2 rejoin-3
    return
  }
  note "promoted: ${promoted}"
  # Only the new master has this write — the rejoined node cannot end up with
  # it by any route other than syncing from the promoted node.
  write_key "$promoted" postkey postvalue \
    || { ko "$t" "post-failover write never succeeded on $promoted" "$promoted"; return; }

  # The old master comes back and Sentinel demotes it live. That is the moment
  # its own Sentinel records the new master on its volume — the state the
  # redeploy below has to read back.
  docker unpause rejoin-1 >/dev/null 2>&1
  wait_for_sentinel_conf_master rejoin-vol-1 "$promoted" \
    || { note "rejoin-vol-1 sentinel monitor host: '$(sentinel_conf_master_host rejoin-vol-1)'"
         ko "$t" "rejoin-1's sentinel never recorded ${promoted} as master" rejoin-1; return; }

  # The redeploy: same volume, same deploy-time env (REPLICA_OF still empty).
  docker rm -f rejoin-1 >/dev/null 2>&1
  start_node rejoin-1 rejoin-vol-1 /data -e SENTINEL_HOSTS="$hosts"
  wait_for_ping rejoin-1 || { ko "$t" "rejoined node never answered PING" rejoin-1; return; }

  # Asserted on the FIRST answer, not after a convergence wait: Sentinel would
  # eventually demote a node that came back as a master, and waiting for that
  # would pass with or without the boot-role fix. The role has to be right
  # because redis.conf said so.
  local role host
  role=$(redis_role rejoin-1)
  host=$(master_host_of rejoin-1)
  [ "$role" = "slave" ] \
    || { ko "$t" "rejoined node came back as '${role}', expected slave of ${promoted}" rejoin-1; return; }
  [ "$host" = "$promoted" ] \
    || { ko "$t" "rejoined node points at '${host}', expected ${promoted}" rejoin-1; return; }
  # Not `grep -q`: under `pipefail` it exits on the first match, docker logs
  # takes SIGPIPE while still writing, and the pipeline reports 141 — a false
  # negative on a pattern that DID match.
  docker logs rejoin-1 2>&1 \
    | grep -F "boot role: replica of ${promoted}:6379 (from sentinel.conf" >/dev/null \
    || { ko "$t" "boot role decision was not logged" rejoin-1; return; }

  wait_for_link_status rejoin-1 up 60 \
    || { ko "$t" "rejoined replica never linked to ${promoted}" rejoin-1 "$promoted"; return; }
  wait_for_key rejoin-1 postkey postvalue \
    || { ko "$t" "post-failover write never reached the rejoined node" rejoin-1 "$promoted"; return; }
  wait_for_key rejoin-1 prekey prevalue 30 \
    || { ko "$t" "pre-failover key missing after the rejoin" rejoin-1; return; }
  # The promoted master keeps its dataset — the rejoining node syncs from it,
  # never the other way round.
  [ "$(rcli "$promoted" GET postkey)" = "postvalue" ] && [ "$(rcli "$promoted" GET prekey)" = "prevalue" ] \
    || { ko "$t" "promoted master lost data when the old master rejoined" "$promoted"; return; }

  docker rm -f rejoin-1 rejoin-2 rejoin-3 >/dev/null 2>&1
  ok "$t"
}

# Deterministic cold-start bootstrap: the one failure mode Sentinel cannot
# self-resolve. With every node's role coming from `REPLICA_OF`, restarting the
# whole cluster recreates the DEPLOY-time topology — the promoted master
# demotes itself back onto the node it was promoted over and the writes it took
# after the failover are full-synced away. Each volume's sentinel.conf names the
# promoted node, so the cluster has to come back around it.
t_cold_restart_preserves_promoted_master() {
  local t=t_cold_restart_preserves_promoted_master
  local hosts="cold-1:26379,cold-2:26379,cold-3:26379"
  start_ha_trio cold \
    || { dump_sentinel_view cold-2 cold-3
         ko "$t" "cluster never became failover-ready" cold-1 cold-2 cold-3; return; }
  write_key cold-1 coldkey coldvalue \
    || { ko "$t" "master never accepted the pre-failover write" cold-1; return; }
  wait_for_key cold-2 coldkey coldvalue || { ko "$t" "cold-2 never synced" cold-1 cold-2; return; }
  wait_for_key cold-3 coldkey coldvalue || { ko "$t" "cold-3 never synced" cold-1 cold-3; return; }

  local promoted
  promoted=$(promote_by_pausing cold-1 cold-2 cold-3) || {
    dump_sentinel_view cold-2 cold-3
    ko "$t" "no replica was promoted after pausing the master" cold-2 cold-3
    return
  }
  note "promoted: ${promoted}"
  # The write that only survives if the promoted node is still master after the
  # cold restart.
  write_key "$promoted" postcoldkey postcoldvalue \
    || { ko "$t" "post-failover write never succeeded on $promoted" "$promoted"; return; }

  # Every node must have observed the switch before the cluster goes down: a
  # node that was down for the whole failover has no record of it and comes
  # back as a master, which is a Sentinel problem, not a boot-role one.
  docker unpause cold-1 >/dev/null 2>&1
  local v
  for v in cold-vol-1 cold-vol-2 cold-vol-3; do
    wait_for_sentinel_conf_master "$v" "$promoted" \
      || { note "${v} sentinel monitor host: '$(sentinel_conf_master_host "$v")'"
           ko "$t" "${v} never recorded ${promoted} as master" cold-1 cold-2 cold-3; return; }
  done

  # Cold restart: every node destroyed and recreated with its original
  # deploy-time env — cold-1 as the initial master, cold-2/3 as its replicas.
  #
  # The promoted node goes first and the others follow once it answers. A
  # container start costs several seconds, so bringing it up last would leave
  # its master address unreachable for longer than SENTINEL_DOWN_AFTER_MS and
  # the sentinels that did come up would legitimately fail over to each other —
  # a Sentinel scenario, not a boot-role one, and it would mask what this test
  # asserts.
  docker rm -f cold-1 cold-2 cold-3 >/dev/null 2>&1
  local n order="$promoted"
  for n in cold-1 cold-2 cold-3; do
    [ "$n" = "$promoted" ] || order="${order} ${n}"
  done
  for n in $order; do
    if [ "$n" = "cold-1" ]; then
      start_node cold-1 cold-vol-1 /data -e SENTINEL_HOSTS="$hosts"
    else
      start_node "$n" "cold-vol-${n#cold-}" /data -e SENTINEL_HOSTS="$hosts" -e REPLICA_OF=cold-1:6379
    fi
    wait_for_ping "$n" || { ko "$t" "${n} never came back after the cold restart" cold-1 cold-2 cold-3; return; }
  done

  # Sampled as soon as all three answer, before Sentinel has had a chance to
  # reconcile anything: the topology has to be right out of the generated
  # confs. Deploy-time roles would show cold-1 as master here.
  local masters=""
  for n in cold-1 cold-2 cold-3; do
    [ "$(redis_role "$n")" = "master" ] && masters="${masters}${masters:+ }${n}"
  done
  [ "$masters" = "$promoted" ] \
    || { ko "$t" "expected exactly one master (${promoted}), got: [${masters}]" cold-1 cold-2 cold-3; return; }
  docker logs "$promoted" 2>&1 \
    | grep -F "boot role: master (sentinel.conf names this node" >/dev/null \
    || { ko "$t" "${promoted} did not take its master role from sentinel.conf" "$promoted"; return; }

  [ "$(rcli "$promoted" GET postcoldkey)" = "postcoldvalue" ] \
    || { ko "$t" "post-failover write lost across the cold restart" "$promoted"; return; }
  [ "$(rcli "$promoted" GET coldkey)" = "coldvalue" ] \
    || { ko "$t" "pre-failover key lost across the cold restart" "$promoted"; return; }
  for n in cold-1 cold-2 cold-3; do
    [ "$n" = "$promoted" ] && continue
    wait_for_key "$n" postcoldkey postcoldvalue \
      || { ko "$t" "${n} never resynced the post-failover write from ${promoted}" "$n" "$promoted"; return; }
  done

  docker rm -f cold-1 cold-2 cold-3 >/dev/null 2>&1
  ok "$t"
}

# A replica pinned to a host that can never answer never heals through
# Redis's own retry loop — nothing changes what it's retrying. Only the
# in-image link-heal watcher, which sources its fix target from Sentinel
# rather than replaying the replica's own (now-wrong) config, can recover it.
# LINK_HEAL_* thresholds are lowered so the dwell fits inside a test timeout.
t_link_heal_recovers_broken_replica_link() {
  local t=t_link_heal_recovers_broken_replica_link
  local hosts="link-1:26379,link-2:26379"
  local fast=(-e LINK_HEAL_POLL_SECONDS=1 -e LINK_HEAL_DWELL_SECONDS=5 -e LINK_HEAL_ACTION_BACKOFF_SECONDS=1)
  mkvol link-vol-1; mkvol link-vol-2
  seed_rdb_volume link-vol-1 /data linkkey linkvalue
  start_node link-1 link-vol-1 /data -e SENTINEL_HOSTS="$hosts" "${fast[@]}"
  start_node link-2 link-vol-2 /data -e SENTINEL_HOSTS="$hosts" -e REPLICA_OF=link-1:6379 "${fast[@]}"
  wait_for_log_line link-1 "enabled AOF after loading adopted RDB" \
    || { ko "$t" "primary never finished adoption" link-1; return; }
  local i
  for i in $(seq 1 60); do
    [ "$(rcli link-2 GET linkkey)" = "linkvalue" ] && break
    sleep 1
  done
  [ "$(rcli link-2 GET linkkey)" = "linkvalue" ] \
    || { ko "$t" "replica never synced before the fault injection" link-1 link-2; return; }

  rcli link-2 REPLICAOF nonexistent-host 6379 >/dev/null
  wait_for_link_status link-2 down 15 \
    || { ko "$t" "fault injection did not take (link never read down)" link-2; return; }

  wait_for_log_line link-2 "reissuing REPLICAOF on a durably broken link" 60 \
    || { ko "$t" "link-heal watcher never acted" link-2; return; }
  wait_for_link_status link-2 up 30 \
    || { ko "$t" "link never recovered after link-heal acted" link-1 link-2; return; }
  [ "$(rcli link-2 GET linkkey)" = "linkvalue" ] \
    || { ko "$t" "adopted key missing after link-heal recovery" link-2; return; }

  docker rm -f link-1 link-2 >/dev/null 2>&1
  ok "$t"
}

# The other link-heal scenario breaks the pointer by hand; this one proves
# the watcher's actual target case — a replica partitioned at the exact
# moment Sentinel reconfigures survivors during a real failover, then
# reconnected with no restart.
#
# 5 nodes, not fewer. Losing the master's colocated sentinel plus the
# partitioned replica's takes out 2 sentinels at once, and two separate
# majorities are at stake:
#   - ODOWN quorum (2, the template default) — only the master needs to go
#     down for this vote, satisfiable with as few as 3 total nodes.
#   - Leader election to decide WHO performs the failover requires a strict
#     majority of ALL configured sentinels, independent of quorum. With N
#     total and 2 gone, survivors (N-2) must clear floor(N/2)+1. N=4 leaves 2
#     survivors needing 3 — impossible. N=5 leaves exactly 3 needing 3 —
#     enough, since those 3 stay mutually reachable.
t_link_heal_recovers_from_partition_during_failover() {
  local t=t_link_heal_recovers_from_partition_during_failover
  local hosts="part-1:26379,part-2:26379,part-3:26379,part-4:26379,part-5:26379"
  local fast=(-e LINK_HEAL_POLL_SECONDS=1 -e LINK_HEAL_DWELL_SECONDS=5 -e LINK_HEAL_ACTION_BACKOFF_SECONDS=1)
  mkvol part-vol-1; mkvol part-vol-2; mkvol part-vol-3; mkvol part-vol-4; mkvol part-vol-5
  seed_rdb_volume part-vol-1 /data partkey partvalue
  start_node part-1 part-vol-1 /data -e SENTINEL_HOSTS="$hosts" "${fast[@]}"
  start_node part-2 part-vol-2 /data -e SENTINEL_HOSTS="$hosts" -e REPLICA_OF=part-1:6379 "${fast[@]}"
  start_node part-3 part-vol-3 /data -e SENTINEL_HOSTS="$hosts" -e REPLICA_OF=part-1:6379 "${fast[@]}"
  start_node part-4 part-vol-4 /data -e SENTINEL_HOSTS="$hosts" -e REPLICA_OF=part-1:6379 "${fast[@]}"
  start_node part-5 part-vol-5 /data -e SENTINEL_HOSTS="$hosts" -e REPLICA_OF=part-1:6379 "${fast[@]}"
  wait_for_role_master part-1 \
    || { ko "$t" "part-1 never became master" part-1 part-2 part-3 part-4 part-5; return; }
  local i n
  for i in $(seq 1 90); do
    [ "$(rcli part-2 GET partkey)" = "partvalue" ] \
      && [ "$(rcli part-3 GET partkey)" = "partvalue" ] \
      && [ "$(rcli part-4 GET partkey)" = "partvalue" ] \
      && [ "$(rcli part-5 GET partkey)" = "partvalue" ] && break
    sleep 1
  done
  [ "$(rcli part-5 GET partkey)" = "partvalue" ] \
    || { ko "$t" "part-5 never synced before the partition" part-1 part-5; return; }

  # Every sentinel needs to see the other 4 and have a live view of all 4
  # replicas BEFORE anything is disconnected — this is what lets part-2,
  # part-3 and part-4 reach BOTH the ODOWN quorum and the leader-election
  # majority on their own once part-1 is gone and part-5 is unreachable.
  for n in part-2 part-3 part-4; do
    wait_for_sentinel_peers "$n" 4 || { ko "$t" "$n sentinel never saw 4 peers" "$n"; return; }
    wait_for_sentinel_slave_view "$n" 4 \
      || { dump_sentinel_view "$n"; ko "$t" "$n sentinel never got a live view of all 4 replicas" "$n"; return; }
  done

  # Record identity before the repair window — proves what follows happens
  # with no restart of part-5's container at all, only reconnecting its
  # network. A redeploy or restart would change this.
  local part5_started
  part5_started=$(docker inspect -f '{{.State.StartedAt}}' part-5)

  # Partition part-5 fully off the network BEFORE the master dies, so it is
  # unreachable for the whole failover and cannot receive Sentinel's
  # reconfiguration. part-2/3/4 stay mutually reachable, so together they
  # still clear both the ODOWN quorum and the leader-election majority.
  docker network disconnect "$NET" part-5 >/dev/null 2>&1
  wait_for_partition part-2 part-5 \
    || { ko "$t" "part-5 was not actually unreachable after disconnect — test setup race" part-2 part-5; return; }
  docker pause part-1 >/dev/null 2>&1

  local promoted=""
  for i in $(seq 1 180); do
    for n in part-2 part-3 part-4; do
      docker exec "$n" sh -c 'wget -qO- http://127.0.0.1:8080/role' 2>/dev/null \
        | grep -q '"role":"master"' && { promoted="$n"; break 2; }
    done
    sleep 1
  done
  [ -n "$promoted" ] || {
    dump_sentinel_view part-2 part-3 part-4
    ko "$t" "no reachable replica was promoted while part-5 stayed partitioned" part-2 part-3 part-4
    return
  }
  note "promoted: ${promoted}"

  # A write accepted only by the NEW master — part-5 cannot have this yet by
  # any means other than correctly resyncing from it after rejoining. Retried,
  # not fire-and-forget: right after SLAVEOF NO ONE the new master has ZERO
  # connected replicas (Sentinel reconfigures survivors one at a time —
  # REDIS_PARALLEL_SYNCS=1), so it is transiently subject to the SAME
  # min-replicas-to-write fence t_fresh_boot asserts. Writing too early gets
  # a silent -NOREPLICAS rejection that nothing will ever resync, which reads
  # exactly like the watcher failing when it is this setup step that did.
  local write_result
  for i in $(seq 1 30); do
    write_result=$(rcli "$promoted" SET postfailoverkey postfailovervalue)
    [ "$write_result" = "OK" ] && break
    sleep 1
  done
  [ "$write_result" = "OK" ] \
    || { ko "$t" "post-failover write never succeeded on $promoted (${write_result})" "$promoted"; return; }

  # Reconnect part-5. Its boot-time REPLICA_OF still names part-1 (now
  # paused/unreachable), so INFO replication will keep reading
  # master_link_status:down until something repoints it — that something is
  # the link-heal watcher under test, not this script.
  docker network connect "$NET" part-5 --alias part-5 >/dev/null 2>&1

  wait_for_replica_repointed part-5 "$promoted" \
    || { dump_sentinel_view part-5; ko "$t" "part-5 never repointed itself at $promoted" part-5; return; }

  local part5_started_after
  part5_started_after=$(docker inspect -f '{{.State.StartedAt}}' part-5)
  [ "$part5_started" = "$part5_started_after" ] \
    || { ko "$t" "part-5 was restarted — this must be fixed live, not by a redeploy" part-5; return; }

  for i in $(seq 1 30); do
    [ "$(rcli part-5 GET postfailoverkey)" = "postfailovervalue" ] && break
    sleep 1
  done
  [ "$(rcli part-5 GET postfailoverkey)" = "postfailovervalue" ] \
    || { ko "$t" "part-5 repointed but never actually resynced the post-failover write" part-5; return; }
  [ "$(rcli part-5 GET partkey)" = "partvalue" ] \
    || { ko "$t" "pre-failover data lost after self-healing the link" part-5; return; }

  docker unpause part-1 >/dev/null 2>&1
  docker rm -f part-1 part-2 part-3 part-4 part-5 >/dev/null 2>&1
  ok "$t"
}


# A node added by a scale-up boots with REPLICA_OF naming whoever was master
# when the template was stamped. If the cluster failed over since, that
# address is a demoted replica: attach there and the new node never appears
# in the real master's INFO, so no sentinel learns it exists and
# +fix-slave-config can never repoint it. First line of defense: with no
# local sentinel state, ask the peers' sentinels who the master is and
# attach there from the very first handshake.
t_new_node_boot_asks_peer_sentinels() {
  local t=t_new_node_boot_asks_peer_sentinels
  start_ha_trio peerq \
    || { ko "$t" "trio never became a functioning cluster" peerq-1 peerq-2 peerq-3; return; }
  local promoted
  promoted=$(promote_by_pausing peerq-1 peerq-2 peerq-3) \
    || { dump_sentinel_view peerq-2 peerq-3; ko "$t" "no replica was promoted" peerq-2 peerq-3; return; }
  note "promoted: ${promoted}"

  # The scale-up node: fresh volume, env topology still naming the paused,
  # demoted peerq-1 — exactly what the platform would stamp.
  mkvol peerq-vol-4
  start_node peerq-4 peerq-vol-4 /data \
    -e SENTINEL_HOSTS="peerq-1:26379,peerq-2:26379,peerq-3:26379" \
    -e REPLICA_OF=peerq-1:6379
  wait_for_log_line peerq-4 "from peer sentinels" 30 \
    || { ko "$t" "boot role never came from the peer sentinels" peerq-4; return; }
  wait_for_replica_repointed peerq-4 "$promoted" 90 \
    || { ko "$t" "new node did not attach to the current master" peerq-4 "$promoted"; return; }

  docker unpause peerq-1 >/dev/null 2>&1
  docker rm -f peerq-1 peerq-2 peerq-3 peerq-4 >/dev/null 2>&1
  ok "$t"
}

# The backstop for the same race, for when the peer query cannot save the
# boot (failover completing WHILE the node syncs, or the query switched
# off): the node attaches to the demoted ex-master, link healthy, chained
# behind the real master, invisible to every sentinel. link-heal's
# wrong-master watch must spot the disagreement with Sentinel's answer and
# repoint it live.
t_link_heal_repoints_wrong_master_attachment() {
  local t=t_link_heal_repoints_wrong_master_attachment
  local fast=(-e LINK_HEAL_POLL_SECONDS=1 -e LINK_HEAL_WRONG_MASTER_DWELL_SECONDS=5 -e LINK_HEAL_ACTION_BACKOFF_SECONDS=1)
  start_ha_trio wrongm "${fast[@]}" \
    || { ko "$t" "trio never became a functioning cluster" wrongm-1 wrongm-2 wrongm-3; return; }
  local promoted
  promoted=$(promote_by_pausing wrongm-1 wrongm-2 wrongm-3) \
    || { dump_sentinel_view wrongm-2 wrongm-3; ko "$t" "no replica was promoted" wrongm-2 wrongm-3; return; }
  note "promoted: ${promoted}"

  # Bring the demoted ex-master back as a replica so it can serve a chained
  # sync to the new node — the shape the incident produced.
  docker unpause wrongm-1 >/dev/null 2>&1
  wait_for_replica_repointed wrongm-1 "$promoted" 120 \
    || { ko "$t" "old master never rejoined as a replica" wrongm-1 "$promoted"; return; }

  mkvol wrongm-vol-4
  start_node wrongm-4 wrongm-vol-4 /data \
    -e SENTINEL_HOSTS="wrongm-1:26379,wrongm-2:26379,wrongm-3:26379" \
    -e REPLICA_OF=wrongm-1:6379 \
    -e BOOT_ROLE_FROM_PEER_SENTINELS=false \
    "${fast[@]}"
  wait_for_replica_repointed wrongm-4 wrongm-1 90 \
    || { ko "$t" "node never attached to the demoted master (fault injection failed)" wrongm-4 wrongm-1; return; }

  wait_for_log_line wrongm-4 "repointing a replica durably attached to the wrong master" 90 \
    || { ko "$t" "wrong-master watch never acted" wrongm-4; return; }
  wait_for_replica_repointed wrongm-4 "$promoted" 90 \
    || { ko "$t" "replica never landed on the real master after the heal" wrongm-4 "$promoted"; return; }

  docker rm -f wrongm-1 wrongm-2 wrongm-3 wrongm-4 >/dev/null 2>&1
  ok "$t"
}

# Every sentinel.conf freezes the quorum its first boot computed, and a
# preserved conf never rereads the restamped env — after a 3→5 scale the old
# nodes kept quorum 2 while the new ones wrote 3. The quorum-sync watcher
# keeps each local sentinel at a strict majority of the sentinels it
# actually gossips with, so the whole cluster converges on 3 here even
# though every node was SEEDED with the stale template default of 2.
#
# The split-brain fence must converge with it: every node here booted with
# min-replicas-to-write 1 (the stale SENTINEL_QUORUM=2 stamp), which on a
# 5-node cluster only fences a FULLY isolated master — a partition trapping
# one replica with the old master would leave two writers. The watcher must
# raise every node to majority − 1 = 2.
t_quorum_follows_registered_membership() {
  local t=t_quorum_follows_registered_membership
  local fast=(-e QUORUM_SYNC_POLL_SECONDS=2 -e QUORUM_SYNC_DWELL_SECONDS=5)
  local hosts="quor-1:26379,quor-2:26379,quor-3:26379,quor-4:26379,quor-5:26379"
  local i
  for i in 1 2 3 4 5; do mkvol "quor-vol-${i}"; done
  start_node quor-1 quor-vol-1 /data -e SENTINEL_HOSTS="$hosts" "${fast[@]}"
  for i in 2 3 4 5; do
    start_node "quor-${i}" "quor-vol-${i}" /data \
      -e SENTINEL_HOSTS="$hosts" -e REPLICA_OF=quor-1:6379 "${fast[@]}"
  done
  wait_for_role_master quor-1 || { ko "$t" "quor-1 never became master" quor-1; return; }

  local n q converged
  for n in quor-1 quor-2 quor-3 quor-4 quor-5; do
    converged=""
    for i in $(seq 1 120); do
      q=$(docker exec "$n" redis-cli -p 26379 SENTINEL master mymaster 2>/dev/null \
        | grep -A1 "^quorum$" | tail -1)
      [ "$q" = "3" ] && { converged=1; break; }
      sleep 1
    done
    [ -n "$converged" ] || { ko "$t" "${n} quorum never converged to 3 (got '\''${q}'\'')" "$n"; return; }
  done

  local f
  for n in quor-1 quor-2 quor-3 quor-4 quor-5; do
    converged=""
    for i in $(seq 1 120); do
      f=$(rcli "$n" CONFIG GET min-replicas-to-write | tail -1)
      [ "$f" = "2" ] && { converged=1; break; }
      sleep 1
    done
    [ -n "$converged" ] \
      || { ko "$t" "${n} fence never converged to 2 (got '\''${f}'\'')" "$n"; return; }
  done

  docker rm -f quor-1 quor-2 quor-3 quor-4 quor-5 >/dev/null 2>&1
  ok "$t"
}


# Sentinel never forgets a removed peer: after a scale-down the leftover
# s_down entries permanently inflate the failover-election denominator (a
# 5->3 cluster still needs 3-of-5, i.e. unanimity of the survivors). The
# quorum-sync watcher prunes peers down past the dwell via a local SENTINEL
# RESET, and the membership-majority quorum then shrinks with them.
t_scale_down_prunes_dead_sentinels() {
  local t=t_scale_down_prunes_dead_sentinels
  local fast=(-e QUORUM_SYNC_POLL_SECONDS=2 -e QUORUM_SYNC_DWELL_SECONDS=5 -e SENTINEL_PRUNE_DWELL_SECONDS=10 -e SENTINEL_PRUNE_BACKOFF_SECONDS=5)
  local hosts="prune-1:26379,prune-2:26379,prune-3:26379,prune-4:26379,prune-5:26379"
  local i
  for i in 1 2 3 4 5; do mkvol "prune-vol-${i}"; done
  start_node prune-1 prune-vol-1 /data -e SENTINEL_HOSTS="$hosts" "${fast[@]}"
  for i in 2 3 4 5; do
    start_node "prune-${i}" "prune-vol-${i}" /data \
      -e SENTINEL_HOSTS="$hosts" -e REPLICA_OF=prune-1:6379 "${fast[@]}"
  done
  wait_for_role_master prune-1 || { ko "$t" "prune-1 never became master" prune-1; return; }

  # Let every survivor-to-be see the full 5-sentinel membership first, so
  # the removal below is a real scale-down of a converged cluster.
  local n
  for n in prune-1 prune-2 prune-3; do
    wait_for_sentinel_peers "$n" 4 120 \
      || { ko "$t" "${n} never saw the full membership" "$n"; return; }
  done

  # Scale down 5 -> 3: the removed services are deleted, DNS and all.
  docker rm -f prune-4 prune-5 >/dev/null 2>&1
  docker volume rm -f prune-vol-4 prune-vol-5 >/dev/null 2>&1

  # Each survivor independently: marks the two dead sentinels s_down, serves
  # the 10s prune dwell, RESETs its local view, and the membership-majority
  # quorum then converges to majority(3) = 2. The 3 survivors are a live
  # majority of the 5 known, so the prune's partition gate lets this pass.
  local q peers converged
  for n in prune-1 prune-2 prune-3; do
    converged=""
    for i in $(seq 1 180); do
      peers=$(docker exec "$n" redis-cli -p 26379 SENTINEL master mymaster 2>/dev/null \
        | grep -A1 "^num-other-sentinels$" | tail -1)
      q=$(docker exec "$n" redis-cli -p 26379 SENTINEL master mymaster 2>/dev/null \
        | grep -A1 "^quorum$" | tail -1)
      [ "$peers" = "2" ] && [ "$q" = "2" ] && { converged=1; break; }
      sleep 1
    done
    [ -n "$converged" ] \
      || { ko "$t" "${n} never pruned to 2 peers / quorum 2 (peers='\''${peers}'\'' quorum='\''${q}'\'')" "$n"; return; }
    wait_for_log_line "$n" "reset the local sentinel to forget peers down past the dwell" 10 \
      || { ko "$t" "${n} converged without the prune log line" "$n"; return; }
  done

  # The fence shrinks with the membership: majority(3) − 1 = 1. Without
  # this a 5→3 scale-down leaves the master demanding 2 acking replicas of
  # the single one it has left, fencing every write on a healthy cluster.
  local f
  for n in prune-1 prune-2 prune-3; do
    converged=""
    for i in $(seq 1 180); do
      f=$(rcli "$n" CONFIG GET min-replicas-to-write | tail -1)
      [ "$f" = "1" ] && { converged=1; break; }
      sleep 1
    done
    [ -n "$converged" ] \
      || { ko "$t" "${n} fence never shrank to 1 (got '\''${f}'\'')" "$n"; return; }
  done

  docker rm -f prune-1 prune-2 prune-3 >/dev/null 2>&1
  ok "$t"
}

# A multi-pair scale-down (7→3 in one jump) deletes a MAJORITY of the
# cluster at once, so the survivors can never satisfy the prune's
# live-majority gate — and without pruning, the fence denominator never
# shrinks and the master rejects writes forever. Deletion is the one case
# the DNS probe can prove: a removed service's name stops resolving
# (NXDOMAIN), while a partitioned peer's record stays. Every s_down peer
# answering NXDOMAIN for the whole prune dwell waives the majority gate.
# Modeled here as 5→2: delete 3 of 5 services outright, leaving the master
# plus one replica — a live minority of the known membership.
t_deleted_majority_unfences_via_nxdomain() {
  local t=t_deleted_majority_unfences_via_nxdomain
  local fast=(-e QUORUM_SYNC_POLL_SECONDS=2 -e QUORUM_SYNC_DWELL_SECONDS=5 -e SENTINEL_PRUNE_DWELL_SECONDS=10 -e SENTINEL_PRUNE_BACKOFF_SECONDS=5)
  local hosts="gone-1:26379,gone-2:26379,gone-3:26379,gone-4:26379,gone-5:26379"
  local i
  for i in 1 2 3 4 5; do mkvol "gone-vol-${i}"; done
  start_node gone-1 gone-vol-1 /data -e SENTINEL_HOSTS="$hosts" "${fast[@]}"
  for i in 2 3 4 5; do
    start_node "gone-${i}" "gone-vol-${i}" /data \
      -e SENTINEL_HOSTS="$hosts" -e REPLICA_OF=gone-1:6379 "${fast[@]}"
  done
  wait_for_role_master gone-1 || { ko "$t" "gone-1 never became master" gone-1; return; }

  # The 5-node fence must be up first, or the test proves nothing.
  local f
  for i in $(seq 1 120); do
    f=$(rcli gone-1 CONFIG GET min-replicas-to-write | tail -1)
    [ "$f" = "2" ] && break
    sleep 1
  done
  [ "$f" = "2" ] || { ko "$t" "fence never reached 2 before the deletion (got '\''${f}'\'')" gone-1; return; }

  # Delete the majority, DNS names and all.
  docker rm -f gone-3 gone-4 gone-5 >/dev/null 2>&1
  docker volume rm -f gone-vol-3 gone-vol-4 gone-vol-5 >/dev/null 2>&1

  # One acking replica < fence 2: the master must reject writes first —
  # this is the wedge the waiver exists to resolve.
  local fenced=""
  for i in $(seq 1 60); do
    rcli gone-1 SET gonekey gonevalue | grep -q NOREPLICAS && { fenced=1; break; }
    sleep 1
  done
  [ -n "$fenced" ] || { ko "$t" "master never fenced after losing the majority" gone-1; return; }

  # Both survivors independently: serve the sdown dwell, observe continuous
  # NXDOMAIN for the deleted three, waive the majority gate, RESET, and the
  # fence follows the shrunken membership back to 1. Writes resume.
  local n converged
  for n in gone-1 gone-2; do
    wait_for_log_line "$n" "reset the local sentinel to forget peers down past the dwell" 120 \
      || { ko "$t" "${n} never pruned the deleted majority" "$n"; return; }
    converged=""
    for i in $(seq 1 120); do
      f=$(rcli "$n" CONFIG GET min-replicas-to-write | tail -1)
      [ "$f" = "1" ] && { converged=1; break; }
      sleep 1
    done
    [ -n "$converged" ] \
      || { ko "$t" "${n} fence never shrank to 1 after the prune (got '\''${f}'\'')" "$n"; return; }
  done
  local write_ok=""
  for i in $(seq 1 60); do
    [ "$(rcli gone-1 SET gonekey gonevalue)" = "OK" ] && { write_ok=1; break; }
    sleep 1
  done
  [ -n "$write_ok" ] || { ko "$t" "writes never resumed after the fence shrank" gone-1; return; }

  docker rm -f gone-1 gone-2 >/dev/null 2>&1
  ok "$t"
}

# The other side of the waiver: a majority that is merely UNREACHABLE — a
# partition, modeled with docker pause so the containers keep their DNS
# records exactly like a partitioned Railway service keeps its private
# domain — must never be pruned by the minority. The names still resolve,
# the NXDOMAIN windows never open, and the master stays fenced: on a real
# partition the paused side may be electing a new master right now, and an
# unfenced old master would be the second writer.
t_paused_majority_keeps_the_fence() {
  local t=t_paused_majority_keeps_the_fence
  local fast=(-e QUORUM_SYNC_POLL_SECONDS=2 -e QUORUM_SYNC_DWELL_SECONDS=5 -e SENTINEL_PRUNE_DWELL_SECONDS=10 -e SENTINEL_PRUNE_BACKOFF_SECONDS=5)
  local hosts="hold-1:26379,hold-2:26379,hold-3:26379,hold-4:26379,hold-5:26379"
  local i
  for i in 1 2 3 4 5; do mkvol "hold-vol-${i}"; done
  start_node hold-1 hold-vol-1 /data -e SENTINEL_HOSTS="$hosts" "${fast[@]}"
  for i in 2 3 4 5; do
    start_node "hold-${i}" "hold-vol-${i}" /data \
      -e SENTINEL_HOSTS="$hosts" -e REPLICA_OF=hold-1:6379 "${fast[@]}"
  done
  wait_for_role_master hold-1 || { ko "$t" "hold-1 never became master" hold-1; return; }

  local f
  for i in $(seq 1 120); do
    f=$(rcli hold-1 CONFIG GET min-replicas-to-write | tail -1)
    [ "$f" = "2" ] && break
    sleep 1
  done
  [ "$f" = "2" ] || { ko "$t" "fence never reached 2 before the pause (got '\''${f}'\'')" hold-1; return; }

  docker pause hold-3 hold-4 hold-5 >/dev/null 2>&1

  local fenced=""
  for i in $(seq 1 60); do
    rcli hold-1 SET holdkey holdvalue | grep -q NOREPLICAS && { fenced=1; break; }
    sleep 1
  done
  [ -n "$fenced" ] || {
    docker unpause hold-3 hold-4 hold-5 >/dev/null 2>&1
    ko "$t" "master never fenced after losing contact with the majority" hold-1; return;
  }

  # Serve out the sdown dwell (10s), the would-be waiver dwell (10s) and a
  # few poll cycles on top: long enough that a wrongly-granted waiver WOULD
  # have pruned and unfenced by now (the deletion test above converges well
  # inside this window).
  sleep 45

  if docker logs hold-1 2>&1 | grep -q "reset the local sentinel to forget peers down past the dwell"; then
    docker unpause hold-3 hold-4 hold-5 >/dev/null 2>&1
    ko "$t" "the minority pruned a majority that still resolves" hold-1; return;
  fi
  f=$(rcli hold-1 CONFIG GET min-replicas-to-write | tail -1)
  [ "$f" = "2" ] || {
    docker unpause hold-3 hold-4 hold-5 >/dev/null 2>&1
    ko "$t" "fence dropped to '\''${f}'\'' while the majority was only unreachable" hold-1; return;
  }
  rcli hold-1 SET holdkey holdvalue | grep -q NOREPLICAS || {
    docker unpause hold-3 hold-4 hold-5 >/dev/null 2>&1
    ko "$t" "master accepted a write while fenced off from the majority" hold-1; return;
  }

  # Heal: the paused side comes back, replicas re-ack, writes resume with
  # the fence untouched.
  docker unpause hold-3 hold-4 hold-5 >/dev/null 2>&1
  local write_ok=""
  for i in $(seq 1 60); do
    [ "$(rcli hold-1 SET holdkey holdvalue)" = "OK" ] && { write_ok=1; break; }
    sleep 1
  done
  [ -n "$write_ok" ] || { ko "$t" "writes never resumed after the heal" hold-1; return; }

  docker rm -f hold-1 hold-2 hold-3 hold-4 hold-5 >/dev/null 2>&1
  ok "$t"
}

# SENTINEL_HOSTS is stamped at deploy time and scale-up does not restamp
# existing nodes, so a founding node restarting after a failover onto a
# scale-up member finds its sentinel.conf naming a master the env never
# heard of. That state is legitimate: the boot must probe the address with
# the cluster's shared password and preserve it, not quarantine a live
# topology on every restart (#19).
t_scaled_member_master_is_preserved_at_boot() {
  local t=t_scaled_member_master_is_preserved_at_boot
  # The "scale-up member the env never learned about": a live master
  # sharing the cluster password, absent from the booting node's
  # SENTINEL_HOSTS.
  mkvol memb-vol-m
  start_node memb-m memb-vol-m /data
  wait_for_role_master memb-m || { ko "$t" "memb-m never became master" memb-m; return; }

  # The founding node: volume already carries sentinel state naming that
  # out-of-topology master — the post-failover shape — while its own env
  # (SENTINEL_HOSTS=memb-1 only, REPLICA_OF empty) knows nothing of it.
  # The seeded conf mirrors what the wrapper writes and Sentinel rewrites:
  # resolve-hostnames must precede a hostname monitor line or Sentinel
  # refuses to boot, and auth-pass is what lets it actually see the master.
  mkvol memb-vol-1
  docker run --rm -v memb-vol-1:/v alpine:latest sh -c "printf '%s\n' \
    'sentinel resolve-hostnames yes' \
    'sentinel announce-hostnames yes' \
    'sentinel monitor mymaster memb-m 6379 2' \
    'sentinel auth-pass mymaster ${PW}' > /v/sentinel.conf && chown -R 999:999 /v" \
    >/dev/null 2>&1
  start_node memb-1 memb-vol-1 /data
  wait_for_log_line memb-1 "authenticates as a live member" 30 \
    || { ko "$t" "membership probe never preserved the undeclared master" memb-1; return; }
  wait_for_replica_repointed memb-1 memb-m 90 \
    || { ko "$t" "node never attached to the preserved master" memb-1 memb-m; return; }
  docker run --rm -v memb-vol-1:/v alpine:latest sh -c 'ls /v/sentinel.conf.ghost-*' \
    >/dev/null 2>&1 \
    && { ko "$t" "live member state must not be quarantined" memb-1; return; }

  docker rm -f memb-m memb-1 >/dev/null 2>&1
  ok "$t"
}

# The membership probe's discriminator is the shared password, not DNS: a
# foreign service that happens to reuse the hostname of a dead member
# resolves and answers, but refuses the cluster's AUTH — that state is a
# dead world and must still be quarantined.
t_foreign_host_reusing_member_name_is_quarantined() {
  local t=t_foreign_host_reusing_member_name_is_quarantined
  docker rm -f memb-x >/dev/null 2>&1
  docker run -d --name memb-x --label "$LABEL" --network "$NET" \
    --network-alias memb-x "$SEED_IMAGE" \
    redis-server --requirepass not-the-cluster-password >/dev/null

  mkvol memb-vol-f
  docker run --rm -v memb-vol-f:/v alpine:latest sh -c \
    'echo "sentinel monitor mymaster memb-x 6379 2" > /v/sentinel.conf && chown -R 999:999 /v' \
    >/dev/null 2>&1
  start_node memb-f memb-vol-f /data
  wait_for_log_line memb-f "moved ghost sentinel.conf aside" 30 \
    || { ko "$t" "foreign-auth master was not quarantined" memb-f memb-x; return; }
  wait_for_role_master memb-f || { ko "$t" "node never fell back to the env topology" memb-f; return; }
  docker run --rm -v memb-vol-f:/v alpine:latest sh -c 'ls /v/sentinel.conf.ghost-*' \
    >/dev/null 2>&1 \
    || { ko "$t" "quarantined state must be preserved on the volume" memb-f; return; }

  docker rm -f memb-x memb-f >/dev/null 2>&1
  ok "$t"
}

# ----- runner ------------------------------------------------------------------
ALL_TESTS=(
  t_fresh_boot
  t_standalone_boot_accepts_writes
  t_rdb_adoption
  t_adoption_survives_restart
  t_crash_window_recovery
  t_adoption_at_custom_mount
  t_data_dir_outside_volume_warns
  t_large_rdb_loading_retry
  t_rewrite_failure_recovers
  t_replication_of_adopted_data
  t_sentinel_failover
  t_restart_old_master_rejoins_as_replica
  t_cold_restart_preserves_promoted_master
  t_link_heal_recovers_broken_replica_link
  t_link_heal_recovers_from_partition_during_failover
  t_new_node_boot_asks_peer_sentinels
  t_link_heal_repoints_wrong_master_attachment
  t_quorum_follows_registered_membership
  t_scale_down_prunes_dead_sentinels
  t_deleted_majority_unfences_via_nxdomain
  t_paused_majority_keeps_the_fence
  t_scaled_member_master_is_preserved_at_boot
  t_foreign_host_reusing_member_name_is_quarantined
)

setup
RUNLIST=("${@:-${ALL_TESTS[@]}}")
for t in "${RUNLIST[@]}"; do
  log "running ${t}"
  "$t"
done

echo
log "passed: ${PASS}  failed: ${FAIL}"
[ "$FAIL" -gt 0 ] && log "failed tests: ${FAILED_TESTS[*]}"
exit "$FAIL"
