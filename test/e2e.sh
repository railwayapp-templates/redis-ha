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

wait_for_role_master() { # wait_for_role_master NODE [timeout]
  local i
  for i in $(seq 1 "${2:-90}"); do
    docker exec "$1" sh -c 'wget -qO- http://127.0.0.1:8080/role' 2>/dev/null \
      | grep -q '"role":"master"' && return 0
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

  docker kill ha-1 >/dev/null 2>&1
  local promoted=""
  for i in $(seq 1 90); do
    for n in ha-2 ha-3; do
      docker exec "$n" sh -c 'wget -qO- http://127.0.0.1:8080/role' 2>/dev/null \
        | grep -q '"role":"master"' && { promoted="$n"; break 2; }
    done
    sleep 1
  done
  [ -n "$promoted" ] || { ko "$t" "no replica was promoted after killing the master" ha-2 ha-3; return; }
  [ "$(rcli "$promoted" GET hakey)" = "havalue" ] \
    || { ko "$t" "adopted key lost across failover (promoted=${promoted})" "$promoted"; return; }
  note "promoted: ${promoted}"
  docker rm -f ha-1 ha-2 ha-3 >/dev/null 2>&1
  ok "$t"
}

# ----- runner ------------------------------------------------------------------
ALL_TESTS=(
  t_fresh_boot
  t_rdb_adoption
  t_adoption_survives_restart
  t_crash_window_recovery
  t_adoption_at_custom_mount
  t_data_dir_outside_volume_warns
  t_large_rdb_loading_retry
  t_replication_of_adopted_data
  t_sentinel_failover
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
