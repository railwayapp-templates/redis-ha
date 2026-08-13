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
# volume mount path, and Sentinel failover preserving adopted data — plus the
# live-traffic contracts: conversion and failover under a continuously
# writing client (through the real HAProxy edge for the failover case), and
# maxmemory pressure degrading writes without destabilizing the cluster.

set -uo pipefail

IMAGE="${IMAGE:-redis-sentinel-e2e:local}"
# The edge (HAProxy) image, exercised by the through-the-edge scenarios. CI
# pre-builds it (cache-warmed) under this tag; a local run that doesn't have
# it builds it on first use — inside the scenario, not setup, so subset runs
# of unrelated scenarios never pay the cargo build.
EDGE_IMAGE="${EDGE_IMAGE:-haproxy-e2e:local}"
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
      # Sentinel floods its log with resolve errors on any failure, pushing
      # the wrapper's own decisions out of the 40-line window — surface the
      # watcher lines (quorum-sync, prune, dns probe) separately; they are
      # what the membership/fence scenarios need to be triaged from CI.
      echo "${R}--- wrapper watcher lines ${c} (last 15) ---${N}" >&2
      docker logs "$c" 2>&1 | grep -aE "quorum|prune|dns|fence|min-replicas" \
        | tail -15 | sed 's/^/    /' >&2
      # The local sentinel's own membership view: peer/replica counts and the
      # quorum it holds explain a wrong fence value directly (the fence
      # follows majority(known membership) - 1).
      echo "${R}--- sentinel view ${c} ---${N}" >&2
      {
        scli "$c" SENTINEL master mymaster 2>/dev/null | paste - - \
          | grep -aE "^(quorum|num-other-sentinels|num-slaves|flags)\b" || true
        echo "sentinels-known: $(scli "$c" SENTINEL sentinels mymaster 2>/dev/null | grep -ac '^name$' || true)"
      } | sed 's/^/    /' >&2
    fi
  done
  # A failed scenario returns without reaching its own cleanup, leaving a
  # whole cluster (up to 5 nodes x redis+sentinel+wrapper each) running until
  # the exit trap. Later scenarios then compete with the stragglers for the
  # runner's CPU — the resource-heavy membership scenarios run last and are
  # exactly the ones that time out under that load. Remove the dumped
  # containers now that their logs are captured.
  for c in "$@"; do
    docker rm -f "$c" >/dev/null 2>&1
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

# Sentinel-port client. Sentinel auth is now on by default for fresh
# clusters, reusing REDIS_PASSWORD, so this authenticates with the same
# password rcli uses. Against a sentinel that requires no auth (kill-switch
# or posture-matched scenarios) redis-cli's AUTH failure is non-fatal — it
# prints "AUTH failed: ..." to stderr (discarded here) and runs the command
# anyway — verified against redis 8.2.1, so one helper serves both postures.
scli() { docker exec "$1" redis-cli -p 26379 -a "$PW" --no-auth-warning "${@:2}" 2>/dev/null; }

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
    peers=$(scli "$1" SENTINEL master mymaster       | grep -A1 "^num-other-sentinels$" | tail -1)
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
    good=$(scli "$1" SENTINEL slaves mymaster       | paste - - | awk '
        $1=="flags" && $2=="slave" { f=1 }
        $1=="master-link-status" && $2=="ok" { l=1 }
        $1=="info-refresh" && $2+0 < 10000 { r=1 }
        $1=="name" { if (f&&l&&r) n++; f=l=r=0 }
        END { if (f&&l&&r) n++; print n+0 }')
    [ "$good" -ge "$2" ] 2>/dev/null && return 0
    if [ $(( i % 20 )) -eq 0 ] && [ "$resets" -lt 2 ]; then
      scli "$1" SENTINEL RESET mymaster >/dev/null
      resets=$((resets + 1))
    fi
    sleep 1
  done
  return 1
}

dump_sentinel_view() { # dump_sentinel_view NODE...
  for n in "$@"; do
    echo "--- SENTINEL view (${n}) ---" >&2
    scli "$n" SENTINEL slaves mymaster       | paste - - | grep -E "^name|^flags|master-link-status|info-refresh|last-ping-reply" >&2
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

wait_for_replica_attached_host() { # wait_for_replica_attached_host NODE EXPECTED_MASTER_HOST [timeout]
  # Attachment target only, link state ignored — for asserting a TRANSIENT
  # attachment a watcher is about to undo. A fresh replica full-syncing from
  # the wrong master holds master_host=<wrong> with the link still down for
  # the whole transfer (diskless-sync delay included), and link-heal's
  # wrong-master dwell runs concurrently off that same master_host field —
  # requiring link "up" here means catching the sub-second window between
  # sync completion and the heal, a coin flip on a 1s poll.
  local i host
  for i in $(seq 1 "${3:-90}"); do
    host=$(master_host_of "$1")
    [ "$host" = "$2" ] && return 0
    sleep 1
  done
  return 1
}

# ----- write-load helpers -----------------------------------------------------
# A paced sequential writer for the "under live traffic" scenarios: SETs
# w:1, w:2, ... against TARGET (a node name or an edge alias) and appends
# "<seq> <epoch>" to /acked inside its own container for every reply that
# came back OK. The ledger is the contract under test: an acked write is the
# server's promise, an unacked one is the client's retry problem — so every
# assertion downstream is phrased over /acked, never over what was attempted.
# ~10 writes/s keeps a failover window populated with in-flight traffic
# without turning the post-run ledger scan into the slow part of the suite.
start_seq_writer() { # start_seq_writer NAME TARGET_HOST
  docker run -d --name "$1" --label "$LABEL" --network "$NET" \
    -e PW="$PW" -e TARGET="$2" "$SEED_IMAGE" sh -c '
      : > /acked
      i=0
      while :; do
        i=$((i+1))
        out=$(redis-cli -a "$PW" -h "$TARGET" SET "w:$i" "$i" 2>/dev/null)
        [ "$out" = "OK" ] && echo "$i $(date +%s)" >> /acked
        sleep 0.1
      done' >/dev/null
}

acked_count() { # acked_count WRITER
  docker exec "$1" sh -c 'wc -l < /acked' 2>/dev/null | tr -d '[:space:]'
}

last_acked() { # last_acked WRITER  ->  highest acked sequence number
  docker exec "$1" sh -c 'tail -1 /acked' 2>/dev/null | awk '{print $1}'
}

wait_for_new_ack() { # wait_for_new_ack WRITER PREV_COUNT [timeout]
  local i c
  for i in $(seq 1 "${3:-90}"); do
    c=$(acked_count "$1")
    [ -n "$c" ] && [ "$c" -gt "$2" ] && return 0
    sleep 1
  done
  return 1
}

# Freeze the writer and verify its whole ledger through CHECK_NODE (a
# container with redis-cli), reading via CHECK_HOST — the same path a client
# would use. SIGKILL, not stop: the ledger only ever gains a line AFTER a
# reply arrived, so killing mid-write can at worst lose an entry the server
# acked (ledger ⊆ acked — safe direction), never record one it didn't.
# The ledger is copied INTO the checking container so the whole scan is one
# `docker exec` with in-container round trips, not one exec per key.
# Echoes the missing "seq:epoch" entries (empty output = ledger fully intact);
# the copied ledger is left at /tmp/acked-<writer> on CHECK_NODE for callers
# that need the raw entries afterwards.
freeze_and_find_missing() { # freeze_and_find_missing WRITER CHECK_NODE CHECK_HOST
  local ledger="/tmp/acked-$1"
  docker kill "$1" >/dev/null 2>&1
  docker cp "$1:/acked" "${TMPDIR:-/tmp}/e2e-acked-$$" >/dev/null 2>&1 || return 1
  # World-readable before it lands in the checking container: `docker cp`
  # preserves the tar's owner (root), and the checker may exec as a non-root
  # user (the edge image runs as `haproxy`) — an unreadable ledger would make
  # the scan silently vacuous, which the sentinel below also guards against.
  chmod 644 "${TMPDIR:-/tmp}/e2e-acked-$$"
  docker cp "${TMPDIR:-/tmp}/e2e-acked-$$" "$2:${ledger}" >/dev/null 2>&1 || return 1
  rm -f "${TMPDIR:-/tmp}/e2e-acked-$$"
  # A ledger the scan cannot read MUST come back as a failure the caller
  # trips over, never as "nothing missing": echo a sentinel entry (sorts as
  # seq 0 — behind every barrier, outside every window) instead of exiting.
  docker exec -e PW="$PW" -e CHECK_HOST="$3" -e LEDGER="$ledger" "$2" sh -c '
    if [ ! -r "$LEDGER" ] || [ ! -s "$LEDGER" ]; then
      echo "0:LEDGER-UNREADABLE-OR-EMPTY"
      exit 0
    fi
    while read -r i ts; do
      v=$(redis-cli -a "$PW" -h "$CHECK_HOST" GET "w:$i" 2>/dev/null)
      [ "$v" = "$i" ] || echo "$i:$ts"
    done < "$LEDGER"'
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
  docker logs "$n" 2>&1 | grep "adopted dataset" >/dev/null \
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
  docker logs "$n" 2>&1 | grep "data directory is outside" >/dev/null \
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

# Conversion while a client is actively writing — the standalone→HA cutover as
# the application experiences it, not as a quiesced lab exercise. A paced
# writer runs against the standalone node (the Railway template's exact shape:
# RDB-only `--save 60 1`, no AOF) through the graceful stop and into the HA
# composition booting on the same volume and private domain.
#
# The zero-loss claim is strict and it is real: a save-point-configured Redis
# performs SHUTDOWN SAVE on SIGTERM, and it is single-threaded — any write it
# acked completed before the shutdown sequence started, so the shutdown RDB
# contains every acked write by construction, including the ones newer than
# any periodic BGSAVE. Adoption then carries that RDB into AOF. If any acked
# key is missing after conversion, the conversion lost real data.
#
# During the gap the writer's SETs simply fail (unacked — the client's retry
# problem, not a loss), and they must start succeeding again WITHOUT any
# client-side reconfiguration once a replica attaches and the write fence
# lifts — same hostname, same port, same credentials.
t_conversion_under_active_writes() {
  local t=t_conversion_under_active_writes
  local hosts="conv-node:26379,conv-2:26379,conv-3:26379"
  mkvol conv-vol-1; mkvol conv-vol-2; mkvol conv-vol-3
  # The standalone template runs as uid 999 against a volume it didn't create.
  docker run --rm -v conv-vol-1:/data alpine:latest chown 999:999 /data >/dev/null 2>&1
  docker run -d --name conv-node --label "$LABEL" --network "$NET" \
    --network-alias conv-node --hostname conv-node -v conv-vol-1:/data \
    "$SEED_IMAGE" redis-server --requirepass "$PW" --save 60 1 --dir /data >/dev/null
  wait_for_ping conv-node || { ko "$t" "standalone node never answered" conv-node; return; }

  start_seq_writer conv-writer conv-node
  wait_for_new_ack conv-writer 0 30 \
    || { ko "$t" "writer never got an ack from the standalone node" conv-node conv-writer; return; }
  sleep 5 # accumulate acked traffic strictly newer than any periodic save

  # Railway's conversion: graceful stop (SIGTERM + grace window), then the HA
  # composition takes over the volume and the root keeps the private domain.
  docker stop -t 30 conv-node >/dev/null 2>&1
  docker rm -f conv-node >/dev/null 2>&1
  start_node conv-node conv-vol-1 /data -e SENTINEL_HOSTS="$hosts"
  start_node conv-2 conv-vol-2 /data -e SENTINEL_HOSTS="$hosts" -e REPLICA_OF=conv-node:6379
  start_node conv-3 conv-vol-3 /data -e SENTINEL_HOSTS="$hosts" -e REPLICA_OF=conv-node:6379
  wait_for_log_line conv-node "enabled AOF after loading adopted RDB" 60 \
    || { ko "$t" "adoption never completed on the converted root" conv-node; return; }

  # Acks must resume with zero writer-side changes once the fence lifts.
  # 180s: full sync of two fresh replicas has to finish first.
  local resumed_from
  resumed_from=$(acked_count conv-writer)
  wait_for_new_ack conv-writer "$resumed_from" 180 \
    || { ko "$t" "writes never resumed after the conversion" conv-node conv-2 conv-writer; return; }
  sleep 3 # a little steady post-conversion traffic

  local missing last
  missing=$(freeze_and_find_missing conv-writer conv-node 127.0.0.1)
  [ -z "$missing" ] \
    || { ko "$t" "acked writes lost across the conversion: ${missing}" conv-node conv-2 conv-3; return; }
  note "ledger: $(docker exec conv-node sh -c 'wc -l < /tmp/acked-conv-writer' | tr -d '[:space:]') acked writes, all intact across the conversion"
  # And the converted dataset — pre-stop and post-conversion writes alike —
  # must actually be replicated, not just present on the root.
  last=$(docker exec conv-node sh -c 'tail -1 /tmp/acked-conv-writer' | awk '{print $1}')
  wait_for_key conv-2 "w:$last" "$last" \
    || { ko "$t" "converted dataset never reached the replica" conv-node conv-2; return; }

  docker rm -f conv-node conv-2 conv-3 conv-writer >/dev/null 2>&1
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

# A client through the REAL edge (the repo's HAProxy image, /role-checked
# routing) across an unplanned failover. Every other failover scenario talks
# to the nodes directly; this one asserts the only endpoint a customer
# application actually holds: one hostname, one port, dumb retries, no
# Sentinel awareness — and the edge alone must reconverge it to the new
# master.
#
# What is asserted about the ledger is exactly what Sentinel + asynchronous
# replication promises, no more: writes acked at or before the replication
# barrier (a key CONFIRMED on both replicas before the master is paused) can
# never be lost; writes acked after it ride a single connection, so any loss
# the promotion cut inflicts is a FIFO tail — at most ONE contiguous run in
# acked order, timestamped inside the failover window. Scattered loss, loss
# behind the barrier, or loss of post-failover acks would each mean a bug
# (in the edge's routing, in promotion, or in replication) rather than the
# documented async-replication tradeoff.
t_edge_client_writes_survive_failover() {
  local t=t_edge_client_writes_survive_failover
  if ! docker image inspect "$EDGE_IMAGE" >/dev/null 2>&1; then
    log "building ${EDGE_IMAGE}"
    docker build -q -f "${REPO_ROOT}/haproxy/Dockerfile" -t "$EDGE_IMAGE" "$REPO_ROOT" >/dev/null || {
      ko "$t" "edge image build failed"; return;
    }
  fi
  start_ha_trio edgec \
    || { ko "$t" "trio never reached failover-ready state" edgec-1 edgec-2 edgec-3; return; }
  docker run -d --name edgec-edge --label "$LABEL" --network "$NET" \
    --network-alias edgec-edge --hostname edgec-edge \
    -e REDIS_NODES="edgec-1:6379,edgec-2:6379,edgec-3:6379" \
    "$EDGE_IMAGE" >/dev/null

  # The edge is routing to the master when a write THROUGH it lands.
  local i
  for i in $(seq 1 60); do
    docker exec -e PW="$PW" edgec-edge sh -c \
      'redis-cli -a "$PW" -h 127.0.0.1 SET edgeprobe ok 2>/dev/null' | grep -q OK && break
    sleep 1
  done
  docker exec -e PW="$PW" edgec-edge sh -c \
    'redis-cli -a "$PW" -h 127.0.0.1 GET edgeprobe 2>/dev/null' | grep -q ok \
    || { ko "$t" "edge never routed a write to the master" edgec-edge edgec-1; return; }

  start_seq_writer edgec-writer edgec-edge
  wait_for_new_ack edgec-writer 0 60 \
    || { ko "$t" "writer never got an ack through the edge" edgec-edge edgec-writer; return; }

  # Replication barrier: everything acked up to `mark` is on BOTH replicas,
  # so no failover — whichever candidate wins — may lose it.
  local mark
  mark=$(last_acked edgec-writer)
  wait_for_key edgec-2 "w:$mark" "$mark" \
    || { ko "$t" "replica edgec-2 never caught up to the barrier" edgec-1 edgec-2; return; }
  wait_for_key edgec-3 "w:$mark" "$mark" \
    || { ko "$t" "replica edgec-3 never caught up to the barrier" edgec-1 edgec-3; return; }

  local pre_count pause_ts promoted
  pre_count=$(acked_count edgec-writer)
  pause_ts=$(date +%s)
  promoted=$(promote_by_pausing edgec-1 edgec-2 edgec-3)
  [ -n "$promoted" ] || {
    dump_sentinel_view edgec-2 edgec-3
    ko "$t" "no replica was promoted" edgec-2 edgec-3
    return
  }
  note "promoted: ${promoted}"

  # The same writer, still pointed at the same edge alias, must start
  # collecting acks again with no client-side action at all.
  wait_for_new_ack edgec-writer "$pre_count" 90 \
    || { ko "$t" "writes never resumed through the edge after the failover" edgec-edge "$promoted" edgec-writer; return; }
  sleep 3 # a little steady post-failover traffic

  # Ledger verification THROUGH the edge — reading via the edge is itself the
  # proof that it converged to the promoted node.
  local missing
  missing=$(freeze_and_find_missing edgec-writer edgec-edge 127.0.0.1)
  if [ -n "$missing" ]; then
    local miss_count barrier_violation window_violation runs
    miss_count=$(echo "$missing" | wc -l | tr -d '[:space:]')
    # Nothing at or behind the replication barrier may be missing.
    barrier_violation=$(echo "$missing" | awk -F: -v m="$mark" '$1 <= m' | head -1)
    [ -z "$barrier_violation" ] \
      || { ko "$t" "write acked BEFORE the replication barrier lost: ${barrier_violation} (barrier w:${mark})" "$promoted" edgec-2 edgec-3; return; }
    # Every missing ack must sit inside the failover window.
    window_violation=$(echo "$missing" | awk -F: -v p="$pause_ts" '$2 < p - 5 || $2 > p + 90' | head -1)
    [ -z "$window_violation" ] \
      || { ko "$t" "write lost OUTSIDE the failover window: ${window_violation} (pause at ${pause_ts})" "$promoted" edgec-edge; return; }
    # FIFO loss: the missing entries must be ONE contiguous run in acked order.
    runs=$(docker exec edgec-edge sh -c 'cat /tmp/acked-edgec-writer' \
      | awk -v miss="$(echo "$missing" | awk -F: "{printf \"%s \", \$1}")" '
          BEGIN { n = split(miss, m, " "); for (k = 1; k <= n; k++) mm[m[k]] = 1 }
          { if (mm[$1]) { if (!inrun) runs++; inrun = 1 } else inrun = 0 }
          END { print runs + 0 }')
    [ "$runs" = "1" ] \
      || { ko "$t" "acked-write loss is scattered (${runs} runs, ${miss_count} keys) — not a single promotion cut" "$promoted" edgec-edge; return; }
    note "async-replication tail loss across the failover: ${miss_count} acked write(s), one contiguous run"
  else
    note "ledger: $(docker exec edgec-edge sh -c 'wc -l < /tmp/acked-edgec-writer' | tr -d '[:space:]') acked writes through the edge, zero lost across the failover"
  fi

  docker unpause edgec-1 >/dev/null 2>&1
  docker rm -f edgec-1 edgec-2 edgec-3 edgec-edge edgec-writer >/dev/null 2>&1
  ok "$t"
}

# POST /switchover must promote THE REQUESTED node, not merely trigger some
# failover. Sentinel selects its candidate from a CACHED view of each
# replica's INFO (refreshed every 10s): a FAILOVER issued right after the
# priority bias races that refresh, and on a tie selection falls back to
# replication offset and then run-id — the OTHER replica about half the
# time. The wrapper therefore waits for its local Sentinel to observe the
# bias before failing over. This scenario targets the replica the run-id
# tie-break would NOT pick, so a regression back to the racy order shows up
# as the wrong node winning.
t_switchover_promotes_requested_node() {
  local t=t_switchover_promotes_requested_node
  start_ha_trio swo || { ko "$t" "cluster never became ready" swo-1 swo-2 swo-3; return; }
  write_key swo-1 swokey swovalue || { ko "$t" "seed write failed" swo-1; return; }
  wait_for_key swo-2 swokey swovalue || { ko "$t" "swo-2 never synced the seed key" swo-2; return; }
  wait_for_key swo-3 swokey swovalue || { ko "$t" "swo-3 never synced the seed key" swo-3; return; }

  # Equal offsets on an idle cluster, so a tie falls to the lexicographically
  # smaller run_id — target the LARGER one, the node an unbiased election
  # would not choose.
  local run2 run3 target
  run2=$(rcli swo-2 INFO server | tr -d '\r' | grep '^run_id:' | cut -d: -f2)
  run3=$(rcli swo-3 INFO server | tr -d '\r' | grep '^run_id:' | cut -d: -f2)
  if [[ "$run2" < "$run3" ]]; then target=swo-3; else target=swo-2; fi
  note "requesting promotion of ${target} (the run-id tie-break loser)"

  # The POST blocks through the bias-visibility wait (~7-11s) before the 202.
  local resp
  resp=$(docker exec "$target" sh -c \
    "wget -qO- --post-data='' http://127.0.0.1:8080/switchover" 2>/dev/null)
  echo "$resp" | grep -q '"status":"failover-initiated"' \
    || { ko "$t" "switchover not accepted: ${resp:-<no 2xx response>}" "$target"; return; }

  wait_for_role_master "$target" 90 || {
    dump_sentinel_view swo-2 swo-3
    ko "$t" "the requested node was not the one promoted" swo-1 swo-2 swo-3
    return
  }

  # The election bias must not outlive the switchover: the settle watch
  # restores the pre-bias replica-priority once /role confirms.
  local i prio=""
  for i in $(seq 1 90); do
    prio=$(rcli "$target" CONFIG GET replica-priority | tail -1)
    [ "$prio" = "100" ] && break
    sleep 1
  done
  [ "$prio" = "100" ] \
    || { ko "$t" "replica-priority bias not restored (still ${prio})" "$target"; return; }

  wait_for_key "$target" swokey swovalue 30 \
    || { ko "$t" "seed key lost across the switchover" "$target"; return; }
  docker rm -f swo-1 swo-2 swo-3 >/dev/null 2>&1
  ok "$t"
}

# The planned-shutdown counterpart of t_sentinel_failover: `docker stop`
# sends SIGTERM (not SIGKILL) and gives the container up to `-t` seconds to
# exit on its own — exactly the redeploy path `demote_on_shutdown` targets.
# Without it, the master's process would just die and the survivors would
# only notice once SENTINEL_DOWN_AFTER_MS elapses; with it, the master's own
# local Sentinel is asked to force the failover BEFORE redis-server is even
# signaled, so the switch is confirmed while the old master is still up to
# observe it — proven here directly from its own log, not inferred from
# timing.
t_sigterm_master_demotes_before_exit() {
  local t=t_sigterm_master_demotes_before_exit
  local hosts="demote-1:26379,demote-2:26379,demote-3:26379"
  mkvol demote-vol-1; mkvol demote-vol-2; mkvol demote-vol-3
  start_node demote-1 demote-vol-1 /data -e SENTINEL_HOSTS="$hosts"
  start_node demote-2 demote-vol-2 /data -e SENTINEL_HOSTS="$hosts" -e REPLICA_OF=demote-1:6379
  start_node demote-3 demote-vol-3 /data -e SENTINEL_HOSTS="$hosts" -e REPLICA_OF=demote-1:6379
  wait_for_role_master demote-1 \
    || { ko "$t" "demote-1 never became master" demote-1 demote-2 demote-3; return; }

  write_key demote-1 demotekey demotevalue \
    || { ko "$t" "master never accepted the pre-shutdown write" demote-1; return; }
  wait_for_key demote-2 demotekey demotevalue || { ko "$t" "demote-2 never synced" demote-1 demote-2; return; }
  wait_for_key demote-3 demotekey demotevalue || { ko "$t" "demote-3 never synced" demote-1 demote-3; return; }

  # Same readiness bar as t_sentinel_failover: both survivors must see each
  # other and have a live view of both replicas before the master leaves, or
  # the failover this test is about to force could never actually succeed.
  wait_for_sentinel_peers demote-2 2 || { ko "$t" "demote-2 sentinel never saw 2 peers" demote-2; return; }
  wait_for_sentinel_peers demote-3 2 || { ko "$t" "demote-3 sentinel never saw 2 peers" demote-3; return; }
  wait_for_sentinel_slave_view demote-2 2 \
    || { dump_sentinel_view demote-2; ko "$t" "demote-2 sentinel never got a live view of both replicas" demote-2; return; }
  wait_for_sentinel_slave_view demote-3 2 \
    || { dump_sentinel_view demote-3; ko "$t" "demote-3 sentinel never got a live view of both replicas" demote-3; return; }

  # Unlike an ordinary unplanned failover — where the dying master is never
  # the leader, so only the survivors' views matter — demote-1's own local
  # Sentinel is the one `SENTINEL FAILOVER` forces into the leader role
  # here, and per the docs it runs the WHOLE state machine unilaterally,
  # including reconfiguring every OTHER known replica. If demote-1's own
  # view of demote-3 is stale at that moment (Sentinel discovers slaves from
  # the master's own INFO on a periodic refresh, a few seconds by default),
  # reconf-slaves only touches the replicas it already knew about, and
  # demote-3 is left pointed at the master that is about to disappear until
  # Sentinel's slower `+fix-slave-config` housekeeping eventually catches
  # it — empirically confirmed live (see the demote_on_shutdown PR
  # description) before this check was added.
  wait_for_sentinel_slave_view demote-1 2 \
    || { dump_sentinel_view demote-1; ko "$t" "demote-1 (the future failover leader) never got a live view of both replicas" demote-1; return; }

  # `docker stop -t 30`: SIGTERM, then up to 30s (Railway's own grace window
  # — see demote_on_shutdown's module doc) before a SIGKILL. A demote that
  # actually ran and confirmed should return in well under that.
  local stop_started stop_finished stop_elapsed
  stop_started=$(date +%s)
  docker stop -t 30 demote-1 >/dev/null 2>&1
  stop_finished=$(date +%s)
  stop_elapsed=$(( stop_finished - stop_started ))

  # Direct proof, from the old master's OWN log, that the demote sequence
  # ran and confirmed the failover before it exited — not inferred from
  # timing. graceful_shutdown (and its own kill fallbacks) only run after
  # demote_before_shutdown returns, so this line existing at all means it
  # returned before redis-server or sentinel were ever signaled to stop.
  # Not `grep -q` — see t_restart_old_master_rejoins_as_replica on the
  # SIGPIPE false negative (this exact assertion flaked red in CI with the
  # demote line demonstrably present in the dumped log).
  docker logs demote-1 2>&1 | grep -F "demote-on-shutdown: master shutting down" >/dev/null \
    || { ko "$t" "demote-1 never attempted the pre-shutdown failover" demote-1; return; }
  docker logs demote-1 2>&1 | grep -F "demote-on-shutdown: failover confirmed" >/dev/null \
    || { ko "$t" "demote-1 never confirmed the failover before exiting" demote-1; return; }

  # And it must have actually been fast — nowhere near the -t budget, which
  # is the whole point versus a timeout failover.
  [ "$stop_elapsed" -lt 15 ] \
    || { ko "$t" "docker stop took ${stop_elapsed}s — looks like the shutdown timed out rather than a triggered failover completing" demote-1; return; }

  # A survivor must have taken over, and the switch must be visible in a
  # survivor's own Sentinel log too (`+switch-master`) — the event the local
  # `SENTINEL FAILOVER` request drove.
  local promoted="" n
  for n in demote-2 demote-3; do
    docker exec "$n" sh -c 'wget -qO- http://127.0.0.1:8080/role' 2>/dev/null \
      | grep -q '"role":"master"' && { promoted="$n"; break; }
  done
  [ -n "$promoted" ] || { ko "$t" "no survivor was promoted after the stop" demote-2 demote-3; return; }
  note "promoted: ${promoted} (docker stop took ${stop_elapsed}s)"

  local switch_seen=""
  for n in demote-2 demote-3; do
    docker logs "$n" 2>&1 | grep -q '+switch-master' && { switch_seen=1; break; }
  done
  [ -n "$switch_seen" ] \
    || { ko "$t" "no survivor logged +switch-master" demote-2 demote-3; return; }

  # The new master must accept writes promptly — the blackout this feature
  # closes, not the 5-10s a timeout failover would cost.
  write_key "$promoted" postdemotekey postdemotevalue 15 \
    || { ko "$t" "new master never accepted a write promptly after the stop" "$promoted"; return; }

  # The stopped node must rejoin as a REPLICA on restart: its own Sentinel
  # observed (indeed drove) the switch before it went down, so boot-role
  # resolution reads that back rather than reasserting deploy-time master.
  docker start demote-1 >/dev/null 2>&1
  wait_for_ping demote-1 || { ko "$t" "demote-1 never came back after restart" demote-1; return; }
  local role
  role=$(redis_role demote-1)
  [ "$role" = "slave" ] \
    || { ko "$t" "demote-1 rejoined as '${role}', expected slave of ${promoted}" demote-1; return; }
  wait_for_link_status demote-1 up 60 \
    || { ko "$t" "demote-1 never linked to ${promoted} after rejoining" demote-1 "$promoted"; return; }
  wait_for_key demote-1 postdemotekey postdemotevalue \
    || { ko "$t" "demote-1 never resynced the post-stop write" demote-1; return; }
  wait_for_key demote-1 demotekey demotevalue 30 \
    || { ko "$t" "pre-shutdown key missing after the rejoin" demote-1; return; }

  docker rm -f demote-1 demote-2 demote-3 >/dev/null 2>&1
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

# A master that is DOWN for the whole failover never observes the
# +switch-master, so its volume's sentinel.conf still names ITSELF. Before the
# peer cross-check that boot came back as a second master until the peers'
# higher config epoch demoted it — a real stale-master window observed in
# production on a redeployed ex-master, held off the write path only by the
# health server's 503. The boot must now ask the peer Sentinels first and come
# back as a replica of the promoted node on the FIRST answer.
t_down_for_failover_master_boots_as_replica() {
  local t=t_down_for_failover_master_boots_as_replica
  local hosts="stale-1:26379,stale-2:26379,stale-3:26379"
  start_ha_trio stale \
    || { dump_sentinel_view stale-2 stale-3
         ko "$t" "cluster never became failover-ready" stale-1 stale-2 stale-3; return; }
  write_key stale-1 prekey prevalue \
    || { ko "$t" "master never accepted the pre-failover write" stale-1; return; }
  wait_for_key stale-2 prekey prevalue || { ko "$t" "stale-2 never synced" stale-1 stale-2; return; }
  wait_for_key stale-3 prekey prevalue || { ko "$t" "stale-3 never synced" stale-1 stale-3; return; }

  # Hard-remove the master: its own Sentinel must never observe the failover,
  # so its volume keeps the pre-failover sentinel.conf naming ITSELF. (Not
  # pause: an unpaused Sentinel resumes and rewrites the conf, which is the
  # t_restart_old_master_rejoins_as_replica scenario, not this one.)
  docker rm -f stale-1 >/dev/null 2>&1

  local i n promoted=""
  # Same budget as promote_by_pausing: sdown at ~5s, but a tied first
  # election only retries after the template's 30s failover-timeout.
  for i in $(seq 1 180); do
    for n in stale-2 stale-3; do
      docker exec "$n" sh -c 'wget -qO- http://127.0.0.1:8080/role' 2>/dev/null \
        | grep -q '"role":"master"' && { promoted="$n"; break 2; }
    done
    sleep 1
  done
  [ -n "$promoted" ] || {
    dump_sentinel_view stale-2 stale-3
    ko "$t" "no replica was promoted after removing the master" stale-2 stale-3
    return
  }
  note "promoted: ${promoted}"

  # Precondition, not an assertion of the fix: the down node's volume still
  # names the node itself — the stale state the boot has to distrust.
  [ "$(sentinel_conf_master_host stale-vol-1)" = "stale-1" ] \
    || { note "stale-vol-1 sentinel monitor host: '$(sentinel_conf_master_host stale-vol-1)'"
         ko "$t" "precondition broke: stale-1's volume no longer names itself"; return; }

  # Only the new master has this write — the rejoined node cannot end up with
  # it by any route other than syncing from the promoted node.
  write_key "$promoted" postkey postvalue \
    || { ko "$t" "post-failover write never succeeded on $promoted" "$promoted"; return; }

  # The redeploy: same volume, same deploy-time env (REPLICA_OF still empty).
  start_node stale-1 stale-vol-1 /data -e SENTINEL_HOSTS="$hosts"
  wait_for_ping stale-1 || { ko "$t" "rejoined node never answered PING" stale-1; return; }

  # Asserted on the FIRST answer, not after a convergence wait: Sentinel's
  # higher config epoch would eventually demote a stale master, and waiting
  # for that would pass with or without the peer cross-check. The role has to
  # be right because the boot asked the peers before serving.
  local role host
  role=$(redis_role stale-1)
  host=$(master_host_of stale-1)
  [ "$role" = "slave" ] \
    || { ko "$t" "rejoined node came back as '${role}', expected slave of ${promoted}" stale-1; return; }
  [ "$host" = "$promoted" ] \
    || { ko "$t" "rejoined node points at '${host}', expected ${promoted}" stale-1; return; }
  # Not `grep -q` in a pipeline off docker logs — see
  # t_restart_old_master_rejoins_as_replica on the SIGPIPE false negative.
  docker logs stale-1 2>&1 \
    | grep -F "peer sentinels contradict this node's sentinel.conf" >/dev/null \
    || { ko "$t" "peer cross-check decision was not logged" stale-1; return; }

  wait_for_link_status stale-1 up 60 \
    || { ko "$t" "rejoined replica never linked to ${promoted}" stale-1 "$promoted"; return; }
  wait_for_key stale-1 postkey postvalue \
    || { ko "$t" "post-failover write never reached the rejoined node" stale-1 "$promoted"; return; }
  wait_for_key stale-1 prekey prevalue 30 \
    || { ko "$t" "pre-failover key missing after the rejoin" stale-1; return; }
  # The promoted master keeps its dataset — the rejoining node syncs from it,
  # never the other way round.
  [ "$(rcli "$promoted" GET postkey)" = "postvalue" ] && [ "$(rcli "$promoted" GET prekey)" = "prevalue" ] \
    || { ko "$t" "promoted master lost data when the stale master rejoined" "$promoted"; return; }

  docker rm -f stale-1 stale-2 stale-3 >/dev/null 2>&1
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
  # Attachment target only (no link-up requirement): the heal under test may
  # legitimately fire the moment its dwell lapses, which races the full
  # sync's own completion — see wait_for_replica_attached_host.
  wait_for_replica_attached_host wrongm-4 wrongm-1 90 \
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
      q=$(scli "$n" SENTINEL master mymaster \
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
      peers=$(scli "$n" SENTINEL master mymaster \
        | grep -A1 "^num-other-sentinels$" | tail -1)
      q=$(scli "$n" SENTINEL master mymaster \
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
#
# The node names live under the reserved `.invalid` TLD so a deleted name is
# NXDOMAIN on ANY resolver: docker's embedded DNS forwards unknown names
# upstream, and what the upstream answers for a deleted bare container name
# is environment luck — GitHub runners' resolver answers SERVFAIL (measured
# via the prune-gate evidence log: `Rcode(2)` continuously), which the
# waiver rightly treats as a possible partition, wedging this scenario on
# CI only. A dotted name under `.invalid` resolves normally through the
# embedded DNS while the container exists, and after deletion the upstream
# forward hits a TLD that does not exist in the root — authoritative
# NXDOMAIN everywhere, which is exactly the deletion semantics Railway's
# own resolver provides in production.
t_deleted_majority_unfences_via_nxdomain() {
  local t=t_deleted_majority_unfences_via_nxdomain
  local fast=(-e QUORUM_SYNC_POLL_SECONDS=2 -e QUORUM_SYNC_DWELL_SECONDS=5 -e SENTINEL_PRUNE_DWELL_SECONDS=10 -e SENTINEL_PRUNE_BACKOFF_SECONDS=5)
  local hosts="gone-1.gone.invalid:26379,gone-2.gone.invalid:26379,gone-3.gone.invalid:26379,gone-4.gone.invalid:26379,gone-5.gone.invalid:26379"
  local i
  for i in 1 2 3 4 5; do mkvol "gone-vol-${i}"; done
  start_node gone-1.gone.invalid gone-vol-1 /data -e SENTINEL_HOSTS="$hosts" "${fast[@]}"
  for i in 2 3 4 5; do
    start_node "gone-${i}.gone.invalid" "gone-vol-${i}" /data \
      -e SENTINEL_HOSTS="$hosts" -e REPLICA_OF=gone-1.gone.invalid:6379 "${fast[@]}"
  done
  wait_for_role_master gone-1.gone.invalid || { ko "$t" "gone-1.gone.invalid never became master" gone-1.gone.invalid; return; }

  # The 5-node fence must be up first, or the test proves nothing. 240s, not
  # 120: five nodes x three processes each converge much slower on a shared
  # CI runner than locally, and this setup gate was the top flake on GH.
  local f
  for i in $(seq 1 240); do
    f=$(rcli gone-1.gone.invalid CONFIG GET min-replicas-to-write | tail -1)
    [ "$f" = "2" ] && break
    sleep 1
  done
  [ "$f" = "2" ] || { ko "$t" "fence never reached 2 before the deletion (got '\''${f}'\'')" gone-1.gone.invalid gone-2.gone.invalid gone-3.gone.invalid gone-4.gone.invalid gone-5.gone.invalid; return; }

  # Delete the majority, DNS names and all.
  docker rm -f gone-3.gone.invalid gone-4.gone.invalid gone-5.gone.invalid >/dev/null 2>&1
  docker volume rm -f gone-vol-3 gone-vol-4 gone-vol-5 >/dev/null 2>&1

  # One acking replica < fence 2: the master must reject writes first —
  # this is the wedge the waiver exists to resolve.
  local fenced=""
  for i in $(seq 1 60); do
    rcli gone-1.gone.invalid SET gonekey gonevalue | grep -q NOREPLICAS && { fenced=1; break; }
    sleep 1
  done
  [ -n "$fenced" ] || { ko "$t" "master never fenced after losing the majority" gone-1.gone.invalid; return; }

  # Both survivors independently: serve the sdown dwell, observe continuous
  # NXDOMAIN for the deleted three, waive the majority gate, RESET, and the
  # fence follows the shrunken membership back to 1. Writes resume.
  local n converged
  for n in gone-1.gone.invalid gone-2.gone.invalid; do
    wait_for_log_line "$n" "reset the local sentinel to forget peers down past the dwell" 240 \
      || { ko "$t" "${n} never pruned the deleted majority" gone-1.gone.invalid gone-2.gone.invalid; return; }
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
    [ "$(rcli gone-1.gone.invalid SET gonekey gonevalue)" = "OK" ] && { write_ok=1; break; }
    sleep 1
  done
  [ -n "$write_ok" ] || { ko "$t" "writes never resumed after the fence shrank" gone-1.gone.invalid; return; }

  docker rm -f gone-1.gone.invalid gone-2.gone.invalid >/dev/null 2>&1
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

  # Same 240s setup gate as t_deleted_majority_unfences_via_nxdomain: the
  # 5-node convergence is CPU-bound on a shared CI runner.
  local f
  for i in $(seq 1 240); do
    f=$(rcli hold-1 CONFIG GET min-replicas-to-write | tail -1)
    [ "$f" = "2" ] && break
    sleep 1
  done
  [ "$f" = "2" ] || { ko "$t" "fence never reached 2 before the pause (got '\''${f}'\'')" hold-1 hold-2 hold-3 hold-4 hold-5; return; }

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

  if docker logs hold-1 2>&1 | grep "reset the local sentinel to forget peers down past the dwell" >/dev/null; then
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

# Whether a volume's sentinel.conf carries a non-empty requirepass. Matches
# both the wrapper's `requirepass <pw>` and Sentinel's own rewrite form
# (`requirepass "<pw>"`).
sentinel_conf_requires_auth() { # sentinel_conf_requires_auth VOLUME
  docker run --rm -v "$1:/v" alpine:latest sh -c \
    'grep -i "^requirepass " /v/sentinel.conf' 2>/dev/null | grep -vq '""'
}

# Sentinel auth, default ON for fresh clusters, reusing REDIS_PASSWORD as
# the sentinel password — nothing extra stamped by the platform. A trio
# started with the exact env the template stamps must come up AUTHED on
# every node (no mixed posture anywhere), refuse the unauthenticated
# `SENTINEL SET/RESET/FAILOVER/REMOVE` the auth exists to close off, accept
# the authenticated equivalent, and still fail over end-to-end — a lock
# that also locks out this image's own watchers (or the peers' votes) would
# be worse than the gap it closes.
t_sentinel_auth_on_by_default_for_fresh_cluster() {
  local t=t_sentinel_auth_on_by_default_for_fresh_cluster
  start_ha_trio authdef \
    || { dump_sentinel_view authdef-2 authdef-3
         ko "$t" "default-authed cluster never became failover-ready" authdef-1 authdef-2 authdef-3
         return; }

  # The env-primary booted with no peers up: the decision must be the
  # default-on arm, and every node's conf must carry requirepass — the
  # posture-consistency invariant a fresh cluster gets for free.
  docker logs authdef-1 2>&1 | grep -F "fresh cluster, auth on by default" >/dev/null \
    || { ko "$t" "authdef-1 did not log the default-on decision" authdef-1; return; }
  local v
  for v in authdef-vol-1 authdef-vol-2 authdef-vol-3; do
    sentinel_conf_requires_auth "$v" \
      || { ko "$t" "${v} sentinel.conf carries no requirepass — auth was not on by default" authdef-1 authdef-2 authdef-3; return; }
  done

  # Replication over the authenticated sentinel mesh — the internal clients
  # (peer boot query, quorum-sync, link-heal, health server) all reaching
  # the co-located and peer Sentinels.
  write_key authdef-1 authkey authvalue \
    || { ko "$t" "master never accepted the write with default auth on" authdef-1; return; }
  wait_for_key authdef-2 authkey authvalue || { ko "$t" "authdef-2 never synced" authdef-1 authdef-2; return; }
  wait_for_key authdef-3 authkey authvalue || { ko "$t" "authdef-3 never synced" authdef-1 authdef-3; return; }

  # The actual gap: full cluster control without credentials. An
  # unauthenticated SENTINEL SET must be refused...
  local unauthed
  unauthed=$(docker exec authdef-1 redis-cli -p 26379 SENTINEL SET mymaster quorum 1 2>&1)
  case "$unauthed" in
    *NOAUTH*) ;;
    *) ko "$t" "unauthenticated SENTINEL SET was not refused: ${unauthed}" authdef-1; return ;;
  esac

  # ...while the REDIS_PASSWORD-authenticated call — what every internal
  # client in this image sends — still succeeds.
  local authed
  authed=$(docker exec authdef-1 redis-cli -p 26379 -a "$PW" --no-auth-warning \
    SENTINEL SET mymaster quorum 2 2>&1)
  [ "$authed" = "OK" ] \
    || { ko "$t" "REDIS_PASSWORD-authenticated SENTINEL SET failed: ${authed}" authdef-1; return; }

  # Failover end-to-end with auth on.
  local promoted
  promoted=$(promote_by_pausing authdef-1 authdef-2 authdef-3) || {
    dump_sentinel_view authdef-2 authdef-3
    ko "$t" "no replica was promoted after pausing the authenticated master" authdef-2 authdef-3
    return
  }
  [ "$(rcli "$promoted" GET authkey)" = "authvalue" ] \
    || { ko "$t" "key lost across failover with auth on (promoted=${promoted})" "$promoted"; return; }
  note "promoted: ${promoted}"

  docker unpause authdef-1 >/dev/null 2>&1
  docker rm -f authdef-1 authdef-2 authdef-3 >/dev/null 2>&1
  ok "$t"
}

# The hazard the posture probe exists for: requirepass is decided once, at
# the boot that generates sentinel.conf, and can never be retrofitted at
# runtime (`SENTINEL CONFIG SET requirepass` -> "Invalid argument"). So a
# node scaled up onto an existing UNAUTHENTICATED cluster must boot
# unauthenticated too — a default-on first boot would otherwise mint the
# one sentinel that rejects every peer's credential-less failover RPCs
# while the peers hard-fail its authed ones: partitioned out of failover
# authorization while looking healthy on the data port. The founding trio
# runs with SENTINEL_AUTH=false, leaving exactly the on-disk state of a
# cluster that predates sentinel auth (a sentinel.conf with no
# requirepass); the scale-up node runs with stock env (auth default ON) and
# must match the open posture it probes.
t_scale_up_of_unauthed_cluster_stays_unauthed() {
  local t=t_scale_up_of_unauthed_cluster_stays_unauthed
  local hosts="scaleup-1:26379,scaleup-2:26379,scaleup-3:26379"
  start_ha_trio scaleup -e SENTINEL_AUTH=false \
    || { dump_sentinel_view scaleup-2 scaleup-3
         ko "$t" "kill-switched trio never became failover-ready" scaleup-1 scaleup-2 scaleup-3
         return; }
  # The kill switch must reproduce the pre-auth behavior exactly: an open
  # sentinel port.
  docker exec scaleup-1 redis-cli -p 26379 PING 2>/dev/null | grep -q PONG \
    || { ko "$t" "SENTINEL_AUTH=false did not leave the sentinel port open" scaleup-1; return; }

  write_key scaleup-1 upkey upvalue \
    || { ko "$t" "master never accepted the pre-scale-up write" scaleup-1; return; }

  # The scale-up: fresh volume, stock env (no SENTINEL_AUTH — the default),
  # deploy-time topology naming the founding master.
  mkvol scaleup-vol-4
  start_node scaleup-4 scaleup-vol-4 /data \
    -e SENTINEL_HOSTS="$hosts" -e REPLICA_OF=scaleup-1:6379
  wait_for_log_line scaleup-4 "matching the cluster's open posture" 30 \
    || { ko "$t" "scale-up node never posture-matched the open cluster" scaleup-4; return; }
  wait_for_file_in_volume scaleup-vol-4 sentinel.conf 30 \
    || { ko "$t" "scale-up node never wrote sentinel.conf" scaleup-4; return; }
  sentinel_conf_requires_auth scaleup-vol-4 \
    && { ko "$t" "scale-up node wrote requirepass into an unauthenticated cluster — mixed auth" scaleup-4; return; }
  # Retried: the conf lands before the sentinel process starts listening.
  local i sentinel_open=""
  for i in $(seq 1 30); do
    docker exec scaleup-4 redis-cli -p 26379 PING 2>/dev/null | grep -q PONG \
      && { sentinel_open=1; break; }
    sleep 1
  done
  [ -n "$sentinel_open" ] \
    || { ko "$t" "scale-up node's sentinel is not open like its peers" scaleup-4; return; }
  wait_for_key scaleup-4 upkey upvalue \
    || { ko "$t" "scale-up node never synced the dataset" scaleup-1 scaleup-4; return; }

  # Failover with the scale-up node's vote REQUIRED: 4 known sentinels mean
  # the leader election needs 3 votes, and pausing the master leaves
  # exactly 3 alive — scaleup-4 must be one of them. A mixed-auth node
  # would make this promotion impossible, so converging here IS the proof
  # that no auth line divides the cluster.
  local n
  for n in scaleup-2 scaleup-3 scaleup-4; do
    wait_for_sentinel_peers "$n" 3 || { ko "$t" "${n} never saw all 3 peers" "$n"; return; }
  done
  wait_for_sentinel_slave_view scaleup-2 3 \
    || { dump_sentinel_view scaleup-2; ko "$t" "scaleup-2 never got a live view of all replicas" scaleup-2; return; }
  wait_for_sentinel_slave_view scaleup-3 3 \
    || { dump_sentinel_view scaleup-3; ko "$t" "scaleup-3 never got a live view of all replicas" scaleup-3; return; }
  local promoted
  promoted=$(promote_by_pausing scaleup-1 scaleup-2 scaleup-3 scaleup-4) || {
    dump_sentinel_view scaleup-2 scaleup-3 scaleup-4
    ko "$t" "no replica was promoted — the scale-up node's vote was needed and missing" scaleup-2 scaleup-3 scaleup-4
    return
  }
  [ "$(rcli "$promoted" GET upkey)" = "upvalue" ] \
    || { ko "$t" "key lost across the post-scale-up failover (promoted=${promoted})" "$promoted"; return; }
  note "promoted: ${promoted}"

  docker unpause scaleup-1 >/dev/null 2>&1
  docker rm -f scaleup-1 scaleup-2 scaleup-3 scaleup-4 >/dev/null 2>&1
  ok "$t"
}

# Editing REDIS_PASSWORD and redeploying must NOT rotate the password the
# dataset already runs with. The platform's variable editor warns exactly
# that ("changes the variable without updating the actual database
# password"), and half-applying the edit is the worst outcome: redis.conf is
# regenerated from env while sentinel.conf is first-boot-only, so a rotated
# requirepass strands Sentinel's outbound auth-pass and every wrapper watcher
# on the old password — a full write outage through /role 503 on every node,
# with Redis itself healthy. The wrapper pins the active password from the
# volume's own previous conf instead; an orchestrated rotation that goes
# through CONFIG SET requirepass + CONFIG REWRITE updates that conf and is
# honored on the next boot.
t_password_variable_edit_does_not_rotate() {
  local t=t_password_variable_edit_does_not_rotate n=pin-1
  mkvol pin-vol
  start_node "$n" pin-vol /data
  wait_for_role_master "$n" || { ko "$t" "never became master on first boot" "$n"; return; }

  docker rm -f "$n" >/dev/null 2>&1
  # start_node stamps -e REDIS_PASSWORD="$PW" first; the extra -e appended
  # here wins (docker takes the last occurrence) — this IS the redeploy
  # after a Variables-tab edit.
  start_node "$n" pin-vol /data -e REDIS_PASSWORD=rotated-by-variable-edit
  wait_for_role_master "$n" 90 \
    || { ko "$t" "never became master after the variable-edit reboot" "$n"; return; }

  # The active password still authenticates...
  rcli "$n" PING | grep -q PONG \
    || { ko "$t" "active password no longer authenticates after the reboot" "$n"; return; }
  # ...the edited variable's value does not...
  docker exec "$n" redis-cli -a rotated-by-variable-edit --no-auth-warning PING 2>/dev/null \
    | grep -q PONG \
    && { ko "$t" "the edited variable value authenticated — the password rotated" "$n"; return; }
  # ...and the wrapper said why, durably.
  wait_for_log_line "$n" "variable edits do not rotate the database password" 10 \
    || { ko "$t" "drift warning never logged" "$n"; return; }

  docker rm -f "$n" >/dev/null 2>&1
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

# The empty-master wipe: the env-primary comes back on a wiped/replaced
# volume before any failover happened, so every peer sentinel still names it
# master. The env fallback would boot it as an EMPTY master; its replicas
# were never repointed, so they reconnect, ack (min-replicas-to-write is
# satisfied — the fence does not protect here) and full-resync the empty
# dataset: the whole cluster's data gone. The boot guard must refuse — exit
# before redis ever listens, volume untouched — so Sentinel fails over to a
# data-bearing replica, and the wiped node's next boot joins it as a
# replica through the peer query.
t_wiped_master_volume_does_not_wipe_cluster() {
  local t=t_wiped_master_volume_does_not_wipe_cluster
  local hosts="wipe-1:26379,wipe-2:26379,wipe-3:26379"
  # A longer down-after keeps the peers naming wipe-1 master while the wiped
  # boot runs its peer query — the real trigger (a redeploy replacing the
  # volume) has the same shape: the node is back well before sdown.
  local slow=(-e SENTINEL_DOWN_AFTER_MS=15000)
  start_ha_trio wipe "${slow[@]}" \
    || { dump_sentinel_view wipe-2 wipe-3
         ko "$t" "cluster never became failover-ready" wipe-1 wipe-2 wipe-3; return; }
  write_key wipe-1 wipekey wipevalue \
    || { ko "$t" "master never accepted the marker write" wipe-1; return; }
  wait_for_key wipe-2 wipekey wipevalue || { ko "$t" "wipe-2 never synced" wipe-1 wipe-2; return; }
  wait_for_key wipe-3 wipekey wipevalue || { ko "$t" "wipe-3 never synced" wipe-1 wipe-3; return; }

  # The volume replacement: same service, same deploy-time env (REPLICA_OF
  # still empty), nothing left on the volume.
  docker rm -f wipe-1 >/dev/null 2>&1
  docker run --rm -v wipe-vol-1:/v alpine:latest sh -c \
    'rm -rf /v/* /v/.[!.]* /v/..?* 2>/dev/null; chown 999:999 /v' >/dev/null 2>&1
  start_node wipe-1 wipe-vol-1 /data -e SENTINEL_HOSTS="$hosts" "${slow[@]}"

  wait_for_log_line wipe-1 "refusing to boot as an empty master" 30 \
    || { ko "$t" "guard never refused the empty-master boot" wipe-1; return; }
  local i state=""
  for i in $(seq 1 30); do
    state=$(docker inspect -f '{{.State.Status}}:{{.State.ExitCode}}' wipe-1 2>/dev/null)
    [ "$state" = "exited:1" ] && break
    sleep 1
  done
  [ "$state" = "exited:1" ] \
    || { ko "$t" "guard logged but the container did not exit(1) (state=${state})" wipe-1; return; }
  # Fail-stop means hands off the dataset: nothing written, nothing
  # quarantined. The one allowed artifact is the empty volume runtime lock
  # file — the wrapper must hold the flock before it may even read the
  # volume to decide the refusal, and a flock file is never unlinked on
  # exit (recreating it would reopen the overlap race it exists to close).
  local leftover
  leftover=$(docker run --rm -v wipe-vol-1:/v alpine:latest sh -c 'ls -A /v' 2>/dev/null \
    | grep -v '^\.railway-redis-runtime\.lock$')
  [ -z "$leftover" ] \
    || { ko "$t" "the refused boot wrote to the wiped volume: ${leftover}" wipe-1; return; }
  docker run --rm -v wipe-vol-1:/v alpine:latest sh -c \
      '[ ! -s /v/.railway-redis-runtime.lock ]' 2>/dev/null \
    || { ko "$t" "the refused boot wrote data into the runtime lock file" wipe-1; return; }

  # An exited container drops its DNS record and Sentinel spins on NXDOMAIN
  # instead of counting the node down (see t_sentinel_failover on kill vs
  # pause). A crashlooping Railway service keeps its private domain, so hold
  # the name with an idle container while the failover runs.
  docker run -d --name wipe-1-dns --label "$LABEL" --network "$NET" \
    --network-alias wipe-1 alpine:latest sleep 600 >/dev/null

  local promoted="" n
  for i in $(seq 1 180); do
    for n in wipe-2 wipe-3; do
      docker exec "$n" sh -c 'wget -qO- http://127.0.0.1:8080/role' 2>/dev/null \
        | grep -q '"role":"master"' && { promoted="$n"; break 2; }
    done
    sleep 1
  done
  [ -n "$promoted" ] || {
    dump_sentinel_view wipe-2 wipe-3
    ko "$t" "no replica was promoted after the wiped master refused to boot" wipe-2 wipe-3
    return
  }
  note "promoted: ${promoted}"
  [ "$(rcli "$promoted" GET wipekey)" = "wipevalue" ] \
    || { ko "$t" "marker data lost on the promoted master" "$promoted"; return; }

  # The next boot of the wiped node (Railway's restart policy; same env,
  # same empty volume): the peers now name the promoted node, so it joins
  # as a replica and resyncs the dataset — the normal first-boot path.
  docker rm -f wipe-1-dns >/dev/null 2>&1
  docker start wipe-1 >/dev/null 2>&1
  wait_for_ping wipe-1 || { ko "$t" "wiped node never came back as a replica" wipe-1; return; }
  wait_for_replica_repointed wipe-1 "$promoted" 90 \
    || { ko "$t" "wiped node never attached to ${promoted}" wipe-1 "$promoted"; return; }
  wait_for_key wipe-1 wipekey wipevalue \
    || { ko "$t" "marker data never resynced to the rejoined node" wipe-1 "$promoted"; return; }
  [ "$(rcli "$promoted" GET wipekey)" = "wipevalue" ] \
    || { ko "$t" "promoted master lost data when the wiped node rejoined" "$promoted"; return; }

  docker rm -f wipe-1 wipe-2 wipe-3 >/dev/null 2>&1
  ok "$t"
}

# maxmemory pressure at runtime must degrade WRITES, not the cluster. The
# wrapper stamps `maxmemory` (MAXMEMORY_MB, or 75% of the cgroup limit) with
# `maxmemory-policy noeviction` — so a full node rejects writes with -OOM
# instead of silently evicting. What must NOT happen when the ceiling is hit:
# no failover (a loaded master is not a dead master — +odown/+switch-master
# appearing here would mean pressure alone tips the cluster over), no broken
# replication (replicas ignore maxmemory for the master's stream by Redis
# default and must stay linked), no broken reads, and no broken persistence
# (a BGSAVE that starts failing under pressure escalates to MISCONF, which
# rejects even the writes that would free memory). And it must be a state,
# not a ratchet: freeing memory has to restore writes with no restart.
t_maxmemory_pressure_keeps_cluster_stable() {
  local t=t_maxmemory_pressure_keeps_cluster_stable
  start_ha_trio mm -e MAXMEMORY_MB=24 \
    || { ko "$t" "trio never reached steady state" mm-1 mm-2 mm-3; return; }
  [ "$(rcli mm-1 CONFIG GET maxmemory | tail -1)" = "25165824" ] \
    || { ko "$t" "maxmemory was not stamped from MAXMEMORY_MB" mm-1; return; }
  [ "$(rcli mm-1 CONFIG GET maxmemory-policy | tail -1)" = "noeviction" ] \
    || { ko "$t" "maxmemory-policy is not noeviction" mm-1; return; }

  # Fill past the ceiling: 40k × 1KiB values ≈ 40MiB attempted against a
  # 24MiB ceiling, pushed through one in-container `redis-cli --pipe` (the
  # fill itself is not the thing under test). --pipe exits non-zero once the
  # ceiling starts rejecting — expected, that IS the pressure.
  docker exec -e PW="$PW" mm-1 sh -c '
    pad=$(head -c 1024 /dev/zero | tr "\0" "x")
    for i in $(seq 1 40000); do echo "SET pad:$i $pad"; done \
      | redis-cli -a "$PW" --pipe' >/dev/null 2>&1 || true

  # The ceiling is enforced: a plain write is refused with -OOM.
  rcli mm-1 SET mmprobe v 2>&1 | grep -qi "OOM" \
    || { ko "$t" "write at the ceiling was not refused with -OOM" mm-1; return; }

  # Reads keep working on the full node.
  [ -n "$(rcli mm-1 GET pad:1)" ] \
    || { ko "$t" "read failed on the full node" mm-1; return; }

  # Persistence keeps working under pressure: BGSAVE completes with status ok
  # (its failure mode, MISCONF, would lock out even the freeing writes).
  rcli mm-1 BGSAVE >/dev/null 2>&1
  local i
  for i in $(seq 1 60); do
    rcli mm-1 INFO persistence | grep -q "rdb_bgsave_in_progress:0" && break
    sleep 1
  done
  rcli mm-1 INFO persistence | grep -q "rdb_last_bgsave_status:ok" \
    || { ko "$t" "BGSAVE failed under maxmemory pressure" mm-1; return; }

  # Hold the pressure and prove the cluster does NOT react to it: master
  # keeps its role, both replication links stay up, and no Sentinel ever
  # escalates past subjective suspicion (a +sdown blip under load is
  # tolerated; +odown or +switch-master is a failover and fails the test).
  sleep 20
  [ "$(redis_role mm-1)" = "master" ] \
    || { ko "$t" "master lost its role under memory pressure" mm-1 mm-2 mm-3; return; }
  [ "$(link_status mm-2)" = "up" ] && [ "$(link_status mm-3)" = "up" ] \
    || { ko "$t" "replication link broke under memory pressure" mm-1 mm-2 mm-3; return; }
  local escalations
  # Not `grep -q` — see t_restart_old_master_rejoins_as_replica on the
  # SIGPIPE false negative; read both logs to completion.
  escalations=$(docker logs mm-2 2>&1 | grep -c -e "+odown" -e "+switch-master")
  [ "$escalations" = "0" ] \
    || { ko "$t" "memory pressure alone escalated to a failover on mm-2's sentinel" mm-1 mm-2; return; }
  escalations=$(docker logs mm-3 2>&1 | grep -c -e "+odown" -e "+switch-master")
  [ "$escalations" = "0" ] \
    || { ko "$t" "memory pressure alone escalated to a failover on mm-3's sentinel" mm-1 mm-3; return; }

  # Recoverable without a restart: free the data, writes come back, and the
  # recovery write replicates.
  docker exec -e PW="$PW" mm-1 sh -c '
    for i in $(seq 1 40000); do echo "UNLINK pad:$i"; done \
      | redis-cli -a "$PW" --pipe' >/dev/null 2>&1 || true
  write_key mm-1 mmafter recovered 30 \
    || { ko "$t" "writes never recovered after freeing memory" mm-1; return; }
  wait_for_key mm-2 mmafter recovered \
    || { ko "$t" "post-recovery write never replicated" mm-1 mm-2; return; }

  docker rm -f mm-1 mm-2 mm-3 >/dev/null 2>&1
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
  t_conversion_under_active_writes
  t_sentinel_failover
  t_edge_client_writes_survive_failover
  t_switchover_promotes_requested_node
  t_sigterm_master_demotes_before_exit
  t_restart_old_master_rejoins_as_replica
  t_down_for_failover_master_boots_as_replica
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
  t_wiped_master_volume_does_not_wipe_cluster
  t_sentinel_auth_on_by_default_for_fresh_cluster
  t_scale_up_of_unauthed_cluster_stays_unauthed
  t_password_variable_edit_does_not_rotate
  t_maxmemory_pressure_keeps_cluster_stable
)

setup
RUNLIST=("${@:-${ALL_TESTS[@]}}")
for t in "${RUNLIST[@]}"; do
  log "running ${t}"
  fail_before=$FAIL
  "$t"
  # A ko returns without the scenario's own cleanup, and the stragglers (up
  # to 5 nodes x redis+sentinel+wrapper each) then starve every later
  # scenario of CPU — one flake cascades into a 16-fail run whose root cause
  # is a single scenario. Sweep after a FAILED scenario so the next one
  # starts clean; a passing scenario's leftovers stay untouched, because
  # some chain on purpose (t_adoption_survives_restart reuses what
  # t_rdb_adoption leaves behind).
  if [ "$FAIL" -gt "$fail_before" ]; then
    cleanup_test_resources
  fi
done

echo
log "passed: ${PASS}  failed: ${FAIL}"
[ "$FAIL" -gt 0 ] && log "failed tests: ${FAILED_TESTS[*]}"
exit "$FAIL"
