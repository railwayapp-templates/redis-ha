//! Trigger a Sentinel-driven failover BEFORE a master's `redis-server` is
//! signaled to stop, so a *planned* shutdown (redeploy, restart, manual
//! scale) pays a triggered-failover cost instead of the timeout-failover
//! cost Sentinel would otherwise pay once it merely notices the master is
//! gone.
//!
//! ## The gap this closes
//! Without this, every redeploy of the master is an *unplanned* failover as
//! far as the other Sentinels are concerned: SIGTERM kills redis, and only
//! then do they notice (down-after, default 5s), elect a leader, and
//! promote — a multi-second write blackout, on top of whatever writes were
//! in flight when the process died and the resync every replica now has to
//! do against the new master. Patroni does the analogous demote-on-stop for
//! postgres-ha; the write-ups on Sentinel failover latency in the wild put a
//! triggered failover around ~0.8s against a timeout failover around ~5.8s
//! (Vinted engineering) — the gap this module is closing.
//!
//! ## Where this runs
//! Called from `process_manager::supervise`'s SIGTERM/SIGINT arms, strictly
//! BEFORE `graceful_shutdown` signals either child, so the local Sentinel is
//! still up to drive the failover it is about to be asked to run. Only
//! meaningful when Sentinel is colocated — the caller gates on
//! `sentinel.is_some()`; a standalone Redis has no Sentinel to ask and
//! nothing to gain here. Sentinel itself is only ever signaled by
//! `graceful_shutdown`, unchanged and still after Redis's own demote wait —
//! this module runs entirely before that sequence starts.
//!
//! ## Sequence
//! 1. Ask local Redis (`redis://:<password>@127.0.0.1:<port>`, timeout-
//!    bounded) `INFO replication`. Not master → return immediately, so a
//!    replica's shutdown does no further work than today's (this one
//!    fast local round trip is the only difference).
//! 2. Best-effort `CLIENT PAUSE <ms> WRITE` on the local Redis, sized to
//!    the failover window this sequence can occupy (the confirmation
//!    deadline, capped at [`MAX_WRITE_PAUSE`]). Every write a
//!    direct-connected client lands on this master between the failover
//!    request and the promotion dies with the node — the same lost-write
//!    tail the docs' manual-failover recipe closes with exactly this
//!    command. The window was already fence-bounded (/role goes 503,
//!    HAProxy's shutdown-sessions cuts routed clients over in ~3.5s,
//!    min-replicas-to-write fences a partitioned master); the pause closes
//!    what those cannot see: clients holding their own direct connection
//!    to this node. Failure is a warn, never a blocker — the pause is an
//!    optimization on top of the fences, not a correctness gate.
//! 3. `SENTINEL FAILOVER <master name>` against the LOCAL Sentinel only
//!    (`127.0.0.1:<sentinel_port>`, AUTHed iff the local sentinel.conf
//!    carries `requirepass` — the same file-resolved
//!    `local_sentinel_password` every other local watcher uses).
//!    Per the Sentinel command reference: *"Force a failover as if the
//!    master was not reachable, and without asking for agreement to other
//!    Sentinels (however a new version of the configuration will be
//!    published so that the other Sentinels will update their
//!    configurations)."* (<https://redis.io/docs/latest/operate/oss_and_stack/management/sentinel/#sentinel-api>,
//!    `SENTINEL FAILOVER` entry). So this neither needs nor waits on any
//!    other Sentinel's agreement — only the local one has to be up. Any
//!    error (`-NOGOODSLAVE`, `-INPROG`, the local Sentinel unreachable) is
//!    logged at `warn` and shutdown proceeds unchanged: a failed demote
//!    request must never block or slow down the shutdown it was trying to
//!    speed up.
//! 4. Poll (every [`POLL_INTERVAL`], each call independently timeout-
//!    bounded) `SENTINEL MASTER <master name>` on the local Sentinel until
//!    BOTH of these hold: its `flags` no longer carry
//!    `failover_in_progress`, AND its `ip`/`port` (or this node's own `INFO
//!    replication`) name a node other than this one. Bounded overall by
//!    [`DEMOTE_TIMEOUT_ENV`] (default [`DEFAULT_TIMEOUT_MS`]); on timeout,
//!    logged at `warn` and shutdown proceeds unchanged.
//!
//!    Both signals are required, not just the address change. This node's
//!    own Sentinel forced itself leader (that is what step 3 asked for), so
//!    it — not some other survivor — is the one running the ENTIRE failover
//!    state machine, including `failover-state-reconf-slaves`: sequentially
//!    telling every OTHER known replica to attach to the winner. That step
//!    runs strictly after the winner is already selected and promoted, i.e.
//!    after the address has already changed and this node may already read
//!    `role:slave`. Killing this node's Sentinel (which `graceful_shutdown`
//!    does right after this function returns) the moment the address flips
//!    — without waiting for `failover_in_progress` to clear — cuts that
//!    reconfiguration short: any replica not yet reached is left pointed at
//!    the master that is about to disappear, with no fix until Sentinel's
//!    own slower `+fix-slave-config` housekeeping eventually notices. This
//!    was caught empirically (a 3-node manual run left the third node
//!    retrying a dead master for ~30s) — the address-only check alone is
//!    necessary but not sufficient. `failover_in_progress` clearing (success
//!    OR abort) is Sentinel's own "the whole state machine is done" signal,
//!    the same flag `quorum::master_is_healthy` already reads for a related
//!    reason. Checking it ALONE is not enough either: an aborted failover
//!    (e.g. `-NOGOODSLAVE`) also clears the flag while this node stays
//!    master, so both must hold together.
//! 5. Best-effort `REPLICAOF <new_host> <new_port>` on the local Redis, at
//!    whoever Sentinel last named as master — after a confirmed demote, and
//!    on the timeout arm too whenever that answer already names another
//!    node. The confirmation deliberately trusts Sentinel's answer while
//!    the local Redis may still report `role:master` (the two signals land
//!    in either order — see `switched_away`), so between confirmation and
//!    the SIGTERM that follows, a direct-connected client can still land
//!    writes here that die with the node. `REPLICAOF` is cheap and
//!    non-destructive (see `link_heal`'s backoff rationale) and flips this
//!    node read-only immediately; when Sentinel's own reconfiguration
//!    already landed it is a no-op re-statement of the same attachment.
//!    Sentinel still naming this node, or never answering, means there is
//!    nothing safe to point at — skipped. Warn-on-failure, never a blocker:
//!    the confirmation semantics above are unchanged, this only narrows
//!    their deliberate tail.
//!
//! ## Budget vs. the existing shutdown waits
//! Railway's SIGTERM grace window is ~30s. `graceful_shutdown` already
//! spends up to 10s waiting on Sentinel to exit and up to 30s waiting on
//! Redis to exit — both unchanged by this module. The demote sequence here
//! runs entirely BEFORE either of those waits starts, and its own default
//! 10s budget is deliberately sized to fit ahead of them rather than compete
//! with them: this module must never, and does not, raise either existing
//! wait.
//!
//! ## Kill switch
//! `DEMOTE_ON_SHUTDOWN` — same convention as `boot_role::enabled` /
//! `link_heal`'s `LINK_HEAL_DISABLED`: only the literal `false` (trimmed,
//! case-insensitive) disables the behavior. Unset, empty, or garbage leaves
//! it on.
//!
//! ## An operational nuance this leans on, not something it can fix
//! `SENTINEL FAILOVER` makes THIS node's own Sentinel the failover leader,
//! which is different from an ordinary unplanned failover: there, the dying
//! master's Sentinel is never the leader (it is the thing dying), so only
//! the survivors' knowledge of each other's replicas matters. Here, the
//! leader IS the node about to disappear, so its own knowledge of every
//! OTHER replica — learned from the master's own `INFO` on Sentinel's
//! periodic refresh, a few seconds by default, not from this module — has
//! to be converged before the failover starts, or `reconf-slaves` can only
//! repoint the replicas it already knew about; the rest sit pointed at the
//! disappearing master until Sentinel's own slower `+fix-slave-config`
//! housekeeping eventually catches them. This surfaced empirically (see the
//! demote-on-shutdown PR description) and is why `test/e2e.sh`'s
//! `t_sigterm_master_demotes_before_exit` waits for the master's own
//! Sentinel to have a live view of every replica before triggering the
//! stop — the same convergence a real cluster reaches steady-state minutes
//! before anyone redeploys it, but worth calling out because nothing in
//! this module can observe or wait on that leader-side knowledge directly.

use crate::boot_role::normalize_host;
use crate::quorum::field_value;
use crate::sentinel_query::{connect, get_master_fields};
use common::{ConfigExt, RailwayEnv};
use std::env;
use std::time::{Duration, Instant};
use tokio::time::timeout;
use tracing::{info, warn};

/// Operator kill switch. Only the literal `false` disables the behavior.
pub const DEMOTE_ON_SHUTDOWN_ENV: &str = "DEMOTE_ON_SHUTDOWN";

/// Overall deadline for step 4 (the confirmation poll), in milliseconds.
pub const DEMOTE_TIMEOUT_ENV: &str = "DEMOTE_ON_SHUTDOWN_TIMEOUT_MS";

const DEFAULT_TIMEOUT_MS: u64 = 10_000;

/// Interval between confirmation polls.
const POLL_INTERVAL: Duration = Duration::from_millis(250);

/// Per-RPC bound (connect + command), independent of the overall poll
/// deadline — a single hung call must never eat more than this.
const CALL_DEADLINE: Duration = Duration::from_secs(1);

/// Upper bound on the best-effort `CLIENT PAUSE ... WRITE` issued before
/// the failover request (module doc, step 2). The pause tracks the
/// confirmation deadline — the window this sequence can actually occupy —
/// but an operator-raised [`DEMOTE_TIMEOUT_ENV`] must not translate into an
/// arbitrarily long client-visible write stall, so it is capped at the same
/// ~30s scale as Railway's SIGTERM grace window: past that the node is
/// exiting anyway.
const MAX_WRITE_PAUSE: Duration = Duration::from_secs(30);

/// What the demote-before-shutdown sequence needs, plumbed in from `Config`
/// at spawn time. A struct beats five loose parameters threaded through
/// `supervise` -> `demote_before_shutdown`.
#[derive(Debug, Clone)]
pub struct DemoteTarget {
    pub redis_port: u16,
    pub redis_password: String,
    pub sentinel_port: u16,
    pub redis_master_name: String,
    pub sentinel_enabled: bool,
    /// What gets this shutdown path past the co-located Sentinel's front
    /// door: `""` unless the on-disk sentinel.conf carries `requirepass`,
    /// resolved by the wrapper from the file exactly like link-heal's and
    /// quorum-sync's (see `sentinel_conf::conf_requires_auth`).
    pub local_sentinel_password: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Role {
    Master,
    Replica,
    Unknown,
}

/// Parse just the `role:` line out of `INFO replication`. Mirrors
/// `link_heal::parse_replication_info`'s line-based, CRLF-tolerant approach
/// as a model, but duplicated rather than shared: this caller needs only the
/// one field, the same "parse locally what this caller needs" convention
/// `process_manager::aof_status_from_info` already follows for `INFO
/// persistence`.
fn parse_role(info: &str) -> Role {
    for line in info.lines() {
        if let Some(v) = line.trim_end().strip_prefix("role:") {
            return match v {
                "master" => Role::Master,
                "slave" => Role::Replica,
                _ => Role::Unknown,
            };
        }
    }
    Role::Unknown
}

/// Kill switch semantics, split out from the environment so it's testable.
/// Only the literal `false` (trimmed, case-insensitive) turns the behavior
/// off — same convention as `boot_role::enabled`.
fn enabled(raw: Option<&str>) -> bool {
    !matches!(raw.map(|v| v.trim().to_ascii_lowercase()), Some(v) if v == "false")
}

/// Whether the demote-before-shutdown sequence should run at all. Pure so
/// the gating logic is unit-tested independently of any I/O: Sentinel must
/// be both configured and actually colocated (nothing to fail over to
/// otherwise), and the kill switch must not be set to the literal `false`.
pub(crate) fn should_run(
    sentinel_enabled: bool,
    sentinel_colocated: bool,
    kill_switch_raw: Option<&str>,
) -> bool {
    sentinel_enabled && sentinel_colocated && enabled(kill_switch_raw)
}

/// Whether the shutdown path should attempt a demote at all, given this
/// node's own role. Only ever on a master — a replica has no failover to
/// trigger, and returning `false` here is what keeps a replica's shutdown at
/// today's cost (this decision plus the one INFO call needed to make it).
pub(crate) fn should_attempt_demote(role: Role) -> bool {
    role == Role::Master
}

/// Whether Sentinel's own bookkeeping considers the failover attempt
/// finished — success OR abort, either way "no longer running". `None`
/// (Sentinel unreachable, or a reply with no `flags` field) is never treated
/// as finished — an indeterminate state must not look like a green light,
/// the same principle `link_heal::attached_to_wrong_master` applies to a
/// `None` on either side of its comparison.
pub(crate) fn failover_finished(flags: Option<&str>) -> bool {
    match flags {
        Some(flags) => !flags.split(',').any(|f| f == "failover_in_progress"),
        None => false,
    }
}

/// Whether the master has actually moved off this node: this node's own
/// `INFO replication` reads `role:slave`, OR Sentinel's current master
/// address names a different host/port than this node — whichever
/// observation lands first. Hosts are compared normalized (case, trailing
/// root dot) the same way `link_heal::attached_to_wrong_master` and
/// `boot_role::addr_is_self` do; the port has to match too, since the same
/// host on a different port is a different Redis instance.
///
/// Necessary but not sufficient on its own — see [`demote_confirmed`].
fn switched_away(
    own_host: &str,
    own_port: u16,
    master_addr: Option<(&str, u16)>,
    local_role: Role,
) -> bool {
    if local_role == Role::Replica {
        return true;
    }
    match master_addr {
        Some((host, port)) => normalize_host(host) != normalize_host(own_host) || port != own_port,
        None => false,
    }
}

/// Whether the confirmation poll is done. Both [`failover_finished`] and
/// [`switched_away`] must hold — see the module doc's "Both signals are
/// required" paragraph for why the address/role change alone is not enough
/// (it can fire before this node's own Sentinel, still the failover leader,
/// has finished reconfiguring every other known replica) and why
/// `failover_in_progress` clearing alone is not enough either (an aborted
/// failover also clears it while this node stays master).
pub(crate) fn demote_confirmed(
    own_host: &str,
    own_port: u16,
    master_addr: Option<(&str, u16)>,
    local_role: Role,
    failover_flags: Option<&str>,
) -> bool {
    failover_finished(failover_flags) && switched_away(own_host, own_port, master_addr, local_role)
}

/// How long the best-effort write pause holds, in the milliseconds `CLIENT
/// PAUSE` takes: the confirmation deadline (the window this sequence can
/// occupy), capped at [`MAX_WRITE_PAUSE`]. `0` — an operator zeroing the
/// deadline out — means no pause is issued at all.
pub(crate) fn write_pause_ms(deadline: Duration) -> u64 {
    deadline.min(MAX_WRITE_PAUSE).as_millis() as u64
}

/// The address the closing best-effort `REPLICAOF` should point the local
/// Redis at: Sentinel's last observed master, but only when it names a node
/// other than this one. Sentinel still naming this node (an aborted
/// failover) or never having answered means there is nothing safe to attach
/// to — `None`, and the caller skips the command. Hosts are compared
/// normalized and the port must match too, same as [`switched_away`].
pub(crate) fn replicaof_target(
    own_host: &str,
    own_port: u16,
    master_addr: Option<(&str, u16)>,
) -> Option<(String, u16)> {
    match master_addr {
        Some((host, port))
            if normalize_host(host) != normalize_host(own_host) || port != own_port =>
        {
            Some((host.to_string(), port))
        }
        _ => None,
    }
}

/// Local Redis's role, via a timeout-bounded `INFO replication`.
/// `Role::Unknown` on any failure (connect refused, handshake timeout, no
/// role line in the reply) — the caller treats that exactly like "not
/// master", so an unreachable local Redis never blocks shutdown.
async fn local_role(redis_url: &str) -> Role {
    let Some(mut conn) = connect(redis_url, CALL_DEADLINE).await else {
        return Role::Unknown;
    };
    match timeout(
        CALL_DEADLINE,
        redis::cmd("INFO")
            .arg("replication")
            .query_async::<String>(&mut conn),
    )
    .await
    {
        Ok(Ok(info)) => parse_role(&info),
        _ => Role::Unknown,
    }
}

/// Best-effort `CLIENT PAUSE <ms> WRITE` on the local Redis, timeout-
/// bounded (module doc, step 2). `Err` on an unreachable local Redis, a
/// timeout, or the server refusing — the caller warns and proceeds without
/// the pause in every case.
async fn pause_writes(redis_url: &str, pause_ms: u64) -> Result<(), String> {
    let Some(mut conn) = connect(redis_url, CALL_DEADLINE).await else {
        return Err("local redis unreachable".to_string());
    };
    match timeout(
        CALL_DEADLINE,
        redis::cmd("CLIENT")
            .arg("PAUSE")
            .arg(pause_ms)
            .arg("WRITE")
            .query_async::<String>(&mut conn),
    )
    .await
    {
        Ok(Ok(_)) => Ok(()),
        Ok(Err(e)) => Err(e.to_string()),
        Err(_) => Err(format!("timed out after {:?}", CALL_DEADLINE)),
    }
}

/// Best-effort `REPLICAOF <host> <port>` on the local Redis, timeout-
/// bounded (module doc, step 5). Same idiom as `link_heal::issue_replicaof`
/// — the command is cheap and non-destructive, and pointing at the master
/// this node is already attached to is a no-op. `Err` for the caller to
/// warn on and proceed.
async fn reattach_to_new_master(redis_url: &str, host: &str, port: u16) -> Result<(), String> {
    let Some(mut conn) = connect(redis_url, CALL_DEADLINE).await else {
        return Err("local redis unreachable".to_string());
    };
    match timeout(
        CALL_DEADLINE,
        redis::cmd("REPLICAOF")
            .arg(host)
            .arg(port)
            .query_async::<()>(&mut conn),
    )
    .await
    {
        Ok(Ok(())) => Ok(()),
        Ok(Err(e)) => Err(e.to_string()),
        Err(_) => Err(format!("timed out after {:?}", CALL_DEADLINE)),
    }
}

/// `SENTINEL FAILOVER <master_name>` against the local Sentinel, timeout-
/// bounded. `Err` covers every way this can fail to help: the local
/// Sentinel unreachable, the call timing out, or Sentinel itself refusing
/// (`-NOGOODSLAVE`, `-INPROG`, ...) — the caller logs and proceeds with the
/// normal shutdown in every case.
async fn request_failover(sentinel_url: &str, master_name: &str) -> Result<(), String> {
    let Some(mut conn) = connect(sentinel_url, CALL_DEADLINE).await else {
        return Err("local sentinel unreachable".to_string());
    };
    match timeout(
        CALL_DEADLINE,
        redis::cmd("SENTINEL")
            .arg("FAILOVER")
            .arg(master_name)
            .query_async::<String>(&mut conn),
    )
    .await
    {
        Ok(Ok(_)) => Ok(()),
        Ok(Err(e)) => Err(e.to_string()),
        Err(_) => Err(format!("timed out after {:?}", CALL_DEADLINE)),
    }
}

/// What the confirmation poll ended with: whether [`demote_confirmed`] held
/// before the deadline, plus Sentinel's last observed master address either
/// way — the closing best-effort `REPLICAOF` (module doc, step 5) wants
/// that address even on the timeout arm, when Sentinel already names
/// another node. A poll iteration where Sentinel gave no usable address
/// leaves the previous observation standing rather than erasing it.
struct DemotionWait {
    confirmed: bool,
    last_master_addr: Option<(String, u16)>,
}

/// Poll (see module docs for the two required signals) until the failover
/// is confirmed or `deadline` elapses. Each RPC is independently bounded by
/// [`CALL_DEADLINE`] via `sentinel_query::connect`/`get_master_fields`;
/// `deadline` is what actually bounds this loop's wall-clock time.
///
/// `SENTINEL MASTER` (not `get-master-addr-by-name`) is the one call this
/// needs: its flat reply carries `ip`/`port` (the same address
/// `get-master-addr-by-name` would answer, once a failover is running or
/// done) AND `flags` (`failover_in_progress`) together, in one round trip.
async fn wait_for_demotion(
    redis_url: &str,
    sentinel_url: &str,
    master_name: &str,
    own_host: &str,
    own_port: u16,
    deadline: Duration,
) -> DemotionWait {
    let start = Instant::now();
    let mut last_master_addr: Option<(String, u16)> = None;
    loop {
        let role = local_role(redis_url).await;
        let fields = match connect(sentinel_url, CALL_DEADLINE).await {
            Some(mut conn) => get_master_fields(&mut conn, master_name, CALL_DEADLINE).await,
            None => None,
        };
        let master_addr = fields.as_ref().and_then(|f| {
            let host = field_value(f, "ip")?;
            let port = field_value(f, "port")?.parse::<u16>().ok()?;
            Some((host, port))
        });
        if master_addr.is_some() {
            last_master_addr = master_addr.clone();
        }
        let flags = fields.as_ref().and_then(|f| field_value(f, "flags"));
        if demote_confirmed(
            own_host,
            own_port,
            master_addr.as_ref().map(|(h, p)| (h.as_str(), *p)),
            role,
            flags.as_deref(),
        ) {
            return DemotionWait {
                confirmed: true,
                last_master_addr,
            };
        }
        if start.elapsed() >= deadline {
            return DemotionWait {
                confirmed: false,
                last_master_addr,
            };
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

/// Run the full demote-before-shutdown sequence. Never blocks the caller's
/// shutdown on failure — every error path is a log line, not a propagated
/// error. `sentinel_colocated` is the caller's live fact (`sentinel.is_some()`
/// in `process_manager::supervise`); `target.sentinel_enabled` is checked too
/// so this function is safe to call from anywhere, not just a caller that
/// remembered to gate correctly.
pub async fn demote_before_shutdown(target: &DemoteTarget, sentinel_colocated: bool) {
    if !should_run(
        target.sentinel_enabled,
        sentinel_colocated,
        env::var(DEMOTE_ON_SHUTDOWN_ENV).ok().as_deref(),
    ) {
        return;
    }

    let redis_url = format!(
        "redis://:{}@127.0.0.1:{}",
        target.redis_password, target.redis_port
    );
    // AUTHed iff the local conf requires it (same file-resolved password as
    // link_heal's and quorum's local Sentinel connections) — a shutdown on
    // an authed node would otherwise get NOAUTH exactly when it is asking
    // for the failover this module exists to trigger.
    let sentinel_url = crate::sentinel_query::build_redis_url(
        "127.0.0.1",
        target.sentinel_port,
        &target.local_sentinel_password,
    );

    let role = local_role(&redis_url).await;
    if !should_attempt_demote(role) {
        info!(
            ?role,
            "demote-on-shutdown: not master, skipping (no failover to trigger)"
        );
        return;
    }

    info!(
        master_name = %target.redis_master_name,
        "demote-on-shutdown: master shutting down — requesting SENTINEL FAILOVER before stopping redis"
    );

    let deadline = Duration::from_millis(u64::env_parse(DEMOTE_TIMEOUT_ENV, DEFAULT_TIMEOUT_MS));

    // Step 2 (module doc): pause direct-connected clients' writes for the
    // window the failover can occupy, so what they land here between the
    // request and the promotion doesn't die with the node. Best-effort —
    // a failed pause must never block or slow the shutdown, and the node
    // exits (dropping every connection, pause included) either way.
    let pause_ms = write_pause_ms(deadline);
    if pause_ms > 0 {
        match pause_writes(&redis_url, pause_ms).await {
            Ok(()) => info!(
                pause_ms,
                "demote-on-shutdown: paused writes on the local redis for the failover window"
            ),
            Err(err) => warn!(
                error = %err,
                pause_ms,
                "demote-on-shutdown: CLIENT PAUSE WRITE failed — proceeding without the write pause"
            ),
        }
    }

    if let Err(err) = request_failover(&sentinel_url, &target.redis_master_name).await {
        warn!(
            error = %err,
            "demote-on-shutdown: SENTINEL FAILOVER request failed — proceeding with normal shutdown"
        );
        return;
    }

    let own_host = RailwayEnv::private_domain();
    let start = Instant::now();
    let wait = wait_for_demotion(
        &redis_url,
        &sentinel_url,
        &target.redis_master_name,
        &own_host,
        target.redis_port,
        deadline,
    )
    .await;
    if wait.confirmed {
        info!(
            elapsed_ms = start.elapsed().as_millis() as u64,
            "demote-on-shutdown: failover confirmed — proceeding with shutdown"
        );
    } else {
        warn!(
            timeout_ms = deadline.as_millis() as u64,
            "demote-on-shutdown: timed out waiting for the failover to land — proceeding with normal shutdown"
        );
    }

    // Step 5 (module doc): whether confirmed or timed out, when Sentinel's
    // last answer names another node, best-effort REPLICAOF the local redis
    // at it — flipping this node read-only for the beat between here and
    // the SIGTERM, where the confirmation deliberately tolerates a local
    // `role:master`. No usable answer, or an answer still naming this node,
    // leaves nothing safe to attach to and the command is skipped.
    if let Some((host, port)) = replicaof_target(
        &own_host,
        target.redis_port,
        wait.last_master_addr
            .as_ref()
            .map(|(h, p)| (h.as_str(), *p)),
    ) {
        match reattach_to_new_master(&redis_url, &host, port).await {
            Ok(()) => info!(
                new_master = %format!("{host}:{port}"),
                "demote-on-shutdown: re-pointed the local redis at the new master before shutdown"
            ),
            Err(err) => warn!(
                error = %err,
                new_master = %format!("{host}:{port}"),
                "demote-on-shutdown: best-effort REPLICAOF failed — proceeding with shutdown"
            ),
        }
    }
}

#[cfg(test)]
mod parse_role_tests {
    use super::*;

    #[test]
    fn parses_master() {
        assert_eq!(
            parse_role("# Replication\r\nrole:master\r\nconnected_slaves:2\r\n"),
            Role::Master
        );
    }

    #[test]
    fn parses_slave() {
        assert_eq!(
            parse_role("# Replication\r\nrole:slave\r\nmaster_link_status:up\r\n"),
            Role::Replica
        );
    }

    #[test]
    fn missing_role_line_is_unknown() {
        assert_eq!(parse_role(""), Role::Unknown);
        assert_eq!(
            parse_role("# Replication\r\nconnected_slaves:0\r\n"),
            Role::Unknown
        );
    }

    #[test]
    fn unrecognized_value_is_unknown() {
        assert_eq!(parse_role("role:sentinel\r\n"), Role::Unknown);
    }
}

#[cfg(test)]
mod kill_switch_tests {
    use super::*;

    #[test]
    fn on_by_default() {
        assert!(enabled(None));
        assert!(enabled(Some("")));
        assert!(enabled(Some("true")));
        assert!(enabled(Some("garbage")));
    }

    #[test]
    fn only_false_turns_it_off() {
        assert!(!enabled(Some("false")));
        assert!(!enabled(Some("FALSE")));
        assert!(!enabled(Some(" False ")));
    }
}

#[cfg(test)]
mod should_run_tests {
    use super::*;

    #[test]
    fn requires_sentinel_enabled_and_colocated() {
        assert!(!should_run(false, true, None));
        assert!(!should_run(true, false, None));
        assert!(should_run(true, true, None));
    }

    #[test]
    fn the_kill_switch_disables_it_regardless_of_the_other_gates() {
        assert!(!should_run(true, true, Some("false")));
    }
}

#[cfg(test)]
mod should_attempt_demote_tests {
    use super::*;

    #[test]
    fn only_a_master_attempts_a_demote() {
        assert!(should_attempt_demote(Role::Master));
        assert!(!should_attempt_demote(Role::Replica));
        assert!(!should_attempt_demote(Role::Unknown));
    }
}

#[cfg(test)]
mod failover_finished_tests {
    use super::*;

    #[test]
    fn no_flags_is_never_finished() {
        assert!(!failover_finished(None));
    }

    #[test]
    fn in_progress_flag_is_not_finished() {
        assert!(!failover_finished(Some("master,failover_in_progress")));
    }

    #[test]
    fn plain_master_flags_are_finished() {
        assert!(failover_finished(Some("master")));
    }

    #[test]
    fn other_flags_without_the_in_progress_token_are_finished() {
        // An aborted failover clears the flag too — this function alone
        // cannot distinguish success from abort; demote_confirmed's other
        // half (switched_away) is what rules the abort case out.
        assert!(failover_finished(Some("master,s_down")));
    }
}

#[cfg(test)]
mod switched_away_tests {
    use super::*;

    fn addr(host: &str, port: u16) -> (String, u16) {
        (host.to_string(), port)
    }

    #[test]
    fn replica_role_alone_confirms_it() {
        assert!(switched_away("self.railway.internal", 6379, None, Role::Replica));
    }

    #[test]
    fn master_addr_naming_another_node_confirms_it_even_while_info_still_reads_master() {
        // The two signals can land in either order; either alone is enough.
        let other = addr("redis-2.railway.internal", 6379);
        assert!(switched_away(
            "self.railway.internal",
            6379,
            Some((other.0.as_str(), other.1)),
            Role::Master
        ));
    }

    #[test]
    fn master_addr_still_naming_self_is_not_confirmed() {
        assert!(!switched_away(
            "self.railway.internal",
            6379,
            Some(("self.railway.internal", 6379)),
            Role::Master
        ));
    }

    #[test]
    fn host_comparison_is_normalized() {
        // Same host as self modulo case and a trailing root dot — still self.
        assert!(!switched_away(
            "self.railway.internal",
            6379,
            Some(("Self.railway.internal.", 6379)),
            Role::Master
        ));
    }

    #[test]
    fn same_host_different_port_is_a_different_instance() {
        assert!(switched_away(
            "self.railway.internal",
            6379,
            Some(("self.railway.internal", 6380)),
            Role::Master
        ));
    }

    #[test]
    fn no_master_addr_and_still_master_is_not_confirmed() {
        assert!(!switched_away("self.railway.internal", 6379, None, Role::Master));
    }

    #[test]
    fn unknown_role_with_no_master_addr_is_not_confirmed() {
        assert!(!switched_away("self.railway.internal", 6379, None, Role::Unknown));
    }
}

#[cfg(test)]
mod demote_confirmed_tests {
    use super::*;

    const SELF_HOST: &str = "self.railway.internal";
    const SELF_PORT: u16 = 6379;
    const OTHER: (&str, u16) = ("redis-2.railway.internal", 6379);
    const DONE: Option<&str> = Some("master");
    const IN_PROGRESS: Option<&str> = Some("master,failover_in_progress");

    #[test]
    fn requires_both_signals_together() {
        // switched_away alone (in_progress still set) is not enough.
        assert!(!demote_confirmed(
            SELF_HOST,
            SELF_PORT,
            Some(OTHER),
            Role::Master,
            IN_PROGRESS
        ));
        // failover_finished alone (address still self) is not enough — the
        // aborted-failover case.
        assert!(!demote_confirmed(
            SELF_HOST,
            SELF_PORT,
            Some((SELF_HOST, SELF_PORT)),
            Role::Master,
            DONE
        ));
        // Both together confirm it.
        assert!(demote_confirmed(
            SELF_HOST,
            SELF_PORT,
            Some(OTHER),
            Role::Master,
            DONE
        ));
    }

    #[test]
    fn replica_role_plus_finished_confirms_it() {
        assert!(demote_confirmed(SELF_HOST, SELF_PORT, None, Role::Replica, DONE));
    }

    #[test]
    fn replica_role_while_still_in_progress_is_not_confirmed() {
        // Sentinel's own bookkeeping (reconfiguring the other replicas) may
        // still be running even after this node's role already flipped.
        assert!(!demote_confirmed(
            SELF_HOST,
            SELF_PORT,
            None,
            Role::Replica,
            IN_PROGRESS
        ));
    }

    #[test]
    fn no_signals_at_all_is_not_confirmed() {
        assert!(!demote_confirmed(SELF_HOST, SELF_PORT, None, Role::Master, None));
    }
}

#[cfg(test)]
mod write_pause_tests {
    use super::*;

    #[test]
    fn the_pause_tracks_the_confirmation_deadline_below_the_cap() {
        // The default deadline: the pause covers exactly the window the
        // demote sequence can occupy, no longer.
        assert_eq!(write_pause_ms(Duration::from_millis(10_000)), 10_000);
        assert_eq!(write_pause_ms(Duration::from_millis(2_500)), 2_500);
    }

    #[test]
    fn an_operator_raised_deadline_is_capped_at_thirty_seconds() {
        assert_eq!(write_pause_ms(Duration::from_secs(120)), 30_000);
        assert_eq!(write_pause_ms(MAX_WRITE_PAUSE), 30_000);
    }

    #[test]
    fn a_zeroed_deadline_means_no_pause_at_all() {
        // DEMOTE_ON_SHUTDOWN_TIMEOUT_MS=0 — the caller skips the CLIENT
        // PAUSE call entirely rather than sending a zero-length pause.
        assert_eq!(write_pause_ms(Duration::ZERO), 0);
    }
}

#[cfg(test)]
mod replicaof_target_tests {
    use super::*;

    const SELF_HOST: &str = "self.railway.internal";
    const SELF_PORT: u16 = 6379;

    #[test]
    fn another_node_is_the_target() {
        assert_eq!(
            replicaof_target(
                SELF_HOST,
                SELF_PORT,
                Some(("redis-2.railway.internal", 6379))
            ),
            Some(("redis-2.railway.internal".to_string(), 6379))
        );
    }

    #[test]
    fn sentinel_still_naming_self_is_never_a_target() {
        // The aborted-failover case: pointing a master at itself would be
        // actively wrong, so an answer that is self — including modulo case
        // and a trailing root dot — yields nothing to attach to.
        assert_eq!(
            replicaof_target(SELF_HOST, SELF_PORT, Some((SELF_HOST, SELF_PORT))),
            None
        );
        let aliased_self = ("Self.railway.internal.", SELF_PORT);
        assert_eq!(
            replicaof_target(SELF_HOST, SELF_PORT, Some(aliased_self)),
            None
        );
    }

    #[test]
    fn same_host_different_port_is_a_different_instance_and_a_target() {
        assert_eq!(
            replicaof_target(SELF_HOST, SELF_PORT, Some((SELF_HOST, 6380))),
            Some((SELF_HOST.to_string(), 6380))
        );
    }

    #[test]
    fn no_answer_is_no_target() {
        assert_eq!(replicaof_target(SELF_HOST, SELF_PORT, None), None);
    }
}
