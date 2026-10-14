//! Peer host pins: every peer hostname this node talks to is kept in
//! `/etc/hosts`, refreshed in the background, so Sentinel's own hostname
//! resolution never waits on the platform resolver.
//!
//! ## Why
//! Sentinel runs with `resolve-hostnames yes` and resolves peer names on
//! its main thread — on every hello message a peer publishes (every ~2s),
//! on every INFO refresh of the master (every 10s, one lookup per replica
//! line) and on every reconnect (`createSentinelAddr` → `anetResolve` →
//! `getaddrinfo`, identical in redis 7.2 and 8.4). That call has no
//! deadline of its own, and the container's `resolv.conf` carries no
//! `options timeout`, so one unanswered query costs glibc's default 5s × 2
//! attempts, multiplied by the search list. A resolver stall of a few
//! seconds therefore freezes Sentinel for 10–30s: it enters TILT, stops
//! answering `SENTINEL get-master-addr-by-name`, the health server's /role
//! fails closed, and HAProxy pulls the primary and resets every client
//! connection — for a stall that never touched Redis. Observed fleet-wide
//! on Railway (2026-10): `Failed to resolve hostname '<peer>'` and `+tilt`
//! landing in the same millisecond on a healthy primary.
//!
//! glibc consults `/etc/hosts` before DNS (`hosts: files dns`), so a pinned
//! name resolves from the file with no network round trip at all — the
//! blocking call simply has nothing to block on.
//!
//! ## A pin is a cache, never a fixed address
//! Container addresses change on every redeploy, so a pin that outlived
//! its peer's address would be worse than no pin. The rules:
//!
//! - **Answered** — the resolver returned addresses: the pin is replaced
//!   with them. A peer's new address lands within one refresh interval
//!   (default 3s); Sentinel re-resolves a peer's name whenever it
//!   reconnects, so it picks the new address up on its next attempt.
//! - **Silent** — timeout, SERVFAIL, REFUSED, a malformed reply, no
//!   resolver: the last known addresses are held. This is the whole point:
//!   a stalled resolver must not take a working cluster with it.
//! - **Absent** — the resolver said, authoritatively, that the name has no
//!   address (NXDOMAIN, or no record of either type): a redeploy in
//!   progress or a removed peer. The last known addresses are held, but
//!   only for `PEER_HOST_PINS_ABSENT_EXPIRY_SECONDS` (default 300) of
//!   continuous absence; past that the pin is dropped and the name falls
//!   back to plain DNS, exactly as before this module existed.
//!
//! The resolver is asked directly over UDP (same shape as `dns_probe`),
//! never through `getaddrinfo`: the question this module answers is "what
//! did the resolver say", and a libc lookup folds "it said nothing" and
//! "it said no" into one error — the distinction the hold-versus-expire
//! rule above depends on.
//!
//! ## Which names
//! The static set from the environment — every host in `SENTINEL_HOSTS`,
//! the replication source in `REPLICA_OF`, this node's own private domain
//! — plus whatever the local Sentinel currently knows: the master's
//! address, every peer Sentinel and every replica it reports. Nodes added
//! by a later scale-up are therefore pinned as soon as gossip names them.
//! A name that is an IP literal is never pinned (nothing to resolve).
//!
//! ## Writing the file
//! The pins live in one marked block at the end of `/etc/hosts`; every
//! line the platform wrote stays untouched. On Railway the file is a bind
//! mount, so an atomic rename-over is refused — the block is written in
//! place: one write at offset 0 followed by a truncate. A reader racing
//! that sees, at worst, the complete new content followed by a cut-off
//! tail of the old one, never a file with the pins missing.
//!
//! ## Supervision
//! Same shape as `quorum`/`link_heal`: an outer respawn loop wraps the
//! refresh loop so a panic surfaces as a log line instead of taking
//! redis-wrapper down. `PEER_HOST_PINS_DISABLED=1` is the kill switch.

use crate::dns_probe::{
    encode_query, first_nameserver, query_id, read_resolv_conf, QTYPE_A, QTYPE_AAAA,
};
use common::ConfigExt;
use std::collections::{BTreeMap, BTreeSet};
use std::fs::OpenOptions;
use std::io::Write;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use tokio::net::UdpSocket;
use tokio::time::{sleep, timeout};
use tracing::{info, warn};

pub const DEFAULT_HOSTS_FILE: &str = "/etc/hosts";
const DEFAULT_INTERVAL_SECONDS: u64 = 3;
const DEFAULT_ABSENT_EXPIRY_SECONDS: u64 = 300;
/// Per-query deadline. The resolver is on the host (or loopback); a healthy
/// answer takes well under a millisecond, and anything slower is the stall
/// this module exists to absorb — waiting longer would only delay the next
/// refresh of every other name.
const QUERY_DEADLINE: Duration = Duration::from_millis(1500);
/// Deadline for reading membership off the local Sentinel. A Sentinel that
/// is itself stalled simply contributes no new names this round.
const SENTINEL_DEADLINE: Duration = Duration::from_secs(1);
const RESPAWN_DELAY: Duration = Duration::from_secs(5);

const BLOCK_BEGIN: &str =
    "# BEGIN redis-ha peer pins (written by redis-wrapper; edits here are overwritten)";
const BLOCK_END: &str = "# END redis-ha peer pins";

/// What the resolver said about one name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Lookup {
    /// At least one address, of either family.
    Addresses(Vec<IpAddr>),
    /// An authoritative "no address": NXDOMAIN, or NODATA for both types.
    Absent,
    /// No usable answer at all.
    Silent,
}

/// One record-type query's outcome, before the two types are combined.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Reply {
    Records(Vec<IpAddr>),
    NoData,
    NxDomain,
    Failed,
}

/// The hold-versus-expire rule applied to one name.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Pin {
    /// Empty until the first answer: a name without addresses renders no
    /// line, so a lookup for it goes to DNS exactly as before.
    addresses: Vec<IpAddr>,
    /// When the current run of `Absent` answers began.
    absent_since: Option<Instant>,
    /// Whether the current run of `Silent` answers has been reported.
    silent_reported: bool,
    /// Whether the current run of `Absent` answers has been reported.
    absent_reported: bool,
}

impl Pin {
    fn new() -> Self {
        Self {
            addresses: Vec::new(),
            absent_since: None,
            silent_reported: false,
            absent_reported: false,
        }
    }
}

/// What one observation changed, for the log line it earns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Event {
    /// The pinned addresses changed (first answer, or a new address).
    Pinned {
        name: String,
        addresses: Vec<IpAddr>,
    },
    /// The resolver stopped answering for this name; its last addresses
    /// are being held. Reported once per silent episode.
    ResolverSilent { name: String, held: Vec<IpAddr> },
    /// The resolver answers again after a silent episode.
    ResolverBack { name: String },
    /// The name has no address right now; held for up to the expiry.
    /// Reported once per absent episode.
    Absent { name: String, held: Vec<IpAddr> },
    /// Continuously absent past the expiry: the pin is gone.
    Dropped { name: String, absent_for: Duration },
}

/// Tunables the refresh loop and its unit tests share.
#[derive(Debug, Clone, Copy)]
pub struct Policy {
    pub absent_expiry: Duration,
}

/// The pin table: one entry per name ever seen, kept until it expires.
#[derive(Debug, Default)]
pub struct Pins {
    entries: BTreeMap<String, Pin>,
}

impl Pins {
    pub fn names(&self) -> impl Iterator<Item = &str> {
        self.entries.keys().map(String::as_str)
    }

    /// Apply one lookup result for `name` at `now`. Returns the events the
    /// transition produced (possibly none).
    pub fn observe(
        &mut self,
        name: &str,
        lookup: Lookup,
        now: Instant,
        policy: &Policy,
    ) -> Vec<Event> {
        let mut events = Vec::new();
        let pin = self
            .entries
            .entry(name.to_string())
            .or_insert_with(Pin::new);
        match lookup {
            Lookup::Addresses(mut addresses) => {
                addresses.sort();
                addresses.dedup();
                if pin.silent_reported {
                    events.push(Event::ResolverBack {
                        name: name.to_string(),
                    });
                }
                pin.silent_reported = false;
                pin.absent_since = None;
                pin.absent_reported = false;
                if pin.addresses != addresses {
                    pin.addresses = addresses.clone();
                    events.push(Event::Pinned {
                        name: name.to_string(),
                        addresses,
                    });
                }
            }
            Lookup::Silent => {
                if !pin.silent_reported {
                    pin.silent_reported = true;
                    events.push(Event::ResolverSilent {
                        name: name.to_string(),
                        held: pin.addresses.clone(),
                    });
                }
            }
            Lookup::Absent => {
                if pin.silent_reported {
                    pin.silent_reported = false;
                    events.push(Event::ResolverBack {
                        name: name.to_string(),
                    });
                }
                let since = *pin.absent_since.get_or_insert(now);
                if pin.addresses.is_empty() {
                    // Nothing held, nothing to expire: a name that has never
                    // resolved stays an inert entry until it does.
                    return events;
                }
                if !pin.absent_reported {
                    pin.absent_reported = true;
                    events.push(Event::Absent {
                        name: name.to_string(),
                        held: pin.addresses.clone(),
                    });
                }
                let absent_for = now.saturating_duration_since(since);
                if absent_for >= policy.absent_expiry {
                    self.entries.remove(name);
                    events.push(Event::Dropped {
                        name: name.to_string(),
                        absent_for,
                    });
                }
            }
        }
        events
    }

    /// The managed block: one `address<TAB>name` line per pinned address,
    /// sorted so an unchanged table renders byte-identically. Empty when
    /// nothing is pinned.
    pub fn render_block(&self) -> String {
        let mut lines = Vec::new();
        for (name, pin) in &self.entries {
            for address in &pin.addresses {
                lines.push(format!("{address}\t{name}"));
            }
        }
        if lines.is_empty() {
            return String::new();
        }
        let mut block = String::from(BLOCK_BEGIN);
        block.push('\n');
        for line in lines {
            block.push_str(&line);
            block.push('\n');
        }
        block.push_str(BLOCK_END);
        block.push('\n');
        block
    }
}

/// `existing` with any previous managed block removed and `block` (which
/// may be empty) appended. Everything outside the markers is preserved
/// byte-for-byte, trailing newline normalised.
pub fn splice_block(existing: &str, block: &str) -> String {
    let mut out = String::with_capacity(existing.len() + block.len() + 1);
    let mut in_block = false;
    for line in existing.split_inclusive('\n') {
        let trimmed = line.trim_end_matches(['\n', '\r']);
        if in_block {
            if trimmed == BLOCK_END {
                in_block = false;
            }
            continue;
        }
        if trimmed == BLOCK_BEGIN {
            in_block = true;
            continue;
        }
        out.push_str(line);
    }
    if !out.is_empty() && !out.ends_with('\n') {
        out.push('\n');
    }
    out.push_str(block);
    out
}

/// Replace the file's contents in place (see the module doc for why not a
/// rename): one write at offset 0, then truncate to the new length.
pub fn write_in_place(path: &Path, contents: &str) -> std::io::Result<()> {
    let mut file = OpenOptions::new().write(true).open(path)?;
    file.write_all(contents.as_bytes())?;
    file.set_len(contents.len() as u64)?;
    file.flush()
}

/// Read, splice, write — the one filesystem round trip per changed block.
fn apply_block(path: &Path, block: &str) -> std::io::Result<()> {
    let existing = std::fs::read_to_string(path)?;
    let next = splice_block(&existing, block);
    if next == existing {
        return Ok(());
    }
    write_in_place(path, &next)
}

/// Whether `host` is a DNS hostname this module will write into
/// `/etc/hosts`: lowercase letters, digits, `-`, `_` and `.`, at most 253
/// bytes, no empty labels. Names reach here from gossip — a peer's
/// announced address, which anyone holding the Sentinel password can set —
/// and a line in the hosts file is only ever as trustworthy as its
/// character set: a newline inside a "hostname" would write a second,
/// attacker-chosen entry (`localhost`, another service's name) under this
/// module's markers.
fn is_hostname_shaped(host: &str) -> bool {
    !host.is_empty()
        && host.len() <= 253
        && host
            .split('.')
            .all(|label| !label.is_empty() && label.len() <= 63)
        && host.bytes().all(|b| {
            b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-' || b == b'_' || b == b'.'
        })
}

/// The host half of a `host:port` entry, `[v6]:port` and bare hosts
/// included. `None` for anything that is not a pinnable name: empty, an IP
/// literal (nothing to resolve), loopback names, or anything that is not
/// hostname-shaped (see `is_hostname_shaped`).
pub fn pinnable_host(entry: &str) -> Option<String> {
    let entry = entry.trim();
    if entry.is_empty() {
        return None;
    }
    let host = if let Some(rest) = entry.strip_prefix('[') {
        rest.split(']').next().unwrap_or_default()
    } else if entry.matches(':').count() == 1 {
        entry
            .rsplit_once(':')
            .map(|(host, _)| host)
            .unwrap_or(entry)
    } else {
        // No port, or a bare IPv6 literal (several colons).
        entry
    };
    let host = host.trim().trim_end_matches('.');
    if host.is_empty() || host.parse::<IpAddr>().is_ok() {
        return None;
    }
    if host.eq_ignore_ascii_case("localhost") {
        return None;
    }
    let host = host.to_ascii_lowercase();
    is_hostname_shaped(&host).then_some(host)
}

/// The names known from the environment alone, before Sentinel is up.
pub fn static_names(
    sentinel_hosts: &str,
    private_domain: &str,
    extra: &[&str],
) -> BTreeSet<String> {
    let mut names = BTreeSet::new();
    for entry in sentinel_hosts.split(',') {
        if let Some(host) = pinnable_host(entry) {
            names.insert(host);
        }
    }
    if let Some(host) = pinnable_host(private_domain) {
        names.insert(host);
    }
    for entry in extra {
        if let Some(host) = pinnable_host(entry) {
            names.insert(host);
        }
    }
    names
}

// ----- the resolver side -----------------------------------------------------

/// The name-walking half of answer parsing: the offset just past a
/// (possibly compressed) domain name starting at `pos`.
fn skip_name(buf: &[u8], mut pos: usize) -> Option<usize> {
    loop {
        let len = *buf.get(pos)? as usize;
        if len == 0 {
            return Some(pos + 1);
        }
        if len & 0xC0 == 0xC0 {
            return (pos + 2 <= buf.len()).then_some(pos + 2);
        }
        pos = pos + 1 + len;
    }
}

/// What a reply matching `id` said. `None` for anything that is not a
/// well-formed response to this query (wrong id, not a response, cut off).
fn decode_reply(id: u16, reply: &[u8]) -> Option<Reply> {
    if reply.len() < 12 || reply[0..2] != id.to_be_bytes() || reply[2] & 0x80 == 0 {
        return None;
    }
    match reply[3] & 0x0F {
        0 => {}
        3 => return Some(Reply::NxDomain),
        _ => return Some(Reply::Failed),
    }
    let questions = u16::from_be_bytes([reply[4], reply[5]]) as usize;
    let answers = u16::from_be_bytes([reply[6], reply[7]]) as usize;
    let mut pos = 12;
    for _ in 0..questions {
        pos = skip_name(reply, pos)?;
        pos = pos.checked_add(4)?;
        if pos > reply.len() {
            return None;
        }
    }
    let mut addresses = Vec::new();
    for _ in 0..answers {
        pos = skip_name(reply, pos)?;
        if pos + 10 > reply.len() {
            return None;
        }
        let rtype = u16::from_be_bytes([reply[pos], reply[pos + 1]]);
        let rdlen = u16::from_be_bytes([reply[pos + 8], reply[pos + 9]]) as usize;
        pos += 10;
        if pos + rdlen > reply.len() {
            return None;
        }
        let rdata = &reply[pos..pos + rdlen];
        match (rtype, rdlen) {
            (QTYPE_A, 4) => {
                addresses.push(IpAddr::V4(Ipv4Addr::new(
                    rdata[0], rdata[1], rdata[2], rdata[3],
                )));
            }
            (QTYPE_AAAA, 16) => {
                let mut octets = [0u8; 16];
                octets.copy_from_slice(rdata);
                addresses.push(IpAddr::V6(Ipv6Addr::from(octets)));
            }
            _ => {} // CNAME and friends: the chased address records follow
        }
        pos += rdlen;
    }
    if addresses.is_empty() {
        Some(Reply::NoData)
    } else {
        Some(Reply::Records(addresses))
    }
}

/// The two record types folded into one verdict. Records of either type
/// win; an NXDOMAIN is a property of the name and makes it absent; a
/// failure of either type with no records makes the whole lookup silent
/// (the other type may just not exist); only two NODATAs mean "exists,
/// but has no address".
fn combine(a: Reply, aaaa: Reply) -> Lookup {
    let mut addresses = Vec::new();
    for reply in [&a, &aaaa] {
        if let Reply::Records(records) = reply {
            addresses.extend(records.iter().copied());
        }
    }
    if !addresses.is_empty() {
        return Lookup::Addresses(addresses);
    }
    if a == Reply::NxDomain || aaaa == Reply::NxDomain {
        return Lookup::Absent;
    }
    if a == Reply::Failed || aaaa == Reply::Failed {
        return Lookup::Silent;
    }
    Lookup::Absent
}

async fn query_once(name: &str, qtype: u16, nameserver: SocketAddr) -> Option<Reply> {
    let id = query_id() ^ qtype;
    let packet = encode_query(id, name, qtype)?;
    let bind_addr: SocketAddr = if nameserver.is_ipv6() {
        "[::]:0".parse().ok()?
    } else {
        "0.0.0.0:0".parse().ok()?
    };
    let socket = UdpSocket::bind(bind_addr).await.ok()?;
    socket.connect(nameserver).await.ok()?;
    socket.send(&packet).await.ok()?;
    let mut reply = [0u8; 1500];
    loop {
        let len = socket.recv(&mut reply).await.ok()?;
        if let Some(parsed) = decode_reply(id, &reply[..len]) {
            return Some(parsed);
        }
    }
}

async fn query(name: &str, qtype: u16, nameserver: SocketAddr, deadline: Duration) -> Reply {
    match timeout(deadline, query_once(name, qtype, nameserver)).await {
        Ok(Some(reply)) => reply,
        _ => Reply::Failed,
    }
}

/// Ask the resolver for `name`'s A and AAAA records in parallel.
pub async fn lookup(name: &str, nameserver: SocketAddr, deadline: Duration) -> Lookup {
    let (a, aaaa) = tokio::join!(
        query(name, QTYPE_A, nameserver, deadline),
        query(name, QTYPE_AAAA, nameserver, deadline)
    );
    combine(a, aaaa)
}

/// All of `names`, resolved concurrently against the system resolver.
async fn lookup_all(names: &BTreeSet<String>) -> Vec<(String, Lookup)> {
    let Some(nameserver) = first_nameserver(&read_resolv_conf()) else {
        return names
            .iter()
            .map(|name| (name.clone(), Lookup::Silent))
            .collect();
    };
    let mut set = tokio::task::JoinSet::new();
    for name in names {
        let name = name.clone();
        set.spawn(async move {
            let verdict = lookup(&name, nameserver, QUERY_DEADLINE).await;
            (name, verdict)
        });
    }
    let mut results = Vec::with_capacity(names.len());
    while let Some(joined) = set.join_next().await {
        if let Ok(result) = joined {
            results.push(result);
        }
    }
    results
}

// ----- the Sentinel side -----------------------------------------------------

/// Every host the local Sentinel currently knows for `master_name`: the
/// master, its replicas and the peer Sentinels. Empty when Sentinel is not
/// answering within the deadline — never a reason to skip the refresh.
async fn sentinel_member_names(sentinel_url: &str, master_name: &str) -> BTreeSet<String> {
    let mut names = BTreeSet::new();
    let Some(mut conn) = crate::sentinel_query::connect(sentinel_url, SENTINEL_DEADLINE).await
    else {
        return names;
    };
    if let Some((host, _)) =
        crate::sentinel_query::get_master_addr(&mut conn, master_name, SENTINEL_DEADLINE).await
    {
        if let Some(host) = pinnable_host(&host) {
            names.insert(host);
        }
    }
    for listing in ["sentinels", "replicas"] {
        let reply = timeout(
            SENTINEL_DEADLINE,
            redis::cmd("SENTINEL")
                .arg(listing)
                .arg(master_name)
                .query_async::<Vec<Vec<String>>>(&mut conn),
        )
        .await;
        let Ok(Ok(entries)) = reply else { continue };
        for entry in &entries {
            if let Some(host) =
                crate::quorum::field_value(entry, "ip").and_then(|ip| pinnable_host(&ip))
            {
                names.insert(host);
            }
        }
    }
    names
}

// ----- the loop --------------------------------------------------------------

/// Everything the refresh loop needs, resolved once at spawn.
#[derive(Debug, Clone)]
pub struct Settings {
    pub hosts_file: PathBuf,
    pub interval: Duration,
    pub policy: Policy,
    pub static_names: BTreeSet<String>,
    /// `None` when Sentinel is not colocated: only the static set is pinned.
    pub sentinel_url: Option<String>,
    pub master_name: String,
}

impl Settings {
    /// The env-driven settings. `extra` carries hosts known only to the
    /// caller (the boot-time replication source).
    pub fn from_env(
        sentinel_hosts: &str,
        private_domain: &str,
        extra: &[&str],
        sentinel_url: Option<String>,
        master_name: &str,
    ) -> Self {
        Self {
            hosts_file: PathBuf::from(String::env_or("PEER_HOST_PINS_FILE", DEFAULT_HOSTS_FILE)),
            interval: Duration::from_secs(u64::env_parse(
                "PEER_HOST_PINS_INTERVAL_SECONDS",
                DEFAULT_INTERVAL_SECONDS,
            )),
            policy: Policy {
                absent_expiry: Duration::from_secs(u64::env_parse(
                    "PEER_HOST_PINS_ABSENT_EXPIRY_SECONDS",
                    DEFAULT_ABSENT_EXPIRY_SECONDS,
                )),
            },
            static_names: static_names(sentinel_hosts, private_domain, extra),
            sentinel_url,
            master_name: master_name.to_string(),
        }
    }
}

pub fn disabled() -> bool {
    std::env::var("PEER_HOST_PINS_DISABLED").ok().as_deref() == Some("1")
}

fn join_addresses(addresses: &[IpAddr]) -> String {
    if addresses.is_empty() {
        return "none".to_string();
    }
    addresses
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(", ")
}

fn report(event: &Event, policy: &Policy) {
    match event {
        Event::Pinned { name, addresses } => info!(
            peer = %name,
            addresses = %join_addresses(addresses),
            "peer-pins: pinned {name} to {} — Sentinel resolves it from /etc/hosts, without waiting on DNS",
            join_addresses(addresses)
        ),
        Event::ResolverSilent { name, held } if held.is_empty() => warn!(
            peer = %name,
            "peer-pins: the resolver gave no answer for {name} and no address is pinned yet; Sentinel may stall resolving it until the resolver answers"
        ),
        Event::ResolverSilent { name, held } => warn!(
            peer = %name,
            held = %join_addresses(held),
            "peer-pins: the resolver gave no answer for {name}; holding its last known address ({}) until the resolver answers again",
            join_addresses(held)
        ),
        Event::ResolverBack { name } => {
            info!(peer = %name, "peer-pins: the resolver is answering for {name} again")
        }
        Event::Absent { name, held } => info!(
            peer = %name,
            held = %join_addresses(held),
            expiry_secs = policy.absent_expiry.as_secs(),
            "peer-pins: {name} has no address right now (redeploying, or removed); keeping its last known address for up to {}s",
            policy.absent_expiry.as_secs()
        ),
        Event::Dropped { name, absent_for } => info!(
            peer = %name,
            absent_secs = absent_for.as_secs(),
            "peer-pins: {name} has had no address for {}s; its pin was dropped and the name resolves through DNS again",
            absent_for.as_secs()
        ),
    }
}

/// One refresh: gather names, resolve them, apply the rules, rewrite the
/// block if it changed. `Err` carries the hosts-file write failure, which
/// the caller reports once per distinct error rather than every interval.
async fn refresh(
    settings: &Settings,
    pins: &mut Pins,
    last_block: &mut Option<String>,
) -> Result<(), String> {
    let mut names: BTreeSet<String> = settings.static_names.clone();
    names.extend(pins.names().map(str::to_string));
    if let Some(url) = &settings.sentinel_url {
        names.extend(sentinel_member_names(url, &settings.master_name).await);
    }
    let results = lookup_all(&names).await;
    let now = Instant::now();
    for (name, lookup) in results {
        for event in pins.observe(&name, lookup, now, &settings.policy) {
            report(&event, &settings.policy);
        }
    }
    let block = pins.render_block();
    if last_block.as_deref() == Some(block.as_str()) {
        return Ok(());
    }
    let path = settings.hosts_file.clone();
    let to_write = block.clone();
    let written = tokio::task::spawn_blocking(move || apply_block(&path, &to_write)).await;
    match written {
        Ok(Ok(())) => {
            *last_block = Some(block);
            Ok(())
        }
        Ok(Err(err)) => Err(err.to_string()),
        Err(err) => Err(format!("write task failed: {err}")),
    }
}

/// One synchronous refresh over the static names, meant to run before
/// Sentinel starts so its very first lookups already hit the file. Bounded
/// by one query deadline (names resolve concurrently). Returns the table
/// for the background loop to carry on with.
pub async fn prime(settings: &Settings) -> Pins {
    if disabled() {
        return Pins::default();
    }
    let mut pins = Pins::default();
    let mut last_block = None;
    let static_only = Settings {
        sentinel_url: None,
        ..settings.clone()
    };
    if let Err(err) = refresh(&static_only, &mut pins, &mut last_block).await {
        warn!(
            error = %err,
            path = %settings.hosts_file.display(),
            "peer-pins: could not write the hosts file before Sentinel started; peers resolve through DNS until it is writable"
        );
    }
    pins
}

/// Spawn the supervised refresh loop, continuing from `pins`.
pub fn spawn(settings: Settings, pins: Pins) {
    if disabled() {
        info!("peer-pins: PEER_HOST_PINS_DISABLED=1, peers resolve through DNS only");
        return;
    }
    info!(
        hosts_file = %settings.hosts_file.display(),
        interval_secs = settings.interval.as_secs(),
        absent_expiry_secs = settings.policy.absent_expiry.as_secs(),
        names = settings.static_names.len(),
        "peer-pins: starting refresh loop"
    );
    tokio::spawn(async move {
        let mut carried = Some(pins);
        loop {
            let cfg = settings.clone();
            let state = carried.take().unwrap_or_default();
            let handle = tokio::task::spawn(async move { run(cfg, state).await });
            match handle.await {
                Ok(()) => warn!("peer-pins: refresh loop returned cleanly — respawning in 5s"),
                Err(e) if e.is_panic() => {
                    warn!(panic = ?e, "peer-pins: refresh loop panicked — respawning in 5s")
                }
                Err(e) => warn!(error = %e, "peer-pins: join error — respawning in 5s"),
            }
            sleep(RESPAWN_DELAY).await;
        }
    });
}

async fn run(settings: Settings, mut pins: Pins) {
    let mut last_block = None;
    let mut reported_write_error: Option<String> = None;
    loop {
        match refresh(&settings, &mut pins, &mut last_block).await {
            Ok(()) => {
                if reported_write_error.take().is_some() {
                    info!(
                        path = %settings.hosts_file.display(),
                        "peer-pins: the hosts file is writable again"
                    );
                }
            }
            Err(err) => {
                if reported_write_error.as_deref() != Some(err.as_str()) {
                    warn!(
                        error = %err,
                        path = %settings.hosts_file.display(),
                        "peer-pins: could not update the hosts file; peers keep resolving through DNS"
                    );
                    reported_write_error = Some(err);
                }
            }
        }
        sleep(settings.interval).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn v4(s: &str) -> IpAddr {
        s.parse().unwrap()
    }

    fn v6(s: &str) -> IpAddr {
        s.parse().unwrap()
    }

    fn policy(secs: u64) -> Policy {
        Policy {
            absent_expiry: Duration::from_secs(secs),
        }
    }

    // ----- names -----

    #[test]
    fn pinnable_host_strips_ports_and_skips_literals() {
        assert_eq!(
            pinnable_host("redis-2.railway.internal:26379"),
            Some("redis-2.railway.internal".to_string())
        );
        assert_eq!(pinnable_host("redis-2"), Some("redis-2".to_string()));
        assert_eq!(
            pinnable_host("Redis-2.Railway.Internal."),
            Some("redis-2.railway.internal".to_string())
        );
        assert_eq!(pinnable_host("[fd12::10]:26379"), None);
        assert_eq!(pinnable_host("fd12::10"), None);
        assert_eq!(pinnable_host("10.0.0.5:6379"), None);
        assert_eq!(pinnable_host("localhost:26379"), None);
        assert_eq!(pinnable_host(""), None);
        assert_eq!(pinnable_host("  "), None);
    }

    #[test]
    fn pinnable_host_refuses_anything_that_could_forge_a_hosts_line() {
        // A newline, a space or a tab inside a "hostname" would let a peer's
        // gossip write an extra entry under this module's markers.
        assert_eq!(pinnable_host("evil\nlocalhost"), None);
        assert_eq!(pinnable_host("evil localhost"), None);
        assert_eq!(pinnable_host("evil\tlocalhost"), None);
        assert_eq!(pinnable_host("evil\rlocalhost"), None);
        assert_eq!(pinnable_host("#comment"), None);
        assert_eq!(pinnable_host("a..b"), None);
        assert_eq!(pinnable_host(&"x".repeat(64)), None);
        assert_eq!(
            pinnable_host(&format!("{}.internal", "a".repeat(250))),
            None
        );
        // Underscores and digits are fine (Railway names can carry both).
        assert_eq!(
            pinnable_host("redis_2-5lbi.railway.internal"),
            Some("redis_2-5lbi.railway.internal".to_string())
        );
    }

    #[test]
    fn static_names_union_without_duplicates() {
        let names = static_names(
            "redis-1.railway.internal:26379,redis-2.railway.internal:26379,redis-1.railway.internal:26379",
            "redis-1.railway.internal",
            &["redis-3.railway.internal:6379", "", "fd12::1"],
        );
        let expected: BTreeSet<String> = [
            "redis-1.railway.internal",
            "redis-2.railway.internal",
            "redis-3.railway.internal",
        ]
        .into_iter()
        .map(String::from)
        .collect();
        assert_eq!(names, expected);
    }

    // ----- the rules -----

    #[test]
    fn first_answer_pins_and_an_unchanged_answer_is_silent() {
        let mut pins = Pins::default();
        let now = Instant::now();
        let events = pins.observe(
            "r2",
            Lookup::Addresses(vec![v6("fd12::2")]),
            now,
            &policy(300),
        );
        assert_eq!(
            events,
            vec![Event::Pinned {
                name: "r2".into(),
                addresses: vec![v6("fd12::2")]
            }]
        );
        let events = pins.observe(
            "r2",
            Lookup::Addresses(vec![v6("fd12::2")]),
            now,
            &policy(300),
        );
        assert!(events.is_empty());
        assert!(pins.render_block().contains("fd12::2\tr2\n"));
    }

    #[test]
    fn a_new_address_replaces_the_old_one_immediately() {
        let mut pins = Pins::default();
        let now = Instant::now();
        pins.observe(
            "r2",
            Lookup::Addresses(vec![v6("fd12::2")]),
            now,
            &policy(300),
        );
        let events = pins.observe(
            "r2",
            Lookup::Addresses(vec![v6("fd12::22")]),
            now,
            &policy(300),
        );
        assert_eq!(
            events,
            vec![Event::Pinned {
                name: "r2".into(),
                addresses: vec![v6("fd12::22")]
            }]
        );
        let block = pins.render_block();
        assert!(block.contains("fd12::22\tr2\n"));
        assert!(!block.contains("fd12::2\t"));
    }

    #[test]
    fn silence_holds_the_last_address_and_reports_once() {
        let mut pins = Pins::default();
        let now = Instant::now();
        pins.observe(
            "r2",
            Lookup::Addresses(vec![v6("fd12::2")]),
            now,
            &policy(300),
        );
        let first = pins.observe(
            "r2",
            Lookup::Silent,
            now + Duration::from_secs(3),
            &policy(300),
        );
        assert_eq!(
            first,
            vec![Event::ResolverSilent {
                name: "r2".into(),
                held: vec![v6("fd12::2")]
            }]
        );
        // Hours of silence: still held, nothing more said.
        let later = pins.observe(
            "r2",
            Lookup::Silent,
            now + Duration::from_secs(7200),
            &policy(300),
        );
        assert!(later.is_empty());
        assert!(pins.render_block().contains("fd12::2\tr2\n"));
        // Back: one line, pin unchanged.
        let back = pins.observe(
            "r2",
            Lookup::Addresses(vec![v6("fd12::2")]),
            now + Duration::from_secs(7203),
            &policy(300),
        );
        assert_eq!(back, vec![Event::ResolverBack { name: "r2".into() }]);
    }

    #[test]
    fn absence_holds_until_the_expiry_then_drops() {
        let mut pins = Pins::default();
        let now = Instant::now();
        pins.observe(
            "r2",
            Lookup::Addresses(vec![v6("fd12::2")]),
            now,
            &policy(300),
        );
        let first = pins.observe(
            "r2",
            Lookup::Absent,
            now + Duration::from_secs(3),
            &policy(300),
        );
        assert_eq!(
            first,
            vec![Event::Absent {
                name: "r2".into(),
                held: vec![v6("fd12::2")]
            }]
        );
        let mid = pins.observe(
            "r2",
            Lookup::Absent,
            now + Duration::from_secs(200),
            &policy(300),
        );
        assert!(mid.is_empty());
        assert!(pins.render_block().contains("fd12::2\tr2\n"));
        let dropped = pins.observe(
            "r2",
            Lookup::Absent,
            now + Duration::from_secs(303),
            &policy(300),
        );
        assert!(matches!(dropped.as_slice(), [Event::Dropped { name, .. }] if name == "r2"));
        assert_eq!(pins.render_block(), "");
        assert_eq!(pins.names().count(), 0);
    }

    #[test]
    fn an_answer_inside_the_absence_window_resets_the_clock() {
        let mut pins = Pins::default();
        let now = Instant::now();
        pins.observe(
            "r2",
            Lookup::Addresses(vec![v6("fd12::2")]),
            now,
            &policy(300),
        );
        pins.observe(
            "r2",
            Lookup::Absent,
            now + Duration::from_secs(10),
            &policy(300),
        );
        // Redeploy finished on a new address.
        pins.observe(
            "r2",
            Lookup::Addresses(vec![v6("fd12::3")]),
            now + Duration::from_secs(60),
            &policy(300),
        );
        // A later absence starts a fresh window: 250s in, still held.
        pins.observe(
            "r2",
            Lookup::Absent,
            now + Duration::from_secs(100),
            &policy(300),
        );
        let events = pins.observe(
            "r2",
            Lookup::Absent,
            now + Duration::from_secs(350),
            &policy(300),
        );
        assert!(events.is_empty());
        assert!(pins.render_block().contains("fd12::3\tr2\n"));
    }

    #[test]
    fn a_name_that_never_resolved_renders_nothing_and_never_expires() {
        let mut pins = Pins::default();
        let now = Instant::now();
        assert!(pins
            .observe("r9", Lookup::Absent, now, &policy(1))
            .is_empty());
        assert!(pins
            .observe(
                "r9",
                Lookup::Absent,
                now + Duration::from_secs(10),
                &policy(1)
            )
            .is_empty());
        assert_eq!(pins.render_block(), "");
        // Still tracked, so it is pinned the moment it resolves.
        assert_eq!(pins.names().collect::<Vec<_>>(), vec!["r9"]);
        let silent = pins.observe("r9", Lookup::Silent, now, &policy(1));
        assert_eq!(
            silent,
            vec![Event::ResolverSilent {
                name: "r9".into(),
                held: vec![]
            }]
        );
    }

    #[test]
    fn both_families_render_and_sort_deterministically() {
        let mut pins = Pins::default();
        let now = Instant::now();
        pins.observe(
            "r2",
            Lookup::Addresses(vec![v6("fd12::2"), v4("10.0.0.2")]),
            now,
            &policy(300),
        );
        pins.observe(
            "r1",
            Lookup::Addresses(vec![v4("10.0.0.1")]),
            now,
            &policy(300),
        );
        let block = pins.render_block();
        let expected =
            format!("{BLOCK_BEGIN}\n10.0.0.1\tr1\n10.0.0.2\tr2\nfd12::2\tr2\n{BLOCK_END}\n");
        assert_eq!(block, expected);
    }

    // ----- the file -----

    #[test]
    fn splice_appends_once_and_replaces_a_previous_block() {
        let platform = "127.0.0.1\tlocalhost\n::1\tlocalhost\nfd12::9\tself\n";
        let block1 = format!("{BLOCK_BEGIN}\nfd12::2\tr2\n{BLOCK_END}\n");
        let once = splice_block(platform, &block1);
        assert_eq!(once, format!("{platform}{block1}"));
        let block2 = format!("{BLOCK_BEGIN}\nfd12::3\tr2\n{BLOCK_END}\n");
        let twice = splice_block(&once, &block2);
        assert_eq!(twice, format!("{platform}{block2}"));
        // Empty block: the platform's lines, and nothing else.
        assert_eq!(splice_block(&twice, ""), platform);
    }

    #[test]
    fn splice_tolerates_a_file_without_trailing_newline_and_a_cut_off_block() {
        let block = format!("{BLOCK_BEGIN}\nfd12::2\tr2\n{BLOCK_END}\n");
        assert_eq!(
            splice_block("127.0.0.1\tlocalhost", &block),
            format!("127.0.0.1\tlocalhost\n{block}")
        );
        // A BEGIN with no END (a torn earlier write): everything from the
        // marker on is treated as the old block.
        let torn = format!("127.0.0.1\tlocalhost\n{BLOCK_BEGIN}\nfd12::1\tr1\n");
        assert_eq!(
            splice_block(&torn, &block),
            format!("127.0.0.1\tlocalhost\n{block}")
        );
        assert_eq!(splice_block("", ""), "");
    }

    #[test]
    fn write_in_place_shrinks_and_grows() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("hosts");
        std::fs::write(
            &path,
            "127.0.0.1\tlocalhost\nsome longer old content here\n",
        )
        .unwrap();
        write_in_place(&path, "short\n").unwrap();
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "short\n");
        write_in_place(&path, "a much longer replacement than before\n").unwrap();
        assert_eq!(
            std::fs::read_to_string(&path).unwrap(),
            "a much longer replacement than before\n"
        );
    }

    #[test]
    fn apply_block_leaves_an_unchanged_file_alone() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("hosts");
        let block = format!("{BLOCK_BEGIN}\nfd12::2\tr2\n{BLOCK_END}\n");
        std::fs::write(&path, format!("127.0.0.1\tlocalhost\n{block}")).unwrap();
        let before = std::fs::metadata(&path).unwrap().modified().unwrap();
        std::thread::sleep(Duration::from_millis(20));
        apply_block(&path, &block).unwrap();
        assert_eq!(
            std::fs::metadata(&path).unwrap().modified().unwrap(),
            before
        );
    }

    // ----- the wire -----

    fn reply_with(id: u16, rcode: u8, answers: &[(u16, &[u8])]) -> Vec<u8> {
        let mut packet = Vec::new();
        packet.extend_from_slice(&id.to_be_bytes());
        packet.extend_from_slice(&[0x81, rcode]); // QR+RD, RA=0
        packet.extend_from_slice(&[0, 1]); // one question
        packet.extend_from_slice(&(answers.len() as u16).to_be_bytes());
        packet.extend_from_slice(&[0, 0, 0, 0]);
        // question: 2"r2" 0, A IN
        packet.extend_from_slice(&[2, b'r', b'2', 0, 0, 1, 0, 1]);
        for (rtype, rdata) in answers {
            packet.extend_from_slice(&[0xC0, 0x0C]); // pointer to the question name
            packet.extend_from_slice(&rtype.to_be_bytes());
            packet.extend_from_slice(&[0, 1]); // IN
            packet.extend_from_slice(&[0, 0, 0, 30]); // TTL
            packet.extend_from_slice(&(rdata.len() as u16).to_be_bytes());
            packet.extend_from_slice(rdata);
        }
        packet
    }

    #[test]
    fn decode_reads_a_and_aaaa_records_through_compression_pointers() {
        let v6_octets = v6("fd12::2");
        let IpAddr::V6(six) = v6_octets else {
            unreachable!()
        };
        let reply = reply_with(
            7,
            0,
            &[(QTYPE_A, &[10, 0, 0, 2]), (QTYPE_AAAA, &six.octets())],
        );
        assert_eq!(
            decode_reply(7, &reply),
            Some(Reply::Records(vec![v4("10.0.0.2"), v6("fd12::2")]))
        );
    }

    #[test]
    fn decode_distinguishes_nodata_nxdomain_and_failure() {
        assert_eq!(decode_reply(7, &reply_with(7, 0, &[])), Some(Reply::NoData));
        assert_eq!(
            decode_reply(7, &reply_with(7, 3, &[])),
            Some(Reply::NxDomain)
        );
        assert_eq!(decode_reply(7, &reply_with(7, 2, &[])), Some(Reply::Failed));
        assert_eq!(decode_reply(7, &reply_with(7, 5, &[])), Some(Reply::Failed));
    }

    #[test]
    fn decode_rejects_foreign_or_malformed_replies() {
        assert_eq!(decode_reply(8, &reply_with(7, 0, &[])), None);
        let mut not_a_response = reply_with(7, 0, &[]);
        not_a_response[2] &= !0x80;
        assert_eq!(decode_reply(7, &not_a_response), None);
        let mut cut = reply_with(7, 0, &[(QTYPE_A, &[10, 0, 0, 2])]);
        cut.truncate(cut.len() - 2);
        assert_eq!(decode_reply(7, &cut), None);
        assert_eq!(decode_reply(7, &[0u8; 5]), None);
        // A CNAME-only answer is NODATA for address purposes.
        let cname = reply_with(7, 0, &[(5, &[2, b'r', b'9', 0])]);
        assert_eq!(decode_reply(7, &cname), Some(Reply::NoData));
    }

    #[test]
    fn combine_prefers_records_then_nxdomain_then_silence() {
        let records = Reply::Records(vec![v6("fd12::2")]);
        assert_eq!(
            combine(Reply::NoData, records.clone()),
            Lookup::Addresses(vec![v6("fd12::2")])
        );
        assert_eq!(
            combine(Reply::Failed, records.clone()),
            Lookup::Addresses(vec![v6("fd12::2")])
        );
        assert_eq!(combine(Reply::NxDomain, Reply::Failed), Lookup::Absent);
        assert_eq!(combine(Reply::NoData, Reply::Failed), Lookup::Silent);
        assert_eq!(combine(Reply::Failed, Reply::Failed), Lookup::Silent);
        assert_eq!(combine(Reply::NoData, Reply::NoData), Lookup::Absent);
        assert_eq!(combine(Reply::NxDomain, Reply::NxDomain), Lookup::Absent);
    }

    #[tokio::test]
    async fn a_resolver_that_never_answers_is_silent_within_the_deadline() {
        // A bound UDP socket that nobody reads: the query is sent, no reply
        // ever comes, and the deadline turns that into Silent.
        let sink = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let addr = sink.local_addr().unwrap();
        let started = Instant::now();
        let verdict = lookup("r2", addr, Duration::from_millis(200)).await;
        assert_eq!(verdict, Lookup::Silent);
        assert!(started.elapsed() < Duration::from_secs(2));
    }

    #[tokio::test]
    async fn a_resolver_answer_is_parsed_end_to_end() {
        // A tiny resolver: every A query gets 10.0.0.2, every AAAA query
        // gets NODATA (the name exists, it just has no IPv6 address).
        let server = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let addr = server.local_addr().unwrap();
        tokio::spawn(async move {
            let mut buf = [0u8; 512];
            for _ in 0..2 {
                let (len, from) = server.recv_from(&mut buf).await.unwrap();
                let id = u16::from_be_bytes([buf[0], buf[1]]);
                let qtype = u16::from_be_bytes([buf[len - 4], buf[len - 3]]);
                let reply = if qtype == QTYPE_A {
                    reply_with(id, 0, &[(QTYPE_A, &[10, 0, 0, 2])])
                } else {
                    reply_with(id, 0, &[])
                };
                server.send_to(&reply, from).await.unwrap();
            }
        });
        let verdict = lookup("r2", addr, Duration::from_secs(2)).await;
        assert_eq!(verdict, Lookup::Addresses(vec![v4("10.0.0.2")]));
    }
}
