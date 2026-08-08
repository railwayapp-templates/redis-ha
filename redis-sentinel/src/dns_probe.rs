//! Authoritative "is anything still behind this name" probe for prune
//! decisions.
//!
//! Railway's private-network resolver (stacker-dnssrv, a CoreDNS plugin)
//! answers each query with a live control-plane lookup and returns:
//!
//! - **NXDOMAIN** when the control plane affirms the name has zero live
//!   container IPs — a deleted service, or a shorter no-container window
//!   such as mid-redeploy. Two control planes are consulted (regional
//!   discovery with a legacy fallback) and NXDOMAIN surfaces only when
//!   neither knows an IP.
//! - **SERVFAIL** when the control plane cannot be reached at all.
//!
//! A partition therefore can never manufacture NXDOMAIN: an unreachable
//! peer's records are still served (the control plane's view does not
//! depend on the querying host's data-plane connectivity), and a host cut
//! off from the control plane gets SERVFAIL. That makes a *persistent*
//! NXDOMAIN the one signal available from inside the container that
//! distinguishes "nothing runs behind this name anymore" from "unreachable
//! right now" — exactly what the Sentinel prune needs before it may forget
//! peers while holding a live minority (see `crate::quorum`). Transient
//! NXDOMAIN windows (a redeploy) are filtered by the caller requiring the
//! verdict continuously across its whole dwell.
//!
//! The probe is a single hand-built UDP query to the first `nameserver` in
//! `/etc/resolv.conf`, judged ONLY by the response RCODE:
//!
//! - `NXDOMAIN` (3) → [`NameVerdict::Gone`].
//! - Everything else → [`NameVerdict::ExistsOrUnknown`]: records returned,
//!   NODATA, SERVFAIL, REFUSED, a malformed reply, a timeout, or no
//!   resolver at all.
//!
//! The asymmetry is the point: every failure mode of the resolver reads as
//! "unknown" and keeps the caller's fence up. Only a resolver that is alive
//! and answering can ever produce `Gone`. This is also why getaddrinfo is
//! not used here — musl folds NODATA into `EAI_NONAME`, blurring exactly
//! the distinction the verdict exists to draw.
//!
//! The query asks for AAAA (Railway's private network is IPv6-only); the
//! verdict does not depend on the record type, since NXDOMAIN is a property
//! of the name, not of the type queried.

use std::net::{IpAddr, SocketAddr};
use std::time::Duration;
use tokio::net::UdpSocket;
use tokio::time::timeout;

/// What a completed probe learned about a name.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NameVerdict {
    /// The resolver answered NXDOMAIN: the name authoritatively no longer
    /// exists. On Railway this means the service was deleted (or renamed —
    /// in which case the peer re-registers under its new name via gossip).
    Gone,
    /// Anything short of an authoritative NXDOMAIN, including every error.
    ExistsOrUnknown,
}

/// Probe `name` against the system resolver, bounding the whole attempt by
/// `deadline`. Never errors: anything that prevents a verdict is
/// [`NameVerdict::ExistsOrUnknown`].
pub async fn probe_name(name: &str, deadline: Duration) -> NameVerdict {
    // An IP literal is not a name and can never be proven deleted — querying
    // one AS a name would NXDOMAIN unconditionally and fabricate deletion
    // proof. Guards against any peer still announced by raw address (e.g. a
    // Sentinel from an image predating `sentinel announce-ip`).
    if name.parse::<IpAddr>().is_ok() {
        return NameVerdict::ExistsOrUnknown;
    }
    let Some(nameserver) = first_nameserver(&read_resolv_conf()) else {
        return NameVerdict::ExistsOrUnknown;
    };
    match timeout(deadline, query_rcode(name, nameserver)).await {
        Ok(Some(3)) => NameVerdict::Gone,
        _ => NameVerdict::ExistsOrUnknown,
    }
}

fn read_resolv_conf() -> String {
    std::fs::read_to_string("/etc/resolv.conf").unwrap_or_default()
}

/// The first `nameserver` entry, port 53. `None` when there is none — a
/// container with no resolver has no opinion on anything.
fn first_nameserver(resolv_conf: &str) -> Option<SocketAddr> {
    resolv_conf
        .lines()
        .map(str::trim)
        .filter(|line| !line.starts_with('#') && !line.starts_with(';'))
        .find_map(|line| {
            let mut tokens = line.split_whitespace();
            (tokens.next() == Some("nameserver"))
                .then(|| tokens.next())
                .flatten()
                .and_then(|addr| addr.parse::<IpAddr>().ok())
        })
        .map(|ip| SocketAddr::new(ip, 53))
}

/// One UDP round-trip; `None` when no matching, well-formed reply arrived.
async fn query_rcode(name: &str, nameserver: SocketAddr) -> Option<u8> {
    let query_id = query_id();
    let packet = encode_query(query_id, name)?;

    let bind_addr: SocketAddr = if nameserver.is_ipv6() {
        "[::]:0".parse().ok()?
    } else {
        "0.0.0.0:0".parse().ok()?
    };
    let socket = UdpSocket::bind(bind_addr).await.ok()?;
    socket.connect(nameserver).await.ok()?;
    socket.send(&packet).await.ok()?;

    let mut reply = [0u8; 512];
    loop {
        let len = socket.recv(&mut reply).await.ok()?;
        if let Some(rcode) = decode_rcode(query_id, &reply[..len]) {
            return Some(rcode);
        }
        // A reply for some other/stale query id: keep listening until the
        // caller's deadline cuts us off.
    }
}

/// Not cryptographic — the id only pairs a reply with its query.
fn query_id() -> u16 {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or(0);
    (nanos ^ (std::process::id() << 8)) as u16
}

/// A standard recursion-desired AAAA question. `None` when the name cannot
/// be encoded (empty, label over 63 bytes, name over 255).
fn encode_query(id: u16, name: &str) -> Option<Vec<u8>> {
    let name = name.strip_suffix('.').unwrap_or(name);
    if name.is_empty() || name.len() > 255 {
        return None;
    }
    let mut packet = Vec::with_capacity(name.len() + 18);
    packet.extend_from_slice(&id.to_be_bytes());
    packet.extend_from_slice(&[
        0x01, 0x00, // flags: RD
        0x00, 0x01, // QDCOUNT 1
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // AN/NS/AR 0
    ]);
    for label in name.split('.') {
        if label.is_empty() || label.len() > 63 {
            return None;
        }
        packet.push(label.len() as u8);
        packet.extend_from_slice(label.as_bytes());
    }
    packet.push(0); // root label
    packet.extend_from_slice(&[0x00, 0x1c]); // QTYPE AAAA
    packet.extend_from_slice(&[0x00, 0x01]); // QCLASS IN
    Some(packet)
}

/// The RCODE of a reply that matches `id` and is actually a response;
/// `None` for anything else.
fn decode_rcode(id: u16, reply: &[u8]) -> Option<u8> {
    if reply.len() < 12 {
        return None;
    }
    if reply[0..2] != id.to_be_bytes() {
        return None;
    }
    let is_response = reply[2] & 0x80 != 0;
    if !is_response {
        return None;
    }
    Some(reply[3] & 0x0F)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn first_nameserver_takes_the_first_entry() {
        let conf = "# comment\nsearch railway.internal\nnameserver fd12::10\nnameserver 1.1.1.1\n";
        assert_eq!(
            first_nameserver(conf),
            Some(SocketAddr::new("fd12::10".parse().unwrap(), 53))
        );
    }

    #[test]
    fn no_nameserver_means_no_probe() {
        assert_eq!(first_nameserver("search foo\n"), None);
        assert_eq!(first_nameserver(""), None);
        assert_eq!(first_nameserver("nameserver not-an-ip\n"), None);
    }

    #[tokio::test]
    async fn ip_literals_are_never_gone() {
        // No resolver round-trip happens: verdicts return immediately.
        assert_eq!(
            probe_name("192.168.158.6", Duration::from_millis(1)).await,
            NameVerdict::ExistsOrUnknown
        );
        assert_eq!(
            probe_name("fd12::10", Duration::from_millis(1)).await,
            NameVerdict::ExistsOrUnknown
        );
    }

    #[test]
    fn encode_rejects_unencodable_names() {
        assert!(encode_query(1, "").is_none());
        assert!(encode_query(1, "a..b").is_none());
        assert!(encode_query(1, &"x".repeat(64)).is_none());
        assert!(encode_query(1, &"a.".repeat(200)).is_none());
    }

    #[test]
    fn encode_builds_a_wellformed_question() {
        let packet = encode_query(0xBEEF, "redis-2.railway.internal.").unwrap();
        assert_eq!(&packet[0..2], &[0xBE, 0xEF]);
        assert_eq!(packet[2], 0x01); // RD
        assert_eq!(&packet[4..6], &[0x00, 0x01]); // one question
        // QNAME: 7"redis-2" 7"railway" 8"internal" 0
        let qname_start = 12;
        assert_eq!(packet[qname_start], 7);
        assert_eq!(&packet[qname_start + 1..qname_start + 8], b"redis-2");
        let end = packet.len();
        assert_eq!(&packet[end - 4..], &[0x00, 0x1c, 0x00, 0x01]); // AAAA IN
    }

    #[test]
    fn decode_reads_rcode_only_from_a_matching_response() {
        let mut reply = vec![0u8; 12];
        reply[0] = 0xBE;
        reply[1] = 0xEF;
        reply[2] = 0x80; // QR
        reply[3] = 0x03; // NXDOMAIN
        assert_eq!(decode_rcode(0xBEEF, &reply), Some(3));
        // Wrong id.
        assert_eq!(decode_rcode(0xBEE0, &reply), None);
        // A query, not a response.
        reply[2] = 0x00;
        assert_eq!(decode_rcode(0xBEEF, &reply), None);
        // Truncated garbage.
        assert_eq!(decode_rcode(0xBEEF, &reply[..5]), None);
    }

    #[test]
    fn decode_masks_the_rcode_nibble() {
        let mut reply = vec![0u8; 12];
        reply[0] = 0x00;
        reply[1] = 0x01;
        reply[2] = 0x80;
        reply[3] = 0xA0; // RA set, RCODE 0 (NOERROR)
        assert_eq!(decode_rcode(1, &reply), Some(0));
    }
}
