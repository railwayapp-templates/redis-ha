//! HAProxy configuration generator for Redis HA.
//!
//! Architecture:
//!   - Port 6379 (writes + reads): HTTP health check on each node's /role
//!     endpoint. Only the node that returns 200 (i.e. role=master) is marked
//!     UP — every client connection lands on the current master.
//!   - Port 8404: stats page for observability.
//!
//! There is deliberately no replica-read frontend. TCP passthrough balances
//! per CONNECTION, and the dominant Redis client shape is one long-lived
//! connection — so a "load-balanced read port" pins each client to whichever
//! node it happened to land on, with no staleness bound if that replica's
//! replication link is broken. Replicas exist for failover, not fan-out.
//!
//! The health checks hit the Rust health server running on each redis-sentinel
//! container (HEALTH_CHECK_PORT, default 8080), not Redis directly. This
//! eliminates the need for raw tcp-check sequences in the Redis protocol.

use crate::config::Config;
use crate::nodes::RedisNode;

fn server_entries(nodes: &[RedisNode], health_port: u16, config: &Config) -> String {
    nodes
        .iter()
        .map(|n| {
            format!(
                "    server {} {}:{} check port {} resolvers railway inter {} fastinter {} downinter {}",
                n.name, n.host, n.redis_port, health_port,
                config.check_interval, config.check_fastinter, config.check_downinter
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

pub fn generate_config(config: &Config, nodes: &[RedisNode]) -> String {
    let servers = server_entries(nodes, config.health_port, config);

    format!(
        r#"global
    maxconn {max_conn}
    log stdout format raw local0

defaults
    log global
    mode tcp
    option tcpka
    option clitcpka
    option srvtcpka
    option redispatch
    retries 3
    timeout connect {timeout_connect}
    timeout client {timeout_client}
    timeout server {timeout_server}
    timeout check {timeout_check}

resolvers railway
    parse-resolv-conf
    resolve_retries 3
    timeout resolve 1s
    timeout retry   1s
    hold other      10s
    hold refused    10s
    hold nx         10s
    hold timeout    10s
    hold valid      10s
    hold obsolete   10s

# Stats page for monitoring
listen stats
    bind :::8404 v4v6
    mode http
    stats enable
    stats uri /stats
    stats refresh 10s
    # This proxy's own traffic is not worth logging: the in-container
    # monitoring loop scrapes /stats every few seconds and each scrape opens
    # two connections, so inheriting `log global` here produced ~1.4k
    # lines/hour of "Connect from ::1 ... (stats/HTTP)" — measured at 99% of
    # this service's entire log volume, burying the lines an operator
    # actually needs (backend UP/DOWN, DNS re-resolution, client connects).
    no log

# Write traffic — routed exclusively to the current master.
# The /role health check returns 200 only on the master node.
frontend redis_writes
    bind :::{redis_port} v4v6
    default_backend redis_primary_backend

backend redis_primary_backend
    option httpchk
    http-check send meth GET uri /role
    http-check expect status 200
    # fall 2 + fastinter 500ms: the first failed /role check switches the
    # probe to the fast interval, so a real demotion is confirmed and the
    # server pulled ~500ms after the first failure — but ONE slow or dropped
    # check can no longer RST every client connection on a healthy master.
    # /role runs a redis PING and a Sentinel confirmation (2s timeout each)
    # against `timeout check 3s`, so a single blip under load is expected,
    # and with no replica passing /role a false mark-down is a self-inflicted
    # write outage until `rise 2` readmits the master. shutdown-sessions RSTs
    # every open client connection the moment the server is genuinely marked
    # down, forcing clients to reconnect and land on the new master.
    default-server fall 2 rise 2 on-marked-down shutdown-sessions
{servers}
"#,
        max_conn = config.max_conn,
        timeout_connect = config.timeout_connect,
        timeout_client = config.timeout_client,
        timeout_server = config.timeout_server,
        timeout_check = config.timeout_check,
        redis_port = config.redis_port,
        servers = servers,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config_for_tests() -> Config {
        Config {
            redis_nodes: "redis-1.railway.internal:6379,redis-2.railway.internal:6379"
                .to_string(),
            health_port: 8080,
            redis_port: 6379,
            max_conn: "1000".to_string(),
            timeout_connect: "5s".to_string(),
            timeout_client: "1d".to_string(),
            timeout_server: "1d".to_string(),
            timeout_check: "3s".to_string(),
            check_interval: "3s".to_string(),
            check_fastinter: "500ms".to_string(),
            check_downinter: "500ms".to_string(),
        }
    }

    fn section<'a>(conf: &'a str, header: &str) -> &'a str {
        let start = conf.find(header).expect("section header not found");
        let rest = &conf[start..];
        // A section runs until the next blank line followed by a non-indented
        // line — good enough for this fixed template.
        match rest.find("\n\n") {
            Some(end) => &rest[..end],
            None => rest,
        }
    }

    /// The stats listener must not log: the in-container monitoring loop
    /// scrapes it every few seconds, and inheriting `log global` made that
    /// self-traffic 99% of the service's log volume, burying backend UP/DOWN
    /// and DNS re-resolution lines.
    #[test]
    fn stats_listener_does_not_log_its_own_traffic() {
        let config = config_for_tests();
        let nodes = crate::nodes::parse_nodes(&config.redis_nodes).unwrap();
        let conf = generate_config(&config, &nodes);

        assert!(section(&conf, "listen stats").contains("no log"));
    }

    /// ...and silencing it must not silence the proxies that carry real
    /// traffic: those still inherit `log global` from defaults.
    #[test]
    fn traffic_proxies_still_log() {
        let config = config_for_tests();
        let nodes = crate::nodes::parse_nodes(&config.redis_nodes).unwrap();
        let conf = generate_config(&config, &nodes);

        assert!(conf.contains("defaults\n    log global"));
        assert!(!section(&conf, "frontend redis_writes").contains("no log"));
    }

    /// The read frontend is gone for good: per-connection TCP balancing gave
    /// single-connection Redis clients no balancing at all, just an unbounded
    /// staleness lottery. Locks the removal so it doesn't quietly return.
    #[test]
    fn no_replica_read_frontend() {
        let config = config_for_tests();
        let nodes = crate::nodes::parse_nodes(&config.redis_nodes).unwrap();
        let conf = generate_config(&config, &nodes);

        assert!(!conf.contains("redis_reads"));
        assert!(!conf.contains("redis_replica_backend"));
        assert!(!conf.contains("6380"));
    }

    /// fall 2, not 1: one slow /role check on a healthy master must not RST
    /// every client connection (fastinter re-probes 500ms later, so a real
    /// demotion is still confirmed almost immediately). /role's internal
    /// budget (PING + Sentinel confirmation, 2s each) can legitimately
    /// exceed `timeout check 3s` under load.
    #[test]
    fn write_backend_tolerates_one_failed_check() {
        let config = config_for_tests();
        let nodes = crate::nodes::parse_nodes(&config.redis_nodes).unwrap();
        let conf = generate_config(&config, &nodes);

        // Whole-conf assert: the write backend is the only backend in this
        // template, and `section()`'s bare find() would land on the
        // frontend's `default_backend redis_primary_backend` line instead.
        assert!(
            conf.contains("default-server fall 2 rise 2 on-marked-down shutdown-sessions")
        );
        assert!(!conf.contains("fall 1"));
    }
}
