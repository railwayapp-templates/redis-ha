//! HAProxy configuration generator for Redis HA.
//!
//! Architecture:
//!   - Port 6379 (writes): HTTP health check on each node's /role endpoint.
//!     Only the node that returns 200 (i.e. role=master) is marked UP.
//!   - Port 6380 (reads): HTTP health check on each node's /health endpoint.
//!     Any healthy Redis node (master or replica) receives read traffic.
//!   - Port 8404: stats page for observability.
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
    # fall 1: one failed /role check is enough to pull a server out of rotation.
    # The check is Sentinel-verified so false positives are unlikely; acting
    # immediately minimises the window where the old master can still receive
    # writes through HAProxy.  shutdown-sessions RSTs every open client
    # connection the moment the server is marked down, forcing clients to
    # reconnect and land on the new master.
    default-server fall 1 rise 2 on-marked-down shutdown-sessions
{servers}

# Read traffic — load-balanced across all healthy nodes (master + replicas).
# The /health check returns 200 on any node that Redis answers PING.
frontend redis_reads
    bind :::6380 v4v6
    default_backend redis_replica_backend

backend redis_replica_backend
    balance leastconn
    option httpchk
    http-check send meth GET uri /health
    http-check expect status 200
    # fall 2 is fine for reads — an occasional blip is less harmful than for writes.
    default-server fall 2 rise 1 on-marked-down shutdown-sessions
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
            timeout_client: "30m".to_string(),
            timeout_server: "30m".to_string(),
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
        assert!(!section(&conf, "frontend redis_reads").contains("no log"));
    }
}
