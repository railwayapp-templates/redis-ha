use anyhow::{anyhow, Result};

pub struct RedisNode {
    pub name: String,
    pub host: String,
    pub redis_port: u16,
}

/// Parse the REDIS_NODES env var.
///
/// Format: "hostname:port,hostname:port,..."
/// Example: "Redis-1.railway.internal:6379,Redis-2.railway.internal:6379"
pub fn parse_nodes(redis_nodes: &str) -> Result<Vec<RedisNode>> {
    redis_nodes
        .split(',')
        .map(|entry| {
            let entry = entry.trim();
            let parts: Vec<&str> = entry.splitn(2, ':').collect();
            if parts.len() != 2 {
                return Err(anyhow!(
                    "invalid node format: '{}'. Expected hostname:port",
                    entry
                ));
            }
            let host = parts[0].to_string();
            let redis_port = parts[1]
                .parse::<u16>()
                .map_err(|_| anyhow!("invalid port in '{}': {}", entry, parts[1]))?;
            let name = host.split('.').next().unwrap_or(&host).to_string();

            Ok(RedisNode { name, host, redis_port })
        })
        .collect()
}
