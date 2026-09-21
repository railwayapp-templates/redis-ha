//! Coordinated password rotation. PREPARE gives every existing default user
//! a temporary overlap, preserving quorum while outbound auth is changed.
//! MEMBER removes the old password and restarts only the local Sentinel.
use crate::{
    atomic_write::write_atomic,
    config::Config,
    redis_conf::{persisted_requirepass, quote_conf_value},
};
use anyhow::{Context, Result};
use axum::{
    http::{header, HeaderMap, StatusCode},
    Json,
};
use base64::Engine;
use redis::{aio::MultiplexedConnection, Client};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{path::Path, time::Duration};
use subtle::ConstantTimeEq;
use tokio::sync::{Mutex, Notify};

static ROTATION: Mutex<()> = Mutex::const_new(());
pub static RESTART_SENTINEL: Notify = Notify::const_new();

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Rotation {
    operation: Operation,
    new_password: String,
    current_password: String,
}
#[derive(Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum Operation {
    Preflight,
    Prepare,
    Database,
    Member,
    Verify,
}

/// Existing watchers cache URLs, but must authenticate using the live pin
/// when reconnecting after a rotation. Credential-less probes stay so.
pub fn active_url(original: impl AsRef<str>) -> String {
    let original = original.as_ref();
    let Ok(mut url) = url::Url::parse(original) else {
        return original.to_string();
    };
    if url.password().is_some() {
        if let Ok(config) = Config::from_env() {
            if let Some(password) = persisted_requirepass(&config.data_dir) {
                let _ = url.set_password(Some(&password));
            }
        }
    }
    url.to_string()
}
async fn connect(port: u16, password: &str) -> Result<MultiplexedConnection> {
    let url = crate::sentinel_query::build_redis_url("127.0.0.1", port, password);
    let mut conn = Client::open(url)?
        .get_multiplexed_async_connection()
        .await?;
    redis::cmd("PING").query_async::<String>(&mut conn).await?;
    Ok(conn)
}
async fn rejects_password(port: u16, password: &str) -> Result<bool> {
    match connect(port, password).await {
        Ok(_) => Ok(false),
        Err(error)
            if error
                .downcast_ref::<redis::RedisError>()
                .is_some_and(|error| error.kind() == redis::ErrorKind::AuthenticationFailed) =>
        {
            Ok(true)
        }
        Err(error) => Err(error), // A transport failure is not proof of revocation.
    }
}

async fn connect_either(port: u16, request: &Rotation) -> Result<MultiplexedConnection> {
    match connect(port, &request.new_password).await {
        Ok(conn) => Ok(conn),
        Err(_) => connect(port, &request.current_password).await,
    }
}

fn replication_ready(info: &str, expected_members: usize) -> bool {
    let fields: std::collections::HashMap<_, _> = info
        .lines()
        .filter_map(|line| line.trim().split_once(':'))
        .collect();
    match fields.get("role").copied() {
        Some("master") => {
            let online = fields
                .iter()
                .filter(|(name, value)| {
                    name.strip_prefix("slave")
                        .is_some_and(|suffix| suffix.parse::<usize>().is_ok())
                        && value.split(',').any(|field| field == "state=online")
                })
                .count();
            online >= expected_members.saturating_sub(1)
        }
        Some("slave") => {
            fields.get("master_link_status") == Some(&"up")
                && fields.get("master_sync_in_progress") == Some(&"0")
        }
        _ => false,
    }
}
async fn wait_replication(data: &mut MultiplexedConnection, config: &Config) -> Result<()> {
    let expected = config
        .sentinel_hosts
        .split(',')
        .filter(|host| !host.trim().is_empty())
        .count();
    loop {
        let info: String = redis::cmd("INFO")
            .arg("replication")
            .query_async(data)
            .await?;
        if replication_ready(&info, expected) {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

pub async fn rotate(
    headers: HeaderMap,
    Json(request): Json<Rotation>,
) -> (StatusCode, Json<Value>) {
    let _guard = ROTATION.lock().await;
    let config = match Config::from_env() {
        Ok(c) => c,
        Err(_) => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"error":"configuration unavailable"})),
            )
        }
    };
    let Some(active) = persisted_requirepass(&config.data_dir) else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error":"credential pin not ready"})),
        );
    };
    let expected = format!("railway:{active}");
    let supplied = headers
        .get(header::AUTHORIZATION)
        .and_then(|h| h.to_str().ok())
        .and_then(|h| h.split_once(' '))
        .filter(|(scheme, _)| scheme.eq_ignore_ascii_case("basic"))
        .and_then(|(_, token)| base64::engine::general_purpose::STANDARD.decode(token).ok());
    if !supplied.is_some_and(|s| bool::from(s.as_slice().ct_eq(expected.as_bytes()))) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(json!({"error":"unauthorized"})),
        );
    }
    if request.new_password.is_empty()
        || request.new_password.len() > 1024
        || request.new_password.contains(['\0', '\n', '\r'])
    {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error":"invalid password"})),
        );
    }
    match tokio::time::timeout(Duration::from_secs(35), apply(&config, request)).await {
        Ok(Ok(value)) => (StatusCode::OK, Json(value)),
        _ => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error":"credential rotation could not be verified"})),
        ),
    }
}

async fn apply(config: &Config, request: Rotation) -> Result<Value> {
    let mut data = connect_either(config.redis_port, &request).await?;
    match request.operation {
        Operation::Preflight => {
            wait_replication(&mut data, config).await?;
            connect(config.redis_port, &request.new_password).await?;
            let mut sentinel = connect(config.sentinel_port, &request.new_password).await?;
            redis::cmd("SENTINEL")
                .arg("CKQUORUM")
                .arg(&config.redis_master_name)
                .query_async::<String>(&mut sentinel)
                .await?;
        }
        Operation::Prepare => {
            write_atomic(
                Path::new(&format!("{}/.railway_rotation", config.data_dir)),
                &serde_json::to_string(&request)?,
                Some(0o600),
            )?;
            // Adding a password leaves the same user, privileges and original
            // credential intact. No separate internal account is introduced.
            redis::cmd("ACL")
                .arg("SETUSER")
                .arg("default")
                .arg(format!(">{}", request.new_password))
                .query_async::<()>(&mut data)
                .await?;
            redis::cmd("CONFIG")
                .arg("SET")
                .arg("masterauth")
                .arg(&request.new_password)
                .query_async::<()>(&mut data)
                .await?;
            redis::cmd("CONFIG")
                .arg("REWRITE")
                .query_async::<()>(&mut data)
                .await?;
            let mut sentinel = connect_either(config.sentinel_port, &request).await?;
            redis::cmd("ACL")
                .arg("SETUSER")
                .arg("default")
                .arg(format!(">{}", request.new_password))
                .query_async::<()>(&mut sentinel)
                .await?;
            redis::cmd("SENTINEL")
                .arg("CONFIG")
                .arg("SET")
                .arg("sentinel-pass")
                .arg(&request.new_password)
                .query_async::<()>(&mut sentinel)
                .await?;
            redis::cmd("SENTINEL")
                .arg("SET")
                .arg(&config.redis_master_name)
                .arg("auth-pass")
                .arg(&request.new_password)
                .query_async::<()>(&mut sentinel)
                .await?;
            redis::cmd("SENTINEL")
                .arg("FLUSHCONFIG")
                .query_async::<()>(&mut sentinel)
                .await?;
        }
        Operation::Database => {
            connect(config.redis_port, &request.new_password).await?;
        } // A prepared primary must accept the target.
        Operation::Member => {
            redis::cmd("CONFIG")
                .arg("SET")
                .arg("masterauth")
                .arg(&request.new_password)
                .arg("requirepass")
                .arg(&request.new_password)
                .query_async::<()>(&mut data)
                .await?;
            redis::cmd("CONFIG")
                .arg("REWRITE")
                .query_async::<()>(&mut data)
                .await?;
            anyhow::ensure!(
                persisted_requirepass(&config.data_dir).as_deref() == Some(&request.new_password),
                "pin differs"
            );
            if connect(config.sentinel_port, &request.new_password)
                .await
                .is_err()
                || (request.current_password != request.new_password
                    && !rejects_password(config.sentinel_port, &request.current_password).await?)
            {
                RESTART_SENTINEL.notify_one();
                // The supervisor rewrites AFTER stopping Sentinel, because
                // Sentinel flushes its old in-memory config when it exits.
                loop {
                    if connect(config.sentinel_port, &request.new_password)
                        .await
                        .is_ok()
                        && (request.current_password == request.new_password
                            || rejects_password(config.sentinel_port, &request.current_password)
                                .await?)
                    {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
            }
            // A listening port is not a quorum member yet. Wait for peer
            // connections before allowing the next Sentinel to restart.
            loop {
                let mut sentinel = connect(config.sentinel_port, &request.new_password).await?;
                let peers: Vec<Vec<String>> = redis::cmd("SENTINEL")
                    .arg("SENTINELS")
                    .arg(&config.redis_master_name)
                    .query_async(&mut sentinel)
                    .await?;
                let connected = peers
                    .iter()
                    .filter(|peer| {
                        peer.chunks_exact(2).any(|field| {
                            field[0] == "flags"
                                && !field[1].contains("disconnected")
                                && !field[1].contains("s_down")
                                && !field[1].contains("o_down")
                        })
                    })
                    .count()
                    + 1;
                let expected = config
                    .sentinel_hosts
                    .split(',')
                    .filter(|host| !host.trim().is_empty())
                    .count();
                if connected >= expected / 2 + 1 && connected >= config.sentinel_quorum as usize {
                    redis::cmd("SENTINEL")
                        .arg("CKQUORUM")
                        .arg(&config.redis_master_name)
                        .query_async::<String>(&mut sentinel)
                        .await?;
                    break;
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }
        Operation::Verify => {
            wait_replication(&mut data, config).await?;
            connect(config.redis_port, &request.new_password).await?;
            let mut sentinel = connect(config.sentinel_port, &request.new_password).await?;
            redis::cmd("SENTINEL")
                .arg("CKQUORUM")
                .arg(&config.redis_master_name)
                .query_async::<String>(&mut sentinel)
                .await?;
            anyhow::ensure!(
                persisted_requirepass(&config.data_dir).as_deref() == Some(&request.new_password),
                "pin differs"
            );
            if request.current_password != request.new_password {
                anyhow::ensure!(
                    rejects_password(config.redis_port, &request.current_password).await?,
                    "old data password still accepted"
                );
                anyhow::ensure!(
                    rejects_password(config.sentinel_port, &request.current_password).await?,
                    "old Sentinel password still accepted"
                );
            }
            match std::fs::remove_file(format!("{}/.railway_rotation", config.data_dir)) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(e.into()),
            }
        }
    }
    let info: String = redis::cmd("INFO")
        .arg("replication")
        .query_async(&mut data)
        .await?;
    Ok(json!({"version":1, "leader":info.lines().any(|line| line.trim() == "role:master")}))
}

/// Preserve Sentinel's learned topology, epochs and IDs. Replace only the
/// credential directives, dropping the temporary default-user ACL overlap.
fn rewrite_sentinel(original: &str, password: &str) -> String {
    let mut lines = Vec::new();
    for line in original.lines() {
        let fields: Vec<_> = line.split_whitespace().collect();
        match fields.as_slice() {
            ["requirepass", ..] => {
                lines.push(format!("requirepass {}", quote_conf_value(password)))
            }
            ["sentinel", "sentinel-pass", ..] => lines.push(format!(
                "sentinel sentinel-pass {}",
                quote_conf_value(password)
            )),
            ["sentinel", "auth-pass", name, ..] => lines.push(format!(
                "sentinel auth-pass {name} {}",
                quote_conf_value(password)
            )),
            ["user", "default", rules @ ..] => {
                // ACL LIST/FLUSHCONFIG writes hashes, never raw secrets.
                // Keep permission/key/channel restrictions exactly as stored.
                let permissions: Vec<_> = rules
                    .iter()
                    .copied()
                    .filter(|rule| {
                        !rule.starts_with('#')
                            && !rule.starts_with('>')
                            && !rule.starts_with('<')
                            && !rule.starts_with('!')
                            && *rule != "nopass"
                            && *rule != "resetpass"
                    })
                    .collect();
                lines.push(format!(
                    "user default {} resetpass {}",
                    permissions.join(" "),
                    format!(
                        "#{:x}",
                        <sha2::Sha256 as sha2::Digest>::digest(password.as_bytes())
                    )
                ));
            }
            _ => lines.push(line.to_string()),
        }
    }
    format!("{}\n", lines.join("\n"))
}
pub fn rewrite_sentinel_password(data_dir: &str) -> Result<()> {
    let password = persisted_requirepass(data_dir).context("missing active password")?;
    let path = format!("{data_dir}/sentinel.conf");
    let content = std::fs::read_to_string(&path)?;
    write_atomic(
        Path::new(&path),
        &rewrite_sentinel(&content, &password),
        Some(0o600),
    )?;
    Ok(())
}

/// Preserve the staged overlap across a container restart. The journal is
/// written before live ACL changes, so even an interrupted PREPARE is safe.
pub fn staged_config(config: &str, data_dir: &str, active_password: &str) -> Result<String> {
    let path = format!("{data_dir}/.railway_rotation");
    let body = match std::fs::read(path) {
        Ok(body) => body,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(config.into()),
        Err(e) => return Err(e.into()),
    };
    let request: Rotation = serde_json::from_slice(&body)?;
    if active_password == request.new_password {
        return Ok(config.into());
    }
    let mut lines: Vec<String> = config.lines().map(String::from).collect();
    // Generated data configs use the default unrestricted user. If a custom
    // ACL exists, append only passwords to that same rule without widening it.
    let passwords = format!(
        "#{:x} #{:x}",
        <sha2::Sha256 as sha2::Digest>::digest(request.current_password.as_bytes()),
        <sha2::Sha256 as sha2::Digest>::digest(request.new_password.as_bytes())
    );
    if let Some(line) = lines
        .iter_mut()
        .find(|line| line.starts_with("user default "))
    {
        line.push_str(&format!(" {passwords}"));
    } else {
        lines.push(format!("user default on ~* &* +@all {passwords}"));
    }
    lines.retain(|line| !line.starts_with("masterauth "));
    lines.push(format!(
        "masterauth {}",
        quote_conf_value(&request.new_password)
    ));
    Ok(format!("{}\n", lines.join("\n")))
}

/// Complete an interrupted local Sentinel restart before spawning it again.
pub fn reconcile_sentinel_boot(data_dir: &str) -> Result<()> {
    let body = match std::fs::read(format!("{data_dir}/.railway_rotation")) {
        Ok(body) => body,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e.into()),
    };
    let request: Rotation = serde_json::from_slice(&body)?;
    if persisted_requirepass(data_dir).as_deref() == Some(&request.new_password) {
        rewrite_sentinel_password(data_dir)?;
    }
    Ok(())
}

/// A restarted member can have missed the local adoption step. Authenticate
/// against a live Sentinel and the primary it names before adopting a staged
/// or environment password; a manual variable edit alone never wins.
pub async fn adopt_proven_boot_password(config: &mut Config) -> bool {
    let Some(pin) = persisted_requirepass(&config.data_dir) else {
        return false;
    };
    let staged = std::fs::read(format!("{}/.railway_rotation", config.data_dir))
        .ok()
        .and_then(|body| serde_json::from_slice::<Rotation>(&body).ok())
        .map(|r| r.new_password);
    let candidate = staged.unwrap_or_else(|| config.redis_password.clone());
    if candidate == pin {
        return false;
    }
    let probe = async {
        for peer in config.sentinel_hosts.split(',').map(str::trim) {
            let url = url::Url::parse(&format!("redis://{peer}"))?;
            let host = url.host_str().context("missing peer host")?;
            let client = Client::open(crate::sentinel_query::build_redis_url(
                host,
                url.port().unwrap_or(26379),
                &candidate,
            ))?;
            let Ok(mut sentinel) = client.get_multiplexed_async_connection().await else {
                continue;
            };
            let Ok(master) = redis::cmd("SENTINEL")
                .arg("GET-MASTER-ADDR-BY-NAME")
                .arg(&config.redis_master_name)
                .query_async::<Vec<String>>(&mut sentinel)
                .await
            else {
                continue;
            };
            if master.len() != 2 {
                continue;
            }
            let port = master[1].parse::<u16>()?;
            // Loopback of another local test node is valid; in production the
            // learned private address belongs to the current primary.
            let client = Client::open(crate::sentinel_query::build_redis_url(
                &master[0], port, &candidate,
            ))?;
            let Ok(mut data) = client.get_multiplexed_async_connection().await else {
                continue;
            };
            let info: String = redis::cmd("INFO")
                .arg("replication")
                .query_async(&mut data)
                .await?;
            if info.lines().any(|line| line.trim() == "role:master") {
                return Ok::<(), anyhow::Error>(());
            }
        }
        anyhow::bail!("no primary accepted candidate")
    };
    if matches!(
        tokio::time::timeout(Duration::from_secs(10), probe).await,
        Ok(Ok(()))
    ) {
        config.redis_password = candidate;
        true
    } else {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn replication_requires_synced_members() {
        assert!(!replication_ready(
            "role:slave\nmaster_link_status:up\nmaster_sync_in_progress:1",
            3
        ));
        assert!(replication_ready(
            "role:slave\nmaster_link_status:up\nmaster_sync_in_progress:0",
            3
        ));
        assert!(!replication_ready(
            "role:master\nslave0:ip=a,state=online\nslave1:ip=b,state=wait_bgsave",
            3
        ));
        assert!(replication_ready(
            "role:master\nslave0:ip=a,state=online\nslave1:ip=b,state=online",
            3
        ));
    }
    #[test]
    fn changes_credentials_without_forgetting_topology() {
        let original = "requirepass old\nsentinel sentinel-pass old\nsentinel auth-pass cluster old\nuser default on #old #new ~* +@all\nsentinel known-sentinel cluster node 26379 id\nsentinel config-epoch cluster 42\n";
        let updated = rewrite_sentinel(original, "p:a ss");
        assert!(updated.contains("requirepass \"p:a ss\""));
        assert!(updated.contains("sentinel auth-pass cluster \"p:a ss\""));
        assert!(updated.contains("sentinel known-sentinel cluster node 26379 id"));
        assert!(updated.contains("sentinel config-epoch cluster 42"));
        assert!(updated.contains("user default on ~* +@all resetpass #"));
        assert!(!updated.contains("#old"));
        assert_eq!(updated, rewrite_sentinel(&updated, "p:a ss"));
    }
}
