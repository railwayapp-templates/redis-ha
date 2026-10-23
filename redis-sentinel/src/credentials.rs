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
use tracing::warn;

pub(crate) static ROTATION: Mutex<()> = Mutex::const_new(());
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
impl Operation {
    fn name(&self) -> &'static str {
        match self {
            Operation::Preflight => "preflight",
            Operation::Prepare => "prepare",
            Operation::Database => "database",
            Operation::Member => "member",
            Operation::Verify => "verify",
        }
    }
}

/// The rotation staged on this volume, as `(current, new)`, while a
/// `.railway_rotation` journal exists (PREPARE writes it, VERIFY removes it).
pub(crate) fn staged_rotation(data_dir: &str) -> Option<(String, String)> {
    let body = std::fs::read(format!("{data_dir}/.railway_rotation")).ok()?;
    let request: Rotation = serde_json::from_slice(&body).ok()?;
    Some((request.current_password, request.new_password))
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
        .await
        .with_context(|| format!("connecting to 127.0.0.1:{port}"))?;
    redis::cmd("PING")
        .query_async::<String>(&mut conn)
        .await
        .with_context(|| format!("authenticating to 127.0.0.1:{port}"))?;
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
    let operation = request.operation.name();
    let secrets = [
        request.new_password.clone(),
        request.current_password.clone(),
    ];
    match tokio::time::timeout(Duration::from_secs(35), apply(&config, request)).await {
        Ok(Ok(value)) => (StatusCode::OK, Json(value)),
        Ok(Err(error)) => {
            let reason = refusal_reason(&error, &secrets);
            warn!(operation, reason, "credential operation refused");
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(
                    json!({"error": format!("credential rotation could not be verified: {reason}")}),
                ),
            )
        }
        Err(_) => {
            let reason = format!("{operation} timed out after 35s");
            warn!(operation, reason, "credential operation refused");
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(
                    json!({"error": format!("credential rotation could not be verified: {reason}")}),
                ),
            )
        }
    }
}

/// The 503 body names the check that refused, so a stuck rotation can be
/// read off backboard's error instead of a diagnostic build: the error chain
/// in one line, every password the request carried cut out, bounded to 200
/// characters.
fn refusal_reason(error: &anyhow::Error, secrets: &[String]) -> String {
    let mut reason = error
        .chain()
        .map(|cause| cause.to_string().replace(['\n', '\r'], " "))
        .collect::<Vec<_>>()
        .join(": ");
    for secret in secrets.iter().filter(|secret| !secret.is_empty()) {
        reason = reason.replace(secret.as_str(), "<redacted>");
    }
    reason.chars().take(200).collect()
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
                .await
                .context("SENTINEL CKQUORUM")?;
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
            // The restarted Sentinel must dial out with the new password, not
            // with whatever a stale writer or a conf the rewrite never reached
            // left in sentinel-pass / auth-pass — otherwise every peer goes
            // s_down and the quorum wait below never ends.
            crate::credential_reconcile::reconcile_once(
                &config.data_dir,
                config.sentinel_port,
                &config.redis_master_name,
                &request.new_password,
            )
            .await;
            // A listening port is not a quorum member yet. Wait for peer
            // connections before allowing the next Sentinel to restart.
            loop {
                let mut sentinel = connect(config.sentinel_port, &request.new_password).await?;
                let peers: Vec<Vec<String>> = redis::cmd("SENTINEL")
                    .arg("SENTINELS")
                    .arg(&config.redis_master_name)
                    .query_async(&mut sentinel)
                    .await
                    .context("SENTINEL SENTINELS")?;
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
                        .await
                        .context("SENTINEL CKQUORUM")?;
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
                .await
                .context("SENTINEL CKQUORUM")?;
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
///
/// Directives are matched the way Redis reads them — case-insensitively,
/// any whitespace, quoted or bare values — so a conf written by an older
/// build (bare values, in that build's order) or rewritten since by Sentinel
/// itself (which writes `sentinel-pass` and `auth-pass` bare and
/// `requirepass` quoted) comes out the same. A conf that requires auth but
/// carries no `sentinel sentinel-pass` line gets one: Sentinel would fall
/// back to `requirepass` for outbound auth, but an explicit line is what the
/// reconcile loop and operators read.
fn rewrite_sentinel(original: &str, password: &str) -> String {
    let mut lines = Vec::new();
    let mut requires_auth = false;
    let mut has_sentinel_pass = false;
    for line in original.lines() {
        let fields: Vec<_> = line.split_whitespace().collect();
        let keyword = |index: usize, expected: &str| {
            fields
                .get(index)
                .is_some_and(|field| field.eq_ignore_ascii_case(expected))
        };
        if keyword(0, "requirepass") {
            requires_auth = true;
            lines.push(format!("requirepass {}", quote_conf_value(password)));
            continue;
        }
        if keyword(0, "sentinel") && keyword(1, "sentinel-pass") {
            has_sentinel_pass = true;
            lines.push(format!(
                "sentinel sentinel-pass {}",
                quote_conf_value(password)
            ));
            continue;
        }
        if keyword(0, "sentinel") && keyword(1, "auth-pass") && fields.len() >= 3 {
            lines.push(format!(
                "sentinel auth-pass {} {}",
                fields[2],
                quote_conf_value(password)
            ));
            continue;
        }
        match fields.as_slice() {
            [user, "default", rules @ ..] if user.eq_ignore_ascii_case("user") => {
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
    if requires_auth && !has_sentinel_pass {
        let after_requirepass = lines
            .iter()
            .position(|line| line.starts_with("requirepass "))
            .map_or(lines.len(), |index| index + 1);
        lines.insert(
            after_requirepass,
            format!("sentinel sentinel-pass {}", quote_conf_value(password)),
        );
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

    /// A conf as an image from before the rotation code wrote it — bare
    /// values, that build's directive order — after Sentinel has owned it
    /// for a while: its own rewrite keeps `sentinel-pass`/`auth-pass` bare,
    /// quotes `requirepass`, and appends the ACL line, its id, epochs and
    /// the peers it learned. The rotation must leave every credential line
    /// at the new password and nothing else changed.
    const LEGACY_CONF: &str = "port 26379
daemonize no
logfile \"\"
loglevel notice
requirepass \"old\"
sentinel sentinel-pass old
sentinel resolve-hostnames yes
sentinel announce-hostnames yes
sentinel announce-ip \"redis-1.railway.internal\"
sentinel announce-port 26379
sentinel monitor mymaster redis-3.railway.internal 6379 2
sentinel auth-pass mymaster old
sentinel down-after-milliseconds mymaster 5000
sentinel failover-timeout mymaster 30000
sentinel parallel-syncs mymaster 1
sentinel master-reboot-down-after-period mymaster 10000
# Generated by CONFIG REWRITE
dir \"/data\"
user default on sanitize-payload #a665a45920422f9d417e4867efdc4fb8a04a1f3fff1fa07e998e86f7f7a27ae3 ~* &* +@all
sentinel myid 731fde5108215c15037bed36e81515568d46f32f
sentinel config-epoch mymaster 1
sentinel leader-epoch mymaster 0
sentinel current-epoch 1
sentinel known-sentinel mymaster redis-2.railway.internal 26379 c101de0bd85d1eefec260edd76ee8f1821a238b3
sentinel known-replica mymaster redis-2.railway.internal 6379
";

    fn credential_lines(conf: &str) -> Vec<&str> {
        conf.lines()
            .filter(|line| {
                line.starts_with("requirepass ")
                    || line.starts_with("sentinel sentinel-pass ")
                    || line.starts_with("sentinel auth-pass ")
                    || line.starts_with("user default ")
            })
            .collect()
    }

    #[test]
    fn a_legacy_conf_comes_out_with_every_credential_at_the_new_password() {
        let updated = rewrite_sentinel(LEGACY_CONF, "new");
        assert_eq!(
            credential_lines(&updated),
            vec![
                "requirepass \"new\"",
                "sentinel sentinel-pass \"new\"",
                "sentinel auth-pass mymaster \"new\"",
                "user default on sanitize-payload ~* &* +@all resetpass #11507a0e2f5e69d5dfa40a62a1bd7b6ee57e6bcd85c67c9b8431b36fff21c437",
            ]
        );
        assert!(!updated.contains("old"));
        // Everything Sentinel learned survives, in place and in order.
        let kept = |line: &str| assert!(updated.lines().any(|l| l == line), "{line}");
        kept("sentinel monitor mymaster redis-3.railway.internal 6379 2");
        kept("sentinel myid 731fde5108215c15037bed36e81515568d46f32f");
        kept("sentinel config-epoch mymaster 1");
        kept("sentinel known-sentinel mymaster redis-2.railway.internal 26379 c101de0bd85d1eefec260edd76ee8f1821a238b3");
        kept("sentinel known-replica mymaster redis-2.railway.internal 6379");
        assert_eq!(updated.lines().count(), LEGACY_CONF.lines().count());
        assert_eq!(updated, rewrite_sentinel(&updated, "new"));
    }

    #[test]
    fn directives_match_the_way_redis_reads_them() {
        // Case-insensitive keywords, tabs, leading blanks: all valid conf.
        let odd = "RequirePass old\n\tSENTINEL  Sentinel-Pass\told\nSentinel AUTH-PASS mymaster old\nUSER default on #x ~* +@all\n";
        let updated = rewrite_sentinel(odd, "new");
        assert_eq!(
            credential_lines(&updated),
            vec![
                "requirepass \"new\"",
                "sentinel sentinel-pass \"new\"",
                "sentinel auth-pass mymaster \"new\"",
                "user default on ~* +@all resetpass #11507a0e2f5e69d5dfa40a62a1bd7b6ee57e6bcd85c67c9b8431b36fff21c437",
            ]
        );
        assert!(!updated.contains("old"));
    }

    #[test]
    fn a_conf_requiring_auth_without_a_sentinel_pass_line_gets_one() {
        // A conf from before the explicit sentinel-pass line existed: the
        // Sentinel fell back to requirepass for outbound auth. After the
        // rewrite the outbound credential is explicit, right after
        // requirepass, and present exactly once.
        let conf = "port 26379\nrequirepass old\nsentinel monitor mymaster h 6379 2\nsentinel auth-pass mymaster old\n";
        let updated = rewrite_sentinel(conf, "new");
        assert_eq!(
            updated,
            "port 26379\nrequirepass \"new\"\nsentinel sentinel-pass \"new\"\nsentinel monitor mymaster h 6379 2\nsentinel auth-pass mymaster \"new\"\n"
        );
        assert_eq!(updated, rewrite_sentinel(&updated, "new"));
    }

    #[test]
    fn an_open_conf_stays_open() {
        // No requirepass: never invent one, never add an outbound password
        // the peers would refuse.
        let conf =
            "port 26379\nsentinel monitor mymaster h 6379 2\nsentinel auth-pass mymaster old\n";
        let updated = rewrite_sentinel(conf, "new");
        assert!(!updated.contains("requirepass"));
        assert!(!updated.contains("sentinel-pass"));
        assert!(updated.contains("sentinel auth-pass mymaster \"new\""));
    }

    #[test]
    fn a_refusal_names_the_check_without_the_passwords() {
        let error = anyhow::anyhow!("AUTH failed for p:a ss\nsecond line")
            .context("authenticating to 127.0.0.1:26379")
            .context("SENTINEL CKQUORUM");
        let reason = refusal_reason(&error, &["p:a ss".to_string(), String::new()]);
        assert_eq!(
            reason,
            "SENTINEL CKQUORUM: authenticating to 127.0.0.1:26379: AUTH failed for <redacted> second line"
        );
        let long = anyhow::anyhow!("{}", "x".repeat(500));
        assert_eq!(refusal_reason(&long, &[]).chars().count(), 200);
    }
}
