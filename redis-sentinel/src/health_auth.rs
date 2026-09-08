//! HTTP Basic auth for the health server's mutating routes.
//!
//! The health server sits on the private network, and `POST /switchover`
//! hands the primary role to whichever node receives it. With
//! [`PASSWORD_ENV`] set, that route answers `401` unless the request carries
//! `Authorization: Basic base64(username:password)` matching the configured
//! credential; the probe routes (`GET /health`, `GET /role`) stay open, so
//! HAProxy's checks and the dashboard's role reads are unaffected.
//!
//! Without the variable the server behaves exactly as before — open — which
//! is what lets a cluster adopt enforcement one variable edit at a time:
//! callers attach the credential unconditionally (a node that does not
//! enforce ignores it), then the variable lands on the data nodes.
//!
//! The username defaults to [`DEFAULT_USERNAME`]; both halves are compared in
//! constant time. A refusal logs one line naming the route, never the header.

use axum::{
    extract::{Request, State},
    http::{header, HeaderValue, StatusCode},
    middleware::Next,
    response::{IntoResponse, Json, Response},
};
use base64::Engine;
use serde_json::json;
use std::fmt;
use subtle::ConstantTimeEq;
use tracing::warn;

/// Password that turns enforcement on (read by `Config`). Trimmed; empty
/// counts as unset.
pub const PASSWORD_ENV: &str = "HEALTH_API_PASSWORD";
/// Username the credential is checked against (read by `Config`). Trimmed;
/// empty falls back to [`DEFAULT_USERNAME`].
pub const USERNAME_ENV: &str = "HEALTH_API_USERNAME";
pub const DEFAULT_USERNAME: &str = "railway";

/// The challenge every refusal carries.
const CHALLENGE: &str = "Basic realm=\"railway-ha\"";

/// The credential the mutating routes require. `Clone` so the middleware can
/// hold it as axum state.
#[derive(Clone, PartialEq, Eq)]
pub struct Credential {
    pub username: String,
    pub password: String,
}

// The password must never reach a log line through `{:?}`.
impl fmt::Debug for Credential {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Credential")
            .field("username", &self.username)
            .field("password", &"<redacted>")
            .finish()
    }
}

impl Credential {
    /// Builds the credential from raw variable values. `None` when the
    /// password is empty after trimming — enforcement is off. An empty
    /// username falls back to [`DEFAULT_USERNAME`].
    pub fn resolve(username: &str, password: &str) -> Option<Self> {
        let password = password.trim();
        if password.is_empty() {
            return None;
        }
        let username = match username.trim() {
            "" => DEFAULT_USERNAME,
            trimmed => trimmed,
        };
        Some(Self {
            username: username.to_string(),
            password: password.to_string(),
        })
    }

    /// Whether an `Authorization` header value proves this credential.
    /// Anything that is not well-formed HTTP Basic is a plain `false`.
    pub fn authorizes(&self, authorization: Option<&HeaderValue>) -> bool {
        let Some((username, password)) = authorization.and_then(parse_basic) else {
            return false;
        };
        // Both comparisons always run: a username mismatch must not return
        // early and leak which half was wrong through timing.
        let user_ok = username.as_bytes().ct_eq(self.username.as_bytes());
        let pass_ok = password.as_bytes().ct_eq(self.password.as_bytes());
        bool::from(user_ok & pass_ok)
    }
}

/// `Basic <token68>` → `(username, password)`, or `None` for anything else.
fn parse_basic(value: &HeaderValue) -> Option<(String, String)> {
    let raw = value.to_str().ok()?.trim();
    let (scheme, token) = raw.split_once(char::is_whitespace)?;
    if !scheme.eq_ignore_ascii_case("basic") {
        return None;
    }
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(token.trim())
        .ok()?;
    let decoded = String::from_utf8(decoded).ok()?;
    let (username, password) = decoded.split_once(':')?;
    Some((username.to_string(), password.to_string()))
}

/// The `401` every refusal answers with.
fn unauthorized() -> Response {
    (
        StatusCode::UNAUTHORIZED,
        [(header::WWW_AUTHENTICATE, HeaderValue::from_static(CHALLENGE))],
        Json(json!({"status": "unauthorized"})),
    )
        .into_response()
}

/// axum middleware for the mutating routes: passes the request through when
/// no credential is configured or the header proves it, refuses otherwise.
/// Attach with `middleware::from_fn_with_state(credential, require_credential)`
/// via `route_layer`, so the probe routes never see it.
pub async fn require_credential(
    State(credential): State<Option<Credential>>,
    request: Request,
    next: Next,
) -> Response {
    let Some(credential) = credential else {
        return next.run(request).await;
    };
    let authorization = request.headers().get(header::AUTHORIZATION);
    if credential.authorizes(authorization) {
        return next.run(request).await;
    }
    let reason = if authorization.is_none() {
        "no credential"
    } else {
        "credential rejected"
    };
    warn!(
        method = %request.method(),
        path = %request.uri().path(),
        reason,
        "health-server: refused a mutating request"
    );
    unauthorized()
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        body::{to_bytes, Body},
        http::{Method, Request as HttpRequest},
        middleware,
        routing::{get, post},
        Router,
    };
    use tower::ServiceExt;

    fn cred() -> Credential {
        Credential::resolve("railway", "s3cr3t").unwrap()
    }

    fn basic(user: &str, pass: &str) -> HeaderValue {
        let token = base64::engine::general_purpose::STANDARD.encode(format!("{user}:{pass}"));
        HeaderValue::from_str(&format!("Basic {token}")).unwrap()
    }

    #[test]
    fn empty_or_blank_password_disables_enforcement() {
        assert_eq!(Credential::resolve("railway", ""), None);
        assert_eq!(Credential::resolve("railway", "   \n"), None);
    }

    #[test]
    fn username_defaults_and_both_halves_are_trimmed() {
        let c = Credential::resolve("", "  pw  ").unwrap();
        assert_eq!(c.username, DEFAULT_USERNAME);
        assert_eq!(c.password, "pw");
        let c = Credential::resolve("  ops  ", "pw").unwrap();
        assert_eq!(c.username, "ops");
    }

    #[test]
    fn debug_redacts_the_password() {
        let shown = format!("{:?}", cred());
        assert!(shown.contains("railway"));
        assert!(!shown.contains("s3cr3t"));
        assert!(shown.contains("<redacted>"));
    }

    #[test]
    fn missing_header_is_refused() {
        assert!(!cred().authorizes(None));
    }

    #[test]
    fn non_basic_scheme_is_refused() {
        let v = HeaderValue::from_static("Bearer s3cr3t");
        assert!(!cred().authorizes(Some(&v)));
    }

    #[test]
    fn bad_base64_is_refused() {
        let v = HeaderValue::from_static("Basic !!!not-base64!!!");
        assert!(!cred().authorizes(Some(&v)));
    }

    #[test]
    fn token_without_a_colon_is_refused() {
        let token = base64::engine::general_purpose::STANDARD.encode("railways3cr3t");
        let v = HeaderValue::from_str(&format!("Basic {token}")).unwrap();
        assert!(!cred().authorizes(Some(&v)));
    }

    #[test]
    fn wrong_username_is_refused() {
        assert!(!cred().authorizes(Some(&basic("admin", "s3cr3t"))));
    }

    #[test]
    fn wrong_password_is_refused() {
        assert!(!cred().authorizes(Some(&basic("railway", "s3cr3t "))));
        assert!(!cred().authorizes(Some(&basic("railway", "wrong"))));
    }

    #[test]
    fn matching_credential_is_accepted() {
        assert!(cred().authorizes(Some(&basic("railway", "s3cr3t"))));
    }

    #[test]
    fn password_may_contain_colons() {
        let c = Credential::resolve("railway", "a:b:c").unwrap();
        assert!(c.authorizes(Some(&basic("railway", "a:b:c"))));
    }

    #[test]
    fn scheme_is_case_insensitive() {
        let token = base64::engine::general_purpose::STANDARD.encode("railway:s3cr3t");
        let v = HeaderValue::from_str(&format!("basic {token}")).unwrap();
        assert!(cred().authorizes(Some(&v)));
        let v = HeaderValue::from_str(&format!("BASIC  {token}")).unwrap();
        assert!(cred().authorizes(Some(&v)));
    }

    // Router-level: the layer sits on the mutating route only, exactly as
    // health_server wires it.
    fn app(credential: Option<Credential>) -> Router {
        let guarded = Router::new()
            .route("/act", post(|| async { "acted" }))
            .route_layer(middleware::from_fn_with_state(
                credential,
                require_credential,
            ));
        Router::new()
            .route("/open", get(|| async { "open" }))
            .merge(guarded)
    }

    fn req(method: Method, path: &str, auth: Option<HeaderValue>) -> HttpRequest<Body> {
        let mut b = HttpRequest::builder().method(method).uri(path);
        if let Some(v) = auth {
            b = b.header(header::AUTHORIZATION, v);
        }
        b.body(Body::empty()).unwrap()
    }

    #[tokio::test]
    async fn open_route_never_asks_for_a_credential() {
        let resp = app(Some(cred()))
            .oneshot(req(Method::GET, "/open", None))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn mutating_route_refuses_without_a_credential() {
        let resp = app(Some(cred()))
            .oneshot(req(Method::POST, "/act", None))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(
            resp.headers()
                .get(header::WWW_AUTHENTICATE)
                .and_then(|v| v.to_str().ok()),
            Some(CHALLENGE)
        );
        let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json, json!({"status": "unauthorized"}));
    }

    #[tokio::test]
    async fn mutating_route_refuses_a_wrong_credential() {
        let resp = app(Some(cred()))
            .oneshot(req(Method::POST, "/act", Some(basic("railway", "nope"))))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn mutating_route_passes_with_the_credential() {
        let resp = app(Some(cred()))
            .oneshot(req(Method::POST, "/act", Some(basic("railway", "s3cr3t"))))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn no_configured_credential_leaves_the_route_open() {
        let resp = app(None)
            .oneshot(req(Method::POST, "/act", None))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }
}
