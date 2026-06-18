use anyhow::{Context, Result};
use std::env;
use std::str::FromStr;

pub trait ConfigExt {
    fn env_or(name: &str, default: &str) -> String {
        env::var(name).unwrap_or_else(|_| default.to_string())
    }

    fn env_required(name: &str) -> Result<String> {
        env::var(name).context(format!("{} must be set", name))
    }

    fn env_bool(name: &str, default: bool) -> bool {
        env::var(name)
            .map(|v| v.to_lowercase() == "true")
            .unwrap_or(default)
    }

    fn env_parse<T: FromStr>(name: &str, default: T) -> T {
        env::var(name)
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(default)
    }
}

impl<T> ConfigExt for T {}

pub struct RailwayEnv;

impl RailwayEnv {
    pub fn is_railway() -> bool {
        env::var("RAILWAY_ENVIRONMENT").is_ok()
    }

    pub fn private_domain() -> String {
        env::var("RAILWAY_PRIVATE_DOMAIN").unwrap_or_else(|_| "localhost".to_string())
    }

    pub fn service_id() -> String {
        env::var("RAILWAY_SERVICE_ID").unwrap_or_default()
    }

    pub fn project_id() -> String {
        env::var("RAILWAY_PROJECT_ID").unwrap_or_default()
    }

    pub fn graphql_endpoint() -> String {
        env::var("RAILWAY_GRAPHQL_ENDPOINT")
            .unwrap_or_else(|_| "https://backboard.railway.app/graphql/internal".to_string())
    }
}
