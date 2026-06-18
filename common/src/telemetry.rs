use crate::config::RailwayEnv;
use reqwest::blocking::Client;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::time::Duration;
use tracing::{info, warn};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum TelemetryEvent {
    /// Redis node started successfully
    NodeStarted { node: String, role: String },

    /// Sentinel detected a failover and a new master was elected
    Failover {
        node: String,
        new_master: String,
        master_name: String,
    },

    /// Node role changed (master → replica or replica → master)
    RoleChanged {
        node: String,
        old_role: String,
        new_role: String,
    },

    /// Redis or Sentinel process died unexpectedly
    ProcessDied {
        node: String,
        process: String,
        exit_code: Option<i32>,
    },

    /// HAProxy has no healthy primary backend
    NoPrimary { backends: Vec<String> },

    /// Generic error during startup or operation
    ComponentError {
        component: String,
        error: String,
        context: String,
    },
}

pub struct Telemetry {
    client: Client,
    endpoint: String,
    service_id: String,
    project_id: String,
}

impl Telemetry {
    pub fn from_env(component: &str) -> Self {
        let client = Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .expect("HTTP client creation should not fail");

        info!(component, "telemetry initialized");

        Self {
            client,
            endpoint: RailwayEnv::graphql_endpoint(),
            service_id: RailwayEnv::service_id(),
            project_id: RailwayEnv::project_id(),
        }
    }

    pub fn send(&self, event: TelemetryEvent) {
        let payload = json!({
            "query": "mutation SendTelemetry($input: TelemetryInput!) { sendTelemetry(input: $input) }",
            "variables": {
                "input": {
                    "serviceId": self.service_id,
                    "projectId": self.project_id,
                    "event": serde_json::to_value(&event).unwrap_or_default(),
                }
            }
        });

        match self.client.post(&self.endpoint).json(&payload).send() {
            Ok(_) => info!(event = ?event, "telemetry sent"),
            Err(e) => warn!(error = %e, "telemetry send failed"),
        }
    }
}
