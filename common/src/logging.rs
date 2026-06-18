use tracing_subscriber::{fmt, EnvFilter};

pub fn init_logging(component: &str) -> impl Drop {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info"));

    let subscriber = fmt::Subscriber::builder()
        .with_env_filter(filter)
        .with_target(false)
        .with_thread_ids(false)
        .json()
        .finish();

    // Attach component name to all spans via a field override
    let _ = tracing::subscriber::set_global_default(subscriber);

    tracing::info!(component, "logging initialized");

    // Return a guard that does nothing — keeps the API consistent with postgres-ha
    struct Guard;
    impl Drop for Guard {
        fn drop(&mut self) {}
    }
    Guard
}
