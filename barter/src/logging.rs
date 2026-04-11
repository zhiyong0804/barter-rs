use crate::engine::audit::state_replica::AUDIT_REPLICA_STATE_UPDATE_SPAN_NAME;
use std::{env, fs, sync::OnceLock};
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

static LOG_GUARD: OnceLock<WorkerGuard> = OnceLock::new();

fn init_file_writer() -> tracing_appender::non_blocking::NonBlocking {
    let log_dir = env::var("BARTER_LOG_DIR").unwrap_or_else(|_| "logs".to_owned());
    let log_file_prefix = env::var("BARTER_LOG_FILE_PREFIX")
        .unwrap_or_else(|_| "binance-futures-risk-manager.log".to_owned());

    let _ = fs::create_dir_all(&log_dir);
    let appender = tracing_appender::rolling::daily(log_dir, log_file_prefix);
    let (non_blocking, guard) = tracing_appender::non_blocking(appender);
    let _ = LOG_GUARD.set(guard);
    non_blocking
}

/// Initialise default non-JSON `Barter` logging.
///
/// Note that this filters out duplicate logs produced by the `AuditManager` updating its replica
/// `EngineState`.
pub fn init_logging() {
    let writer = init_file_writer();

    tracing_subscriber::registry()
        .with(
            tracing_subscriber::filter::EnvFilter::builder()
                .with_default_directive(tracing_subscriber::filter::LevelFilter::DEBUG.into())
                .from_env_lossy(),
        )
        .with(
            tracing_subscriber::fmt::layer()
                .with_ansi(false)
                .with_writer(writer),
        )
        .with(AuditSpanFilter)
        .init()
}

/// Initialise default JSON `Barter` logging.
///
/// Note that this filters out duplicate logs produced by the `AuditManager` updating its replica
/// `EngineState`.
pub fn init_json_logging() {
    let writer = init_file_writer();

    tracing_subscriber::registry()
        .with(
            tracing_subscriber::filter::EnvFilter::builder()
                .with_default_directive(tracing_subscriber::filter::LevelFilter::DEBUG.into())
                .from_env_lossy(),
        )
        .with(
            tracing_subscriber::fmt::layer()
                .json()
                .flatten_event(true)
                .with_ansi(false)
                .with_writer(writer),
        )
        .with(AuditSpanFilter)
        .init()
}

#[derive(Debug)]
pub struct AuditSpanFilter;

impl<S> tracing_subscriber::layer::Layer<S> for AuditSpanFilter
where
    S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
{
    fn event_enabled(
        &self,
        _: &tracing::Event<'_>,
        ctx: tracing_subscriber::layer::Context<'_, S>,
    ) -> bool {
        if let Some(span) = ctx.lookup_current()
            && span.name() == AUDIT_REPLICA_STATE_UPDATE_SPAN_NAME
        {
            false
        } else {
            true
        }
    }
}
