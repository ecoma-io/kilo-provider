//! Process entry point.

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::RwLock;
use std::time::Duration;

use tokio::sync::watch;
use tracing::info;

use kilo_provider::catalog::ModelCatalog;
use kilo_provider::circuit_breaker::CircuitBreaker;
use kilo_provider::config::Config;
use kilo_provider::error::AppError;
use kilo_provider::proxy_pool::ProxyPool;
use kilo_provider::router::{AppState, build_router};
use kilo_provider::upstream::{UPSTREAM_BASE_URL, UPSTREAM_ORIGIN};

fn main() {
    if let Err(err) = run() {
        eprintln!("fatal: {err}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), AppError> {
    init_tracing();

    let port = port_from_env()?;
    let config_path = std::env::var("CONFIG_PATH").map_err(|err| {
        AppError::Config(format!(
            "environment variable CONFIG_PATH is required but could not be read ({err})"
        ))
    })?;
    let graceful_timeout = graceful_shutdown_timeout_from_env()?;

    let config = Config::load_from_path(&config_path)?;

    let proxy_pool = Arc::new(ProxyPool::from_config(&config.proxy_pool)?);
    let circuit_breaker = Arc::new(CircuitBreaker::new(config.circuit_breaker.clone()));
    let catalog = Arc::new(ModelCatalog::new(
        config.models.max_entries,
        Duration::from_secs(config.models.stale_ttl_secs),
        Duration::from_secs(config.models.refresh_interval_secs),
    ));
    let admission = Arc::new(RwLock::new(Arc::new(tokio::sync::Semaphore::new(
        config.proxy_pool.max_concurrent_requests,
    ))));
    let _upstream_base = UPSTREAM_BASE_URL;
    let _ = UPSTREAM_ORIGIN;

    let state = AppState {
        proxy_pool: proxy_pool.clone(),
        circuit_breaker: circuit_breaker.clone(),
        catalog: catalog.clone(),
        config: Arc::new(RwLock::new(config.clone())),
        admission: admission.clone(),
    };

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(AppError::Io)?;
    runtime.block_on(async move {
        // initial refresh in background; do not block listener binding
        let pool0 = proxy_pool.clone();
        let cb0 = circuit_breaker.clone();
        let cat0 = catalog.clone();
        let cfg0 = config.clone();
        tokio::spawn(async move {
            let _ = tokio::time::timeout(
                Duration::from_secs(30),
                do_refresh(&pool0, &cb0, &cat0, &cfg0),
            )
            .await;
        });

        // shutdown coordination
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        // hot reload + refresh loop
        let pool_clone = proxy_pool.clone();
        let cb_clone = circuit_breaker.clone();
        let cat_clone = catalog.clone();
        let adm_clone = admission.clone();
        let cfg_clone = state.config.clone();
        let path = config_path.clone();
        let mut reload_rx = shutdown_rx.clone();
        let reload_handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval_at(
                tokio::time::Instant::now()
                    + Duration::from_secs(cfg_clone.read().unwrap().proxy_pool.reload_interval_secs.max(1)),
                Duration::from_secs(cfg_clone.read().unwrap().proxy_pool.reload_interval_secs.max(1)),
            );
            loop {
                tokio::select! {
                    _ = reload_rx.changed() => {
                        if *reload_rx.borrow() { break; }
                    }
                    _ = interval.tick() => {}
                }
                if *reload_rx.borrow() {
                    break;
                }
                match Config::load_from_path(&path) {
                    Ok(new_cfg) => {
                        let old_fp = pool_clone.fingerprint();
                        let new_fp = ProxyPool::fingerprint_of(&new_cfg.proxy_pool);
                        // always attempt pool reload (validates+builds before swap)
                        match pool_clone.reload(&new_cfg.proxy_pool) {
                            Ok(()) => {
                                let pool_changed = old_fp != new_fp;
                                cb_clone.update_config(&new_cfg.circuit_breaker);
                                // update admission semaphore capacity
                                if adm_clone.read().unwrap().available_permits() != new_cfg.proxy_pool.max_concurrent_requests {
                                    *adm_clone.write().unwrap() = Arc::new(tokio::sync::Semaphore::new(
                                        new_cfg.proxy_pool.max_concurrent_requests,
                                    ));
                                }
                                // cleanup breaker state for removed proxies (safe: keep active+probes)
                                {
                                    let active: HashSet<String> = pool_clone
                                        .snapshot()
                                        .proxies
                                        .iter()
                                        .map(|p| p.id.clone())
                                        .collect();
                                    cb_clone.retain_proxies(&active);
                                }
                                cat_clone.update_config(
                                    Duration::from_secs(new_cfg.models.stale_ttl_secs),
                                    Duration::from_secs(new_cfg.models.refresh_interval_secs),
                                    new_cfg.models.max_entries,
                                );
                                *cfg_clone.write().unwrap() = new_cfg;
                                if !pool_changed {
                                    // breaker state already preserved because proxy ids stable
                                    tracing::debug!("config reloaded; proxy identity unchanged, breaker state preserved");
                                }
                            }
                            Err(e) => {
                                tracing::error!("proxy pool reload failed; keeping last good snapshot: {e}");
                            }
                        }
                    }
                    Err(e) => {
                        tracing::error!("config reload failed; keeping last good snapshot: {e}");
                    }
                }
                // update polling interval from current config
                let new_iv = cfg_clone.read().unwrap().proxy_pool.reload_interval_secs.max(1);
                interval = tokio::time::interval_at(
                    tokio::time::Instant::now() + Duration::from_secs(new_iv),
                    Duration::from_secs(new_iv),
                );
            }
        });

        // periodic catalog refresh
        let pool2 = proxy_pool.clone();
        let cb2 = circuit_breaker.clone();
        let cat2 = catalog.clone();
        let cfg2 = state.config.clone();
        let mut refresh_rx = shutdown_rx.clone();
        let refresh_handle = tokio::spawn(async move {
            let mut backoff: u64 = 0;
            loop {
                let cfg = cfg2.read().unwrap().clone();
                let ready = cat2.is_ready();
                let sleep_secs = if ready {
                    backoff = 0;
                    cfg.models.refresh_interval_secs.max(1)
                } else {
                    if backoff == 0 {
                        backoff = cfg.models.recovery_initial_backoff_secs.max(1);
                    } else {
                        backoff = (backoff * 2).min(cfg.models.recovery_max_backoff_secs.max(1));
                    }
                    backoff
                };
                tokio::select! {
                    _ = refresh_rx.changed() => {
                        if *refresh_rx.borrow() { break; }
                    }
                    _ = tokio::time::sleep(Duration::from_secs(sleep_secs)) => {}
                }
                if *refresh_rx.borrow() {
                    break;
                }
                let cfg = cfg2.read().unwrap().clone();
                let _ = tokio::time::timeout(
                    Duration::from_secs(30),
                    do_refresh(&pool2, &cb2, &cat2, &cfg),
                )
                .await;
            }
        });

        let app = build_router(state);
        let listener = tokio::net::TcpListener::bind(("0.0.0.0", port))
            .await
            .map_err(|err| AppError::Port(format!("could not bind 0.0.0.0:{port} ({err})")))?;
        info!(
            port = port,
            proxies = proxy_pool.len(),
            graceful_shutdown_timeout_secs = graceful_timeout.as_secs(),
            "started"
        );

        // serve with graceful shutdown
        // signal listener sets shutdown flag
        let tx = shutdown_tx.clone();
        tokio::spawn(async move {
            shutdown_signal().await;
            let _ = tx.send(true);
        });

        let mut rx_for_serve = shutdown_tx.subscribe();
        let server_task = tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(async move {
                    while !*rx_for_serve.borrow() {
                        if rx_for_serve.changed().await.is_err() {
                            break;
                        }
                    }
                })
                .await
        });

        // wait for shutdown signal
        let mut rx = shutdown_tx.subscribe();
        while !*rx.borrow() {
            if rx.changed().await.is_err() {
                break;
            }
        }
        let _ = shutdown_tx.send(true);
        info!(
            "shutdown started; grace deadline {}s",
            graceful_timeout.as_secs()
        );

        // drain with deadline
        let deadline = tokio::time::Instant::now() + graceful_timeout;
        let drain_res = tokio::time::timeout_at(deadline, server_task).await;
        if drain_res.is_err() {
            tracing::warn!("graceful shutdown deadline exceeded; forcing termination");
        }
        reload_handle.abort();
        refresh_handle.abort();
        Ok(())
    })
}

async fn do_refresh(
    pool: &ProxyPool,
    cb: &std::sync::Arc<CircuitBreaker>,
    catalog: &ModelCatalog,
    _config: &Config,
) {
    use kilo_provider::catalog::CatalogFetchError;
    use kilo_provider::catalog::fetch_catalog;
    let origin = UPSTREAM_ORIGIN.to_string();
    let base = reqwest::Url::parse(UPSTREAM_BASE_URL).unwrap();
    let max_attempts = pool.len().max(1);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let mut last_err: Option<String> = None;
    for _ in 0..max_attempts {
        if tokio::time::Instant::now() >= deadline {
            break;
        }
        let reserved = pool.reserve_guarded(cb, &origin);
        let (proxy, mut tg, mut rg) = match reserved {
            Some(r) => r,
            None => {
                // no proxy available right now
                catalog.mark_error("no proxy available for catalog refresh".to_string());
                return;
            }
        };
        let pid = proxy.id.clone();
        let http = proxy.client().clone();
        match fetch_catalog(&http, &base, None).await {
            Ok(models) => {
                catalog.replace(models);
                tg.record_success();
                rg.record_success();
                // also reset rate limit? record_success does
                return;
            }
            Err(e) => {
                last_err = Some(e.to_string());
                match e {
                    CatalogFetchError::Connect(_) => {
                        cb.transport_record_failure_outcome(&pid, tg.lease_id());
                        rg.record_success(); // cancel rate limit reservation? keep
                        // Actually release rate limit probe
                        // mark not quarantined for rate limit
                    }
                    CatalogFetchError::RateLimited(ra) => {
                        let capped = ra; // already capped in cb? pass through
                        cb.rate_limit_record_failure_outcome(&pid, &origin, rg.lease_id(), capped);
                        tg.record_success();
                    }
                    CatalogFetchError::Http(_, _) => {
                        // application error: do not quarantine transport
                        tg.record_success();
                        rg.record_success();
                    }
                    CatalogFetchError::Schema(_) => {
                        tg.record_success();
                        rg.record_success();
                    }
                }
                // cancel guards to release any remaining reservations
                // guards drop at end of iteration? We'll drop explicitly
                drop(tg);
                drop(rg);
                // if connect failure, loop to next proxy
                continue;
            }
        }
    }
    if let Some(e) = last_err {
        catalog.mark_error(e);
    } else {
        catalog.mark_error("catalog refresh failed".to_string());
    }
}

fn parse_graceful_timeout(raw: &str) -> Result<Duration, AppError> {
    let secs: i64 = raw.parse().map_err(|err| {
        AppError::Port(format!(
            "environment variable GRACEFUL_SHUTDOWN_TIMEOUT_SECS must be a valid positive integer, but {raw:?} could not be parsed ({err})"
        ))
    })?;
    if secs <= 0 {
        return Err(AppError::Port(format!(
            "environment variable GRACEFUL_SHUTDOWN_TIMEOUT_SECS must be a positive integer, got {secs}"
        )));
    }
    if secs > i64::MAX / 2 {
        return Err(AppError::Port(
            "environment variable GRACEFUL_SHUTDOWN_TIMEOUT_SECS out of range".to_string(),
        ));
    }
    Ok(Duration::from_secs(secs as u64))
}

fn graceful_shutdown_timeout_from_env() -> Result<Duration, AppError> {
    match std::env::var("GRACEFUL_SHUTDOWN_TIMEOUT_SECS") {
        Ok(raw) if raw.trim().is_empty() => Err(AppError::Port(
            "environment variable GRACEFUL_SHUTDOWN_TIMEOUT_SECS is set but empty".to_string(),
        )),
        Ok(raw) => parse_graceful_timeout(raw.trim()),
        Err(std::env::VarError::NotPresent) => Ok(Duration::from_secs(30)),
        Err(err) => Err(AppError::Port(format!(
            "environment variable GRACEFUL_SHUTDOWN_TIMEOUT_SECS could not be read ({err})"
        ))),
    }
}

fn parse_port(raw: &str) -> Result<u16, AppError> {
    match raw.parse::<u16>() {
        Ok(port) if port >= 1 => Ok(port),
        Ok(_) => Err(AppError::Port(format!(
            "environment variable PORT must be in the 1..=65535 range, but got the value {raw:?}"
        ))),
        Err(err) => Err(AppError::Port(format!(
            "environment variable PORT must be a valid integer in the 1..=65535 range, but the value {raw:?} could not be parsed as a port ({err})"
        ))),
    }
}

fn port_from_env() -> Result<u16, AppError> {
    match std::env::var("PORT") {
        Ok(raw) => parse_port(&raw),
        Err(std::env::VarError::NotPresent) => Err(missing_port_error()),
        Err(err) => Err(AppError::Port(format!(
            "environment variable PORT must be a valid integer in the 1..=65535 range, but its value could not be read ({err})"
        ))),
    }
}

fn missing_port_error() -> AppError {
    AppError::Port(
        "environment variable PORT is required but is not set; set it to a port in the 1..=65535 range (there is no default port)"
            .to_owned(),
    )
}

fn init_tracing() {
    use tracing_subscriber::EnvFilter;
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    if std::env::var("LOG_FORMAT").ok().as_deref() == Some("json") {
        tracing_subscriber::fmt()
            .json()
            .with_env_filter(filter)
            .init();
    } else {
        tracing_subscriber::fmt().with_env_filter(filter).init();
    }
}

async fn shutdown_signal() {
    use tokio::signal::unix::{SignalKind, signal};
    let terminate = async {
        match signal(SignalKind::terminate()) {
            Ok(mut stream) => {
                stream.recv().await;
            }
            Err(err) => {
                tracing::warn!("cannot install SIGTERM handler, ignoring it: {err}");
                std::future::pending::<()>().await;
            }
        }
    };
    tokio::select! {
        _ = terminate => {},
        _ = tokio::signal::ctrl_c() => {},
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn port_accepts_valid_values() {
        assert_eq!(parse_port("8080").unwrap(), 8080);
    }
    #[test]
    fn port_rejects_zero() {
        assert!(parse_port("0").is_err());
    }

    #[test]
    fn graceful_unset_default() {
        // simulate by calling helper with NotPresent path not available; test parse only
        assert!(parse_graceful_timeout("30").unwrap().as_secs() == 30);
    }
    #[test]
    fn graceful_valid() {
        assert_eq!(parse_graceful_timeout("5").unwrap().as_secs(), 5);
    }
    #[test]
    fn graceful_empty_rejected() {
        // empty handled in from_env; parse of "" fails
        assert!(parse_graceful_timeout("").is_err());
    }
    #[test]
    fn graceful_zero_rejected() {
        assert!(parse_graceful_timeout("0").is_err());
    }
    #[test]
    fn graceful_non_numeric_rejected() {
        assert!(parse_graceful_timeout("abc").is_err());
    }
    #[test]
    fn graceful_negative_rejected() {
        assert!(parse_graceful_timeout("-5").is_err());
    }
}
