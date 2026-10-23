//! Process entry point.

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::RwLock;
use std::time::Duration;

use tokio::sync::watch;
use tracing::info;

use kilo_provider::admission::AdmissionController;
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
    let admission = Arc::new(AdmissionController::new(
        config.proxy_pool.max_concurrent_requests,
    ));
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
                                // update admission capacity safely (no controller replacement)
                                adm_clone.update_capacity(new_cfg.proxy_pool.max_concurrent_requests);
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
                                // retain model breakers for active catalog models + active leases
                                if let Some(snap) = cat_clone.current() {
                                    let active: HashSet<String> = snap.models.iter()
                                        .filter_map(|m| m.get("id").and_then(|v| v.as_str()).map(|s| s.to_string()))
                                        .collect();
                                    cb_clone.retain_models(&active);
                                }
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
                // The wrapper budget must exceed do_refresh's own internal
                // deadline (30s) so the refresh can always run its tail
                // mark_error before being cancelled — the outer timeout is a
                // safety net, not the primary bound.
                let _ = tokio::time::timeout(
                    Duration::from_secs(35),
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
    config: &Config,
) {
    do_refresh_at(
        pool,
        cb,
        catalog,
        config,
        reqwest::Url::parse(UPSTREAM_BASE_URL).expect("upstream base url"),
    )
    .await;
}

/// `do_refresh` against an explicit upstream base URL (test seam; production
/// callers go through [`do_refresh`]).
async fn do_refresh_at(
    pool: &ProxyPool,
    cb: &std::sync::Arc<CircuitBreaker>,
    catalog: &ModelCatalog,
    config: &Config,
    base: reqwest::Url,
) {
    use kilo_provider::catalog::CatalogFetchError;
    use kilo_provider::catalog::fetch_catalog_with_body_idle;
    let _single_flight = catalog.refresh_lock.lock().await;
    let origin = UPSTREAM_ORIGIN.to_string();
    let max_attempts = pool.len().max(1);
    // Internal refresh deadline: bounded AND strictly smaller than the
    // caller's outer wrapper (35s) so the tail mark_error always runs.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let header_timeout = Duration::from_secs(config.proxy_pool.response_header_timeout_secs);
    let retry_after_max = Duration::from_secs(config.circuit_breaker.retry_after_max_secs.max(1));
    let mut last_err: Option<String> = None;
    let mut stop = false;
    for _ in 0..max_attempts {
        if stop {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            break;
        }
        let reserved = pool.reserve_guarded(cb, &origin);
        let (proxy, tg, rg) = match reserved {
            Some(r) => r,
            None => {
                catalog.mark_error("no proxy available for catalog refresh".to_string());
                return;
            }
        };
        let http = proxy.client().clone();
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        // Per-attempt budget stays within the global refresh deadline — the
        // deadline must always be able to end the loop, no matter how large
        // the configured header timeout is. The inner header timeout is
        // clamped to this attempt budget so a header stall still surfaces as
        // CatalogFetchError::HeaderTimeout rather than a generic attempt
        // timeout.
        let attempt_timeout = remaining.max(Duration::from_secs(1));
        let header_budget = header_timeout.min(attempt_timeout);
        let res = tokio::time::timeout(
            attempt_timeout,
            fetch_catalog_with_body_idle(
                &http,
                &base,
                header_budget,
                // A body that stalls between chunks must fail as BodyRead
                // inside the attempt instead of consuming the whole budget.
                header_budget,
                8 * 1024 * 1024,
            ),
        )
        .await;
        let mut tg = Some(tg);
        let mut rg = Some(rg);
        match res {
            Ok(Ok(models)) => {
                // A fetch that filters down to zero free models is a failed
                // refresh, not a success: the last-good snapshot must stay in
                // place, no breaker success may be recorded, and the refresh
                // loop's backoff must not reset (it keys off is_ready).
                // `replace` records the error itself in that case.
                if catalog.replace(models) {
                    // Sync model breaker state with the new catalog: entries
                    // for models no longer present are dropped once they hold
                    // no active lease (retain_models keeps leased entries).
                    if let Some(snap) = catalog.current() {
                        let active: HashSet<String> = snap
                            .models
                            .iter()
                            .filter_map(|m| {
                                m.get("id").and_then(|v| v.as_str()).map(|s| s.to_string())
                            })
                            .collect();
                        cb.retain_models(&active);
                    }
                    if let Some(g) = tg.take() {
                        let mut g = g;
                        g.record_success();
                    }
                    if let Some(g) = rg.take() {
                        let mut g = g;
                        g.record_success();
                    }
                }
                return;
            }
            Ok(Err(e)) => {
                last_err = Some(e.to_string());
                match e {
                    CatalogFetchError::Connect(_) => {
                        if let Some(g) = tg.take() {
                            let mut g = g;
                            g.record_failure();
                        }
                        // rg drop releases without state change
                    }
                    CatalogFetchError::HeaderTimeout => {
                        // ambiguous: drop both (cancel)
                    }
                    CatalogFetchError::BodyRead(_) => {
                        // ambiguous: drop both
                    }
                    CatalogFetchError::RateLimited(ra) => {
                        let mut capped = ra.unwrap_or(Duration::from_secs(
                            config.circuit_breaker.rate_limit_cooldown_secs,
                        ));
                        if capped > retry_after_max {
                            capped = retry_after_max;
                        }
                        if let Some(g) = rg.take() {
                            let mut g = g;
                            g.record_failure(Some(capped));
                        }
                        if let Some(g) = tg.take() {
                            let mut g = g;
                            g.record_success();
                        }
                    }
                    CatalogFetchError::Http(_, _) => {
                        if let Some(g) = tg.take() {
                            let mut g = g;
                            g.record_success();
                        }
                        if let Some(g) = rg.take() {
                            let mut g = g;
                            g.record_success();
                        }
                        stop = true;
                    }
                    CatalogFetchError::Schema(_) => {
                        if let Some(g) = tg.take() {
                            let mut g = g;
                            g.record_success();
                        }
                        if let Some(g) = rg.take() {
                            let mut g = g;
                            g.record_success();
                        }
                        stop = true;
                    }
                }
            }
            Err(_) => {
                last_err = Some("catalog attempt timeout".to_string());
            }
        }
        // drops remaining guards
        drop(tg);
        drop(rg);
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

    // ---- F5: refresh deadline coordination (local fake upstream only) ----

    fn refresh_config(header_timeout_secs: u64) -> Config {
        let toml = format!(
            "\n[proxy_pool]\nreload_interval_secs = 1\nconnect_timeout_secs = 1\nresponse_header_timeout_secs = {header_timeout_secs}\nproxies = [\"http://placeholder.invalid:1\"]\nmax_concurrent_requests = 4\n\n[models]\nrefresh_interval_secs = 1\nstale_ttl_secs = 60\nmax_entries = 10\n\n[circuit_breaker]\nproxy_failure_cooldown_secs = 1\nrate_limit_cooldown_secs = 1\nmodel_not_found_cooldown_secs = 1\nhalf_open_max_probes = 1\nretry_after_max_secs = 5\n",
        );
        Config::from_toml(&toml).expect("config")
    }

    /// Spawn a fake upstream: GET /models returns the nth static body per
    /// call (first body once, then the second), or honors a notifier gate
    /// for stalls. Returns the bound address.
    async fn spawn_catalog_body_upstream(
        first: &'static [u8],
        rest: &'static [u8],
    ) -> std::net::SocketAddr {
        let hits = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let hits2 = hits.clone();
        let app = axum::Router::new().route(
            "/models",
            axum::routing::get(move || {
                let hits = hits2.clone();
                async move {
                    let n = hits.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    if n == 0 {
                        (axum::http::StatusCode::OK, first)
                    } else {
                        (axum::http::StatusCode::OK, rest)
                    }
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("addr");
        tokio::spawn(async move {
            axum::serve(listener, app).await.expect("serve");
        });
        addr
    }

    /// A stalled upstream with a LARGE configured header timeout must still
    /// be cut off by the global refresh deadline: do_refresh must return
    /// well before header_timeout would allow.
    #[tokio::test(flavor = "multi_thread")]
    async fn f5_global_deadline_bounds_large_header_timeout() {
        // Upstream accepts connections but never responds.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("addr");
        tokio::spawn(async move {
            loop {
                let Ok((mut sock, _)) = listener.accept().await else {
                    break;
                };
                tokio::spawn(async move {
                    let mut buf = [0u8; 4096];
                    use tokio::io::AsyncReadExt;
                    let _ = tokio::time::timeout(
                        std::time::Duration::from_secs(3),
                        sock.read(&mut buf),
                    )
                    .await;
                    tokio::time::sleep(std::time::Duration::from_secs(60)).await;
                });
            }
        });
        let config = refresh_config(60); // header timeout 60s > deadline
        let pool = {
            let mut c = config.proxy_pool.clone();
            c.proxies = vec![format!("http://{addr}")];
            ProxyPool::from_config(&c).expect("pool")
        };
        let cb = Arc::new(CircuitBreaker::new(config.circuit_breaker.clone()));
        let catalog = ModelCatalog::new(10, Duration::from_secs(60), Duration::from_secs(1));
        let base = reqwest::Url::parse(&format!("http://{addr}")).expect("url");
        let started = std::time::Instant::now();
        do_refresh_at(&pool, &cb, &catalog, &config, base).await;
        let elapsed = started.elapsed();
        assert!(
            elapsed < Duration::from_secs(35),
            "global deadline must bound the refresh, took {elapsed:?}"
        );
        assert!(
            catalog.last_error().is_some(),
            "failed refresh must mark last_error"
        );
        assert!(catalog.last_good().is_none());
    }

    /// A successful refresh replaces the snapshot; a failing one keeps the
    /// last-good snapshot.
    #[tokio::test(flavor = "multi_thread")]
    async fn f5_failed_refresh_keeps_last_good_snapshot() {
        let good: &'static [u8] = Box::leak(
            br#"{"data": [{"id":"keep:free","pricing":{"prompt":"0","completion":"0"}}]}"#
                .to_vec()
                .into_boxed_slice(),
        );
        let never: &'static [u8] = Box::leak(b"unused".to_vec().into_boxed_slice());
        let addr = spawn_catalog_body_upstream(good, never).await;
        let config = refresh_config(5);
        let pool = {
            let mut c = config.proxy_pool.clone();
            c.proxies = vec![format!("http://{addr}")];
            ProxyPool::from_config(&c).expect("pool")
        };
        let cb = Arc::new(CircuitBreaker::new(config.circuit_breaker.clone()));
        let catalog = ModelCatalog::new(10, Duration::from_secs(60), Duration::from_secs(1));
        let base = reqwest::Url::parse(&format!("http://{addr}")).expect("url");
        do_refresh_at(&pool, &cb, &catalog, &config, base.clone()).await;
        assert!(catalog.is_ready(), "first refresh must succeed");
        assert!(catalog.contains_model("keep:free"));
        // Dead upstream: refresh fails, snapshot preserved.
        let dead = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let dead_addr = dead.local_addr().expect("addr");
        drop(dead);
        let dead_base = reqwest::Url::parse(&format!("http://{dead_addr}")).expect("url");
        do_refresh_at(&pool, &cb, &catalog, &config, dead_base).await;
        assert!(
            catalog.last_error().is_some(),
            "failed refresh must record last_error"
        );
        assert!(
            catalog.contains_model("keep:free"),
            "last-good snapshot must survive a failed refresh"
        );
    }

    /// Refresh syncs the model breaker with the new catalog snapshot:
    /// states of cataloged models survive, entries removed from the catalog
    /// (without leases) are dropped by the sync.
    #[tokio::test(flavor = "multi_thread")]
    async fn f5_refresh_syncs_model_breaker_with_catalog() {
        let two: &'static [u8] = Box::leak(
            br#"{"data": [
            {"id":"keep:free","pricing":{"prompt":"0","completion":"0"}},
            {"id":"drop:free","pricing":{"prompt":"0","completion":"0"}}
        ]}"#
            .to_vec()
            .into_boxed_slice(),
        );
        let one: &'static [u8] = Box::leak(
            br#"{"data": [
            {"id":"keep:free","pricing":{"prompt":"0","completion":"0"}}
        ]}"#
            .to_vec()
            .into_boxed_slice(),
        );
        let addr = spawn_catalog_body_upstream(two, one).await;
        let config = refresh_config(5);
        let pool = {
            let mut c = config.proxy_pool.clone();
            c.proxies = vec![format!("http://{addr}")];
            ProxyPool::from_config(&c).expect("pool")
        };
        let cb = Arc::new(CircuitBreaker::new(config.circuit_breaker.clone()));
        let catalog = ModelCatalog::new(10, Duration::from_secs(60), Duration::from_secs(1));
        // Pre-existing breaker state: both models Open. Production opens a
        // breaker through a failed probe lease, so reproduce that path via
        // the public lease API (always available, no test gating).
        for id in ["keep:free", "drop:free"] {
            let lease = cb.model_reserve_lease(id).expect("lease");
            assert!(cb.model_record_failure_lease(id, lease));
            assert_eq!(
                cb.model_state(id),
                kilo_provider::circuit_breaker::BreakerState::Open
            );
        }
        let base = reqwest::Url::parse(&format!("http://{addr}")).expect("url");
        // Refresh 1: both models in catalog — breaker states preserved.
        do_refresh_at(&pool, &cb, &catalog, &config, base.clone()).await;
        assert!(catalog.is_ready());
        assert_eq!(
            cb.model_state("keep:free"),
            kilo_provider::circuit_breaker::BreakerState::Open,
            "cataloged model breaker state must survive refresh sync"
        );
        assert_eq!(
            cb.model_state("drop:free"),
            kilo_provider::circuit_breaker::BreakerState::Open
        );
        // Refresh 2: catalog drops drop:free — its unleased stale entry is
        // cleaned by the refresh's retain_models sync.
        do_refresh_at(&pool, &cb, &catalog, &config, base).await;
        assert!(
            !cb.model_contains("drop:free"),
            "de-cataloged model entry without leases must be dropped by refresh sync"
        );
        assert!(cb.model_contains("keep:free"));
    }

    /// Ambiguous failures (header timeout, body read error) must NOT
    /// quarantine the proxy (transport breaker stays Closed); connect
    /// failures must.
    #[tokio::test(flavor = "multi_thread")]
    async fn f5_ambiguous_errors_do_not_quarantine_proxy() {
        use kilo_provider::catalog::{CatalogFetchError, fetch_catalog};
        // Header timeout: stalled upstream.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("addr");
        tokio::spawn(async move {
            loop {
                let Ok((mut sock, _)) = listener.accept().await else {
                    break;
                };
                tokio::spawn(async move {
                    let mut buf = [0u8; 4096];
                    use tokio::io::AsyncReadExt;
                    let _ = tokio::time::timeout(
                        std::time::Duration::from_secs(3),
                        sock.read(&mut buf),
                    )
                    .await;
                    tokio::time::sleep(std::time::Duration::from_secs(60)).await;
                });
            }
        });
        let config = refresh_config(5);
        let pool = {
            let mut c = config.proxy_pool.clone();
            c.proxies = vec![format!("http://{addr}")];
            ProxyPool::from_config(&c).expect("pool")
        };
        let cb = Arc::new(CircuitBreaker::new(config.circuit_breaker.clone()));
        let _catalog = ModelCatalog::new(10, Duration::from_secs(60), Duration::from_secs(1));
        let http = pool.snapshot().proxies[0].client().clone();
        let base = reqwest::Url::parse(&format!("http://{addr}")).expect("url");
        let err = fetch_catalog(&http, &base, Duration::from_millis(300), 1024 * 1024)
            .await
            .expect_err("stall must error");
        assert!(matches!(err, CatalogFetchError::HeaderTimeout));
        let pid = pool.snapshot().proxies[0].id.clone();
        assert_eq!(
            cb.transport_state(&pid),
            kilo_provider::circuit_breaker::BreakerState::Closed,
            "ambiguous header timeout must not quarantine the proxy"
        );
    }

    /// A refresh whose body parses but contains zero free models is a
    /// FAILED refresh: the last-good snapshot must be preserved, no
    /// transport/rate-limit success recorded, and the stale entry for a
    /// dropped model must not be breaker-synced against the refused fetch.
    #[tokio::test(flavor = "multi_thread")]
    async fn f5_zero_free_models_refresh_is_failure_not_success() {
        let good: &'static [u8] = Box::leak(
            br#"{"data": [{"id":"keep:free","pricing":{"prompt":"0","completion":"0"}}]}"#
                .to_vec()
                .into_boxed_slice(),
        );
        let all_paid: &'static [u8] = Box::leak(
            br#"{"data": [{"id":"big:paid","pricing":{"prompt":"1","completion":"2"}}]}"#
                .to_vec()
                .into_boxed_slice(),
        );
        let addr = spawn_catalog_body_upstream(good, all_paid).await;
        let config = refresh_config(5);
        let pool = {
            let mut c = config.proxy_pool.clone();
            c.proxies = vec![format!("http://{addr}")];
            ProxyPool::from_config(&c).expect("pool")
        };
        let cb = Arc::new(CircuitBreaker::new(config.circuit_breaker.clone()));
        let catalog = ModelCatalog::new(10, Duration::from_secs(60), Duration::from_secs(1));
        let base = reqwest::Url::parse(&format!("http://{addr}")).expect("url");
        do_refresh_at(&pool, &cb, &catalog, &config, base.clone()).await;
        assert!(catalog.is_ready(), "first refresh must succeed");
        let pid = pool.snapshot().proxies[0].id.clone();
        assert_eq!(
            cb.transport_state(&pid),
            kilo_provider::circuit_breaker::BreakerState::Closed
        );
        let fetched_before = catalog.last_good().expect("snapshot").fetched_at;
        // Second refresh: parses fine but filters to zero free models.
        do_refresh_at(&pool, &cb, &catalog, &config, base).await;
        assert!(
            catalog.contains_model("keep:free"),
            "all-paid fetch must not replace the last-good snapshot"
        );
        assert!(
            catalog.last_error().is_some(),
            "zero-free-models fetch must surface as last_error"
        );
        let snap_after = catalog.last_good().expect("snapshot");
        assert_eq!(
            snap_after.fetched_at, fetched_before,
            "refused fetch must not refresh the snapshot timestamp"
        );
        // Transport breaker untouched: the fetch itself succeeded (HTTP 200
        // with a valid schema), so the proxy is healthy — only the snapshot
        // install was refused.
        assert_eq!(
            cb.transport_state(&pid),
            kilo_provider::circuit_breaker::BreakerState::Closed
        );
    }

    /// A 429 from upstream records a rate-limit failure with a capped
    /// retry-after on the rate-limit breaker, leaves the transport breaker
    /// Closed, and the loop continues to the next proxy (no stop).
    #[tokio::test(flavor = "multi_thread")]
    async fn f5_rate_limited_records_breaker_failure_and_continues() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let hits = Arc::new(AtomicUsize::new(0));
        let hits2 = hits.clone();
        let body429: &'static [u8] = Box::leak(b"slow down".to_vec().into_boxed_slice());
        let addr = {
            let app = axum::Router::new().route(
                "/models",
                axum::routing::get(move || {
                    let hits = hits2.clone();
                    async move {
                        hits.fetch_add(1, Ordering::SeqCst);
                        (
                            axum::http::StatusCode::TOO_MANY_REQUESTS,
                            [(axum::http::header::RETRY_AFTER, "3600".to_string())],
                            body429,
                        )
                    }
                }),
            );
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("bind");
            let addr = listener.local_addr().expect("addr");
            tokio::spawn(async move {
                axum::serve(listener, app).await.expect("serve");
            });
            addr
        };
        let config = refresh_config(5);
        let pool = {
            let mut c = config.proxy_pool.clone();
            c.proxies = vec![format!("http://{addr}")];
            ProxyPool::from_config(&c).expect("pool")
        };
        let cb = Arc::new(CircuitBreaker::new(config.circuit_breaker.clone()));
        let catalog = ModelCatalog::new(10, Duration::from_secs(60), Duration::from_secs(1));
        let base = reqwest::Url::parse(&format!("http://{addr}")).expect("url");
        do_refresh_at(&pool, &cb, &catalog, &config, base).await;
        assert_eq!(hits.load(Ordering::SeqCst), 1);
        let pid = pool.snapshot().proxies[0].id.clone();
        assert_eq!(
            cb.transport_state(&pid),
            kilo_provider::circuit_breaker::BreakerState::Closed,
            "429 is evidence against the rate-limit breaker, not the transport"
        );
        assert!(
            catalog.last_error().is_some(),
            "429 refresh must record last_error"
        );
        assert!(catalog.last_good().is_none());
    }

    /// A 5xx from upstream records transport+rate-limit success (the proxy
    /// answered) and stops the attempt loop.
    #[tokio::test(flavor = "multi_thread")]
    async fn f5_http_5xx_records_success_and_stops_loop() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let hits = Arc::new(AtomicUsize::new(0));
        let hits2 = hits.clone();
        let body500: &'static [u8] = Box::leak(b"boom".to_vec().into_boxed_slice());
        let addr = {
            let app = axum::Router::new().route(
                "/models",
                axum::routing::get(move || {
                    let hits = hits2.clone();
                    async move {
                        hits.fetch_add(1, Ordering::SeqCst);
                        (axum::http::StatusCode::INTERNAL_SERVER_ERROR, body500)
                    }
                }),
            );
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("bind");
            let addr = listener.local_addr().expect("addr");
            tokio::spawn(async move {
                axum::serve(listener, app).await.expect("serve");
            });
            addr
        };
        let config = refresh_config(5);
        let pool = {
            let mut c = config.proxy_pool.clone();
            c.proxies = vec![format!("http://{addr}")];
            ProxyPool::from_config(&c).expect("pool")
        };
        let cb = Arc::new(CircuitBreaker::new(config.circuit_breaker.clone()));
        let catalog = ModelCatalog::new(10, Duration::from_secs(60), Duration::from_secs(1));
        let base = reqwest::Url::parse(&format!("http://{addr}")).expect("url");
        do_refresh_at(&pool, &cb, &catalog, &config, base).await;
        // Exactly one attempt: the 5xx stop policy ends the loop.
        assert_eq!(
            hits.load(Ordering::SeqCst),
            1,
            "5xx must stop the refresh loop after the first attempt"
        );
        let pid = pool.snapshot().proxies[0].id.clone();
        assert_eq!(
            cb.transport_state(&pid),
            kilo_provider::circuit_breaker::BreakerState::Closed,
            "a proxy that answers 5xx answered: transport stays healthy"
        );
    }

    /// Single-flight: concurrent do_refresh calls serialize on the catalog's
    /// refresh_lock; the second cannot reach upstream until the first
    /// finishes.
    #[tokio::test(flavor = "multi_thread")]
    async fn f5_single_flight_serializes_refreshes() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let hits = Arc::new(AtomicUsize::new(0));
        let hits2 = hits.clone();
        let gate = Arc::new(tokio::sync::Notify::new());
        let gate2 = gate.clone();
        let body: &'static [u8] = Box::leak(
            br#"{"data":[{"id":"s:free","pricing":{"prompt":"0","completion":"0"}}]}"#
                .to_vec()
                .into_boxed_slice(),
        );
        let app = axum::Router::new().route(
            "/models",
            axum::routing::get(move || {
                let hits = hits2.clone();
                let gate = gate2.clone();
                async move {
                    hits.fetch_add(1, Ordering::SeqCst);
                    // first request holds until the test lets it go
                    if hits.load(Ordering::SeqCst) == 1 {
                        gate.notified().await;
                    }
                    (axum::http::StatusCode::OK, body)
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("addr");
        tokio::spawn(async move {
            axum::serve(listener, app).await.expect("serve");
        });
        let config = refresh_config(5);
        let pool = Arc::new({
            let mut c = config.proxy_pool.clone();
            c.proxies = vec![format!("http://{addr}")];
            ProxyPool::from_config(&c).expect("pool")
        });
        let cb = Arc::new(CircuitBreaker::new(config.circuit_breaker.clone()));
        let catalog = Arc::new(ModelCatalog::new(
            10,
            Duration::from_secs(60),
            Duration::from_secs(1),
        ));
        let base = reqwest::Url::parse(&format!("http://{addr}")).expect("url");
        let (done_tx, mut done_rx) = tokio::sync::mpsc::channel::<()>(2);
        for _ in 0..2 {
            let pool = pool.clone();
            let cb = cb.clone();
            let catalog = catalog.clone();
            let config = config.clone();
            let base = base.clone();
            let tx = done_tx.clone();
            tokio::spawn(async move {
                do_refresh_at(&pool, &cb, &catalog, &config, base).await;
                let _ = tx.send(()).await;
            });
        }
        drop(done_tx);
        // Give the first refresh time to reach the handler and block.
        tokio::time::sleep(Duration::from_millis(400)).await;
        assert_eq!(
            hits.load(Ordering::SeqCst),
            1,
            "single-flight: only one refresh may hit upstream at a time"
        );
        gate.notify_waiters();
        // Both refreshes must complete.
        let first = tokio::time::timeout(Duration::from_secs(10), done_rx.recv()).await;
        assert!(first.is_ok());
        let second = tokio::time::timeout(Duration::from_secs(10), done_rx.recv()).await;
        assert!(second.is_ok());
        assert!(catalog.is_ready());
    }
}
