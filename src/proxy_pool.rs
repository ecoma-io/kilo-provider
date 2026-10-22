//! Proxy pool with explicit per-proxy HTTP clients, round-robin rotation,
//! transport/rate-limit awareness and atomic hot reload.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use reqwest::Url;
use sha2::{Digest, Sha256};

use crate::config::ProxyPoolConfig;
use crate::error::AppError;

#[derive(Debug, Clone)]
pub struct ProxyClient {
    pub id: String,
    pub url: Url,
    client: reqwest::Client,
}

impl ProxyClient {
    pub fn client(&self) -> &reqwest::Client {
        &self.client
    }
}

#[derive(Debug, Clone)]
pub struct PoolSnapshot {
    pub proxies: Vec<Arc<ProxyClient>>,
    pub fingerprint: String,
}

pub struct ProxyPool {
    inner: RwLock<Arc<PoolSnapshot>>,
    rr: AtomicUsize,
}

impl std::fmt::Debug for ProxyPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProxyPool")
            .field("len", &self.len())
            .finish()
    }
}

impl ProxyPool {
    pub fn from_config(config: &ProxyPoolConfig) -> Result<Self, AppError> {
        let pool = Self {
            inner: RwLock::new(Arc::new(PoolSnapshot {
                proxies: Vec::new(),
                fingerprint: String::new(),
            })),
            rr: AtomicUsize::new(0),
        };
        pool.reload(config)?;
        Ok(pool)
    }

    pub fn fingerprint_of(config: &ProxyPoolConfig) -> String {
        let mut h = Sha256::new();
        h.update(config.connect_timeout_secs.to_le_bytes());
        for p in &config.proxies {
            h.update(p.as_bytes());
            h.update(b"\n");
        }
        hex(&h.finalize())
    }

    pub fn reload(&self, config: &ProxyPoolConfig) -> Result<(), AppError> {
        if config.proxies.is_empty() {
            return Err(AppError::Config(
                "proxy_pool.proxies must contain at least one proxy".into(),
            ));
        }
        let new_fp = Self::fingerprint_of(config);
        // If fingerprint unchanged, skip rebuild.
        if self.snapshot().fingerprint == new_fp && !self.snapshot().proxies.is_empty() {
            return Ok(());
        }
        let mut proxies = Vec::with_capacity(config.proxies.len());
        for url_str in &config.proxies {
            let url = Url::parse(url_str).map_err(|err| {
                AppError::Config(format!("invalid URL in '[proxy_pool] proxies': {err}"))
            })?;
            let scheme = url.scheme();
            if scheme != "http" && scheme != "https" && scheme != "socks5" && scheme != "socks5h" {
                return Err(AppError::Config(
                    "unsupported proxy scheme in '[proxy_pool] proxies'".to_string(),
                ));
            }
            // Build client routed through this proxy explicitly; ignore env proxies.
            let proxy = reqwest::Proxy::all(url_str)
                .map_err(|err| AppError::Config(format!("invalid proxy configuration: {err}")))?;
            let client = reqwest::Client::builder()
                .proxy(proxy)
                .connect_timeout(Duration::from_secs(config.connect_timeout_secs))
                .build()
                .map_err(|err| AppError::Config(format!("could not build proxy client: {err}")))?;
            let id = redacted_id(&url, url_str);
            proxies.push(Arc::new(ProxyClient { id, url, client }));
        }
        let snap = Arc::new(PoolSnapshot {
            proxies,
            fingerprint: new_fp,
        });
        *self.inner.write().expect("pool lock") = snap;
        Ok(())
    }

    pub fn snapshot(&self) -> Arc<PoolSnapshot> {
        self.inner.read().expect("pool lock").clone()
    }

    pub fn len(&self) -> usize {
        self.inner.read().expect("pool lock").proxies.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn fingerprint(&self) -> String {
        self.snapshot().fingerprint.clone()
    }

    pub fn select(
        &self,
        breaker: &crate::circuit_breaker::CircuitBreaker,
        origin: &str,
    ) -> Option<Arc<ProxyClient>> {
        let snap = self.snapshot();
        let n = snap.proxies.len();
        if n == 0 {
            return None;
        }
        let start = self.rr.fetch_add(1, Ordering::Relaxed) % n;
        for i in 0..n {
            let p = &snap.proxies[(start + i) % n];
            if breaker.transport_check(&p.id) && breaker.rate_limit_check(&p.id, origin) {
                return Some(p.clone());
            }
        }
        None
    }

    /// Reserve a proxy for a request: checks + reserves probe slot.
    pub fn reserve(
        &self,
        breaker: &crate::circuit_breaker::CircuitBreaker,
        origin: &str,
    ) -> Option<Arc<ProxyClient>> {
        let snap = self.snapshot();
        let n = snap.proxies.len();
        if n == 0 {
            return None;
        }
        let start = self.rr.fetch_add(1, Ordering::Relaxed) % n;
        for i in 0..n {
            let p = &snap.proxies[(start + i) % n];
            // reserve both; if one fails cancel
            if breaker.transport_reserve(&p.id) {
                if breaker.rate_limit_reserve(&p.id, origin) {
                    return Some(p.clone());
                } else {
                    breaker.transport_cancel(&p.id);
                }
            }
        }
        None
    }
}

fn redacted_id(url: &Url, raw: &str) -> String {
    // Stable opaque-ish identity including credentials via fingerprint, but not raw url.
    let host = url.host_str().unwrap_or("unknown");
    let port = url.port().map(|p| p.to_string()).unwrap_or_default();
    // fingerprint of full url (including creds) for uniqueness, but only expose host:port + short hash
    let mut h = Sha256::new();
    h.update(raw.as_bytes());
    let digest = hex(&h.finalize());
    let short = &digest[..8];
    if port.is_empty() {
        format!("{host}#{short}")
    } else {
        format!("{host}:{port}#{short}")
    }
}

fn hex(bytes: &[u8]) -> String {
    const HEX: &[u8] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0x0f) as usize] as char);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::circuit_breaker::CircuitBreaker;
    use crate::config::Config;

    fn pool_from(toml_text: &str) -> Result<ProxyPool, AppError> {
        let config = Config::from_toml(toml_text).expect("toml must parse");
        ProxyPool::from_config(&config.proxy_pool)
    }

    #[test]
    fn entries_are_parsed() {
        let toml = r#"
[proxy_pool]
reload_interval_secs = 1
connect_timeout_secs = 10
proxies = ["http://proxy-a.example.internal:8080"]

[models]
refresh_interval_secs = 1
stale_ttl_secs = 1
max_entries = 1

[circuit_breaker]
proxy_failure_cooldown_secs = 1
rate_limit_cooldown_secs = 1
model_not_found_cooldown_secs = 1
half_open_max_probes = 1
"#;
        let pool = pool_from(toml).unwrap();
        assert_eq!(pool.len(), 1);
    }

    #[test]
    fn empty_pool_rejected() {
        let toml = r#"
[proxy_pool]
reload_interval_secs = 1
connect_timeout_secs = 10
proxies = []

[models]
refresh_interval_secs = 1
stale_ttl_secs = 1
max_entries = 1

[circuit_breaker]
proxy_failure_cooldown_secs = 1
rate_limit_cooldown_secs = 1
model_not_found_cooldown_secs = 1
half_open_max_probes = 1
"#;
        let err = pool_from(toml).unwrap_err();
        assert!(err.to_string().contains("at least one proxy"));
    }

    #[test]
    fn reload_replaces_snapshot() {
        let toml1 = r#"
[proxy_pool]
reload_interval_secs = 1
connect_timeout_secs = 10
proxies = ["http://proxy-a.example.internal:8080"]

[models]
refresh_interval_secs = 1
stale_ttl_secs = 1
max_entries = 1

[circuit_breaker]
proxy_failure_cooldown_secs = 1
rate_limit_cooldown_secs = 1
model_not_found_cooldown_secs = 1
half_open_max_probes = 1
"#;
        let pool = pool_from(toml1).unwrap();
        let toml2 = r#"
[proxy_pool]
reload_interval_secs = 1
connect_timeout_secs = 10
proxies = ["http://proxy-b.example.internal:8080", "http://proxy-c.example.internal:8080"]

[models]
refresh_interval_secs = 1
stale_ttl_secs = 1
max_entries = 1

[circuit_breaker]
proxy_failure_cooldown_secs = 1
rate_limit_cooldown_secs = 1
model_not_found_cooldown_secs = 1
half_open_max_probes = 1
"#;
        let cfg = Config::from_toml(toml2).unwrap();
        pool.reload(&cfg.proxy_pool).unwrap();
        assert_eq!(pool.len(), 2);
    }

    #[test]
    fn select_skips_quarantined() {
        let toml = r#"
[proxy_pool]
reload_interval_secs = 1
connect_timeout_secs = 10
proxies = ["http://proxy-a.example.internal:8080", "http://proxy-b.example.internal:8080"]

[models]
refresh_interval_secs = 1
stale_ttl_secs = 1
max_entries = 1

[circuit_breaker]
proxy_failure_cooldown_secs = 60
rate_limit_cooldown_secs = 120
model_not_found_cooldown_secs = 300
half_open_max_probes = 1
"#;
        let pool = pool_from(toml).unwrap();
        let cb = CircuitBreaker::default();
        cb.transport_record_failure(pool.snapshot().proxies[0].id.as_str());
        let selected = pool.select(&cb, "https://api.kilo.ai").unwrap();
        assert_ne!(selected.id, pool.snapshot().proxies[0].id);
    }

    #[test]
    fn invalid_proxy_url_rejected() {
        let toml = r#"
[proxy_pool]
reload_interval_secs = 1
connect_timeout_secs = 10
proxies = ["not a url"]

[models]
refresh_interval_secs = 1
stale_ttl_secs = 1
max_entries = 1

[circuit_breaker]
proxy_failure_cooldown_secs = 1
rate_limit_cooldown_secs = 1
model_not_found_cooldown_secs = 1
half_open_max_probes = 1
"#;
        let err = pool_from(toml).unwrap_err();
        assert!(
            err.to_string().contains("[proxy_pool] proxies")
                || err.to_string().contains("proxy_pool")
        );
    }

    #[test]
    fn fingerprint_stable_same_config() {
        let toml = r#"
[proxy_pool]
reload_interval_secs = 1
connect_timeout_secs = 10
proxies = ["http://proxy-a.example.internal:8080"]

[models]
refresh_interval_secs = 1
stale_ttl_secs = 1
max_entries = 1

[circuit_breaker]
proxy_failure_cooldown_secs = 1
rate_limit_cooldown_secs = 1
model_not_found_cooldown_secs = 1
half_open_max_probes = 1
"#;
        let cfg = Config::from_toml(toml).unwrap();
        let fp1 = ProxyPool::fingerprint_of(&cfg.proxy_pool);
        let fp2 = ProxyPool::fingerprint_of(&cfg.proxy_pool);
        assert_eq!(fp1, fp2);
    }

    #[test]
    fn creds_differentiate_id() {
        let toml1 = r#"
[proxy_pool]
reload_interval_secs = 1
connect_timeout_secs = 10
proxies = ["http://user1:pass@host:8080"]

[models]
refresh_interval_secs = 1
stale_ttl_secs = 1
max_entries = 1

[circuit_breaker]
proxy_failure_cooldown_secs = 1
rate_limit_cooldown_secs = 1
model_not_found_cooldown_secs = 1
half_open_max_probes = 1
"#;
        let toml2 = toml1.replace("user1:pass", "user2:pass");
        let p1 = pool_from(toml1).unwrap();
        let p2 = pool_from(&toml2).unwrap();
        assert_ne!(p1.snapshot().proxies[0].id, p2.snapshot().proxies[0].id);
    }

    #[test]
    fn reload_unchanged_no_rebuild() {
        let toml = r#"
[proxy_pool]
reload_interval_secs = 1
connect_timeout_secs = 10
proxies = ["http://proxy-a.example.internal:8080"]

[models]
refresh_interval_secs = 1
stale_ttl_secs = 1
max_entries = 1

[circuit_breaker]
proxy_failure_cooldown_secs = 1
rate_limit_cooldown_secs = 1
model_not_found_cooldown_secs = 1
half_open_max_probes = 1
"#;
        let pool = pool_from(toml).unwrap();
        let cfg = Config::from_toml(toml).unwrap();
        let before = pool.snapshot().proxies[0].id.clone();
        pool.reload(&cfg.proxy_pool).unwrap();
        let after = pool.snapshot().proxies[0].id.clone();
        assert_eq!(before, after);
    }

    #[test]
    fn redaction_no_credentials_in_id() {
        let toml = r#"
[proxy_pool]
reload_interval_secs = 1
connect_timeout_secs = 10
proxies = ["http://secretuser:secretpass@proxy-a.example.internal:8080"]

[models]
refresh_interval_secs = 1
stale_ttl_secs = 1
max_entries = 1

[circuit_breaker]
proxy_failure_cooldown_secs = 1
rate_limit_cooldown_secs = 1
model_not_found_cooldown_secs = 1
half_open_max_probes = 1
"#;
        let pool = pool_from(toml).unwrap();
        let id = pool.snapshot().proxies[0].id.clone();
        assert!(!id.contains("secretuser"));
        assert!(!id.contains("secretpass"));
    }
}
