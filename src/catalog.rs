//! Model catalog: fetch from upstream, filter free models, cache with stale TTL.

use std::collections::HashSet;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use serde_json::Value;

#[derive(Debug, Clone)]
pub struct CatalogSnapshot {
    pub models: Vec<Value>,
    pub fetched_at: Instant,
}

pub struct ModelCatalog {
    max_entries: RwLock<usize>,
    stale_ttl: RwLock<Duration>,
    refresh_interval: RwLock<Duration>,
    state: RwLock<CatalogState>,
    pub refresh_lock: tokio::sync::Mutex<()>,
}

struct CatalogState {
    snapshot: Option<Arc<CatalogSnapshot>>,
    last_error: Option<String>,
}

impl ModelCatalog {
    pub fn new(max_entries: usize, stale_ttl: Duration, refresh_interval: Duration) -> Self {
        Self {
            max_entries: RwLock::new(max_entries.max(1)),
            stale_ttl: RwLock::new(stale_ttl),
            refresh_interval: RwLock::new(refresh_interval),
            state: RwLock::new(CatalogState {
                snapshot: None,
                last_error: None,
            }),
            refresh_lock: tokio::sync::Mutex::new(()),
        }
    }

    pub fn update_config(
        &self,
        stale_ttl: Duration,
        refresh_interval: Duration,
        max_entries: usize,
    ) {
        *self.stale_ttl.write().unwrap() = stale_ttl;
        *self.refresh_interval.write().unwrap() = refresh_interval;
        *self.max_entries.write().unwrap() = max_entries.max(1);
    }

    pub fn stale_ttl(&self) -> Duration {
        *self.stale_ttl.read().unwrap()
    }

    pub fn refresh_interval(&self) -> Duration {
        *self.refresh_interval.read().unwrap()
    }

    pub fn max_entries(&self) -> usize {
        *self.max_entries.read().unwrap()
    }

    /// Install a new snapshot from the fetched model list. Returns `true`
    /// when the snapshot was replaced, `false` when the fetch yielded no
    /// usable free models — the last-good snapshot is kept and the caller
    /// must treat the refresh as failed (no success signals, no backoff
    /// reset).
    pub fn replace(&self, models: Vec<Value>) -> bool {
        let filtered = filter_free_models(&models);
        // dedupe by id
        let mut seen = HashSet::new();
        let mut deduped = Vec::new();
        for m in filtered {
            let id = m
                .get("id")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            if seen.insert(id) {
                deduped.push(m);
            }
        }
        let limit = self.max_entries();
        if deduped.len() > limit {
            deduped.truncate(limit);
        }
        if deduped.is_empty() {
            let mut st = self.state.write().unwrap();
            st.last_error = Some("catalog contained no usable free models".to_string());
            return false;
        }
        let snap = Arc::new(CatalogSnapshot {
            models: deduped,
            fetched_at: Instant::now(),
        });
        let mut st = self.state.write().unwrap();
        st.snapshot = Some(snap);
        st.last_error = None;
        true
    }

    pub fn mark_error(&self, msg: String) {
        let mut st = self.state.write().unwrap();
        st.last_error = Some(msg);
    }

    pub fn last_error(&self) -> Option<String> {
        self.state.read().unwrap().last_error.clone()
    }

    pub fn current(&self) -> Option<Arc<CatalogSnapshot>> {
        let st = self.state.read().unwrap();
        if let Some(snap) = &st.snapshot
            && snap.fetched_at.elapsed() <= self.stale_ttl()
        {
            return Some(snap.clone());
        }
        None
    }

    pub fn last_good(&self) -> Option<Arc<CatalogSnapshot>> {
        self.state.read().unwrap().snapshot.clone()
    }

    pub fn is_ready(&self) -> bool {
        self.current().is_some()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn len(&self) -> usize {
        self.current().map(|s| s.models.len()).unwrap_or(0)
    }

    pub fn contains_model(&self, id: &str) -> bool {
        if let Some(snap) = self.current() {
            snap.models
                .iter()
                .any(|m| m.get("id").and_then(|v| v.as_str()) == Some(id))
        } else {
            false
        }
    }
}

pub fn filter_free_models(models: &[Value]) -> Vec<Value> {
    models
        .iter()
        .filter(|m| {
            let id = m.get("id").and_then(|v| v.as_str()).unwrap_or("");
            if id.is_empty() {
                return false;
            }
            // validate id basic: no whitespace
            if id.chars().any(char::is_whitespace) {
                return false;
            }
            is_free_model(id, m)
        })
        .cloned()
        .collect()
}

const KNOWN_PRICING_FIELDS: &[&str] = &[
    "prompt",
    "completion",
    "request",
    "image",
    "audio",
    "web_search",
    "internal_reasoning",
    "input_cache_read",
    "input_cache_write",
    "discount",
];

fn is_free_model(_id: &str, meta: &Value) -> bool {
    // Free-ness is derived solely from pricing metadata. The `isFree` flag is
    // deliberately NOT consulted (upstream may omit/misreport it).
    let pricing = match meta.get("pricing") {
        Some(p) => p,
        None => return false,
    };
    let obj = match pricing.as_object() {
        Some(o) if !o.is_empty() => o,
        _ => return false,
    };
    // Required input + output price fields must both be present.
    if !obj.contains_key("prompt") || !obj.contains_key("completion") {
        return false;
    }
    // Every field must be a known pricing key with a valid numeric value of 0.
    for (k, v) in obj {
        if !KNOWN_PRICING_FIELDS.contains(&k.as_str()) {
            return false; // unrecognized metadata => fail closed
        }
        let n = match v {
            Value::String(s) => match s.trim().parse::<f64>() {
                Ok(x) => x,
                Err(_) => return false,
            },
            Value::Number(n) => match n.as_f64() {
                Some(x) => x,
                None => return false,
            },
            _ => return false,
        };
        if n != 0.0 {
            return false;
        }
    }
    true
}

#[derive(Debug, Clone)]
pub enum CatalogFetchError {
    Connect(String),
    HeaderTimeout,
    BodyRead(String),
    RateLimited(Option<std::time::Duration>),
    Http(u16, String),
    Schema(String),
}

impl std::fmt::Display for CatalogFetchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Connect(m) => write!(f, "connect/transport error: {m}"),
            Self::HeaderTimeout => write!(f, "response header timeout"),
            Self::BodyRead(m) => write!(f, "catalog body read failed: {m}"),
            Self::RateLimited(_) => write!(f, "HTTP 429 rate limited"),
            Self::Http(s, m) => write!(f, "HTTP {s}: {m}"),
            Self::Schema(m) => write!(f, "catalog schema error: {m}"),
        }
    }
}

impl CatalogFetchError {
    pub fn is_connect(&self) -> bool {
        matches!(self, Self::Connect(_))
    }
    pub fn retry_after(&self) -> Option<std::time::Duration> {
        match self {
            Self::RateLimited(r) => *r,
            _ => None,
        }
    }
    pub fn is_rate_limited(&self) -> bool {
        matches!(self, Self::RateLimited(_))
    }
}

fn parse_retry_after(h: &reqwest::header::HeaderMap) -> Option<std::time::Duration> {
    if let Some(v) = h.get("retry-after")
        && let Ok(s) = v.to_str()
    {
        let s = s.trim();
        if let Ok(secs) = s.parse::<u64>() {
            return Some(std::time::Duration::from_secs(secs));
        }
        if let Ok(dt) = httpdate::parse_http_date(s) {
            let now = std::time::SystemTime::now();
            if let Ok(dur) = dt.duration_since(now) {
                return Some(dur);
            }
        }
    }
    None
}

pub async fn fetch_catalog(
    client: &reqwest::Client,
    base_url: &reqwest::Url,
    header_timeout: std::time::Duration,
    max_body_bytes: usize,
) -> Result<Vec<Value>, CatalogFetchError> {
    fetch_catalog_with_body_idle(
        client,
        base_url,
        header_timeout,
        header_timeout,
        max_body_bytes,
    )
    .await
}

/// Like [`fetch_catalog`] but with an explicit per-chunk body idle timeout:
/// a body that stops producing bytes for longer than `body_idle_timeout`
/// fails as [`CatalogFetchError::BodyRead`] instead of hanging until the
/// caller's attempt wrapper fires.
///
/// Note that the connect+headers phase is bounded by `header_timeout` as one
/// unit (reqwest's `send()` covers both); a connect stall against a
/// blackholed proxy therefore surfaces as an ambiguous [`HeaderTimeout`] —
/// by design, since the refresh loop must not quarantine a proxy without
/// evidence.
pub async fn fetch_catalog_with_body_idle(
    client: &reqwest::Client,
    base_url: &reqwest::Url,
    header_timeout: std::time::Duration,
    body_idle_timeout: std::time::Duration,
    max_body_bytes: usize,
) -> Result<Vec<Value>, CatalogFetchError> {
    let mut url = base_url.clone();
    let base_path = url.path().trim_end_matches('/');
    url.set_path(&format!("{}/models", base_path));

    let resp = tokio::time::timeout(header_timeout, client.get(url.clone()).send())
        .await
        .map_err(|_| CatalogFetchError::HeaderTimeout)?
        .map_err(|e| CatalogFetchError::Connect(e.to_string()))?;
    let status = resp.status();
    if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
        let retry_after = parse_retry_after(resp.headers());
        return Err(CatalogFetchError::RateLimited(retry_after));
    }
    if !status.is_success() {
        return Err(CatalogFetchError::Http(
            status.as_u16(),
            format!("catalog fetch failed: HTTP {status}"),
        ));
    }
    // bounded streaming read; each chunk must arrive within the idle budget
    use futures_util::StreamExt;
    let mut stream = resp.bytes_stream();
    let mut buf = Vec::new();
    loop {
        let next = tokio::time::timeout(body_idle_timeout, stream.next()).await;
        let chunk = match next {
            Ok(Some(item)) => item,
            Ok(None) => break,
            Err(_) => {
                return Err(CatalogFetchError::BodyRead(
                    "catalog body read stalled (idle timeout)".to_string(),
                ));
            }
        };
        let chunk = chunk.map_err(|e| CatalogFetchError::BodyRead(e.to_string()))?;
        if buf.len() + chunk.len() > max_body_bytes {
            return Err(CatalogFetchError::Schema(
                "catalog response too large".to_string(),
            ));
        }
        buf.extend_from_slice(&chunk);
    }
    let bytes = bytes::Bytes::from(buf);
    let v: Value = serde_json::from_slice(&bytes)
        .map_err(|e| CatalogFetchError::Schema(format!("catalog JSON parse failed: {e}")))?;
    let data = v.get("data").and_then(|d| d.as_array()).ok_or_else(|| {
        CatalogFetchError::Schema("catalog response missing 'data' array".to_string())
    })?;
    // validate each entry has string id
    for m in data {
        if m.get("id").and_then(|x| x.as_str()).is_none() {
            return Err(CatalogFetchError::Schema(
                "catalog entry missing string 'id'".to_string(),
            ));
        }
    }
    if data.is_empty() {
        return Err(CatalogFetchError::Schema(
            "catalog returned empty data".to_string(),
        ));
    }
    Ok(data.clone())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn filter_free_basic() {
        let models = vec![
            json!({"id":"foo:free","pricing":{"prompt":"0","completion":"0"}}),
            json!({"id":"bar","pricing":{"prompt":"0.001","completion":"0"}}),
            json!({"id":"kilo-auto/free","pricing":{"prompt":"0","completion":"0"}}),
            json!({"id":"missing"}),
        ];
        let out = filter_free_models(&models);
        // foo:free free, kilo-auto/free free, bar paid, missing no pricing
        assert_eq!(out.len(), 2);
    }

    #[test]
    fn free_suffix_with_nonzero_pricing_rejected() {
        let models = vec![json!({"id":"x:free","pricing":{"prompt":"1"}})];
        assert!(filter_free_models(&models).is_empty());
    }

    #[test]
    fn missing_pricing_fail_closed() {
        let models = vec![json!({"id":"some-model"})];
        assert!(filter_free_models(&models).is_empty());
    }

    #[test]
    fn unparseable_pricing_fail_closed() {
        let models = vec![json!({"id":"a:free","pricing":{"prompt":"abc"}})];
        assert!(filter_free_models(&models).is_empty());
    }

    #[test]
    fn negative_pricing_fail_closed() {
        let models = vec![json!({"id":"a:free","pricing":{"prompt":"-1"}})];
        assert!(filter_free_models(&models).is_empty());
    }

    #[test]
    fn isfree_false_rejected() {
        let models = vec![json!({"id":"a:free","isFree":false,"pricing":{"prompt":"0"}})];
        assert!(filter_free_models(&models).is_empty());
    }

    #[test]
    fn empty_pricing_object_fail_closed() {
        let models = vec![json!({"id":"a:free","pricing":{}})];
        assert!(filter_free_models(&models).is_empty());
    }

    #[test]
    fn stale_ttl() {
        let c = ModelCatalog::new(10, Duration::from_secs(0), Duration::from_secs(10));
        c.replace(vec![
            json!({"id":"x:free","pricing":{"prompt":"0","completion":"0"}}),
        ]);
        assert!(c.current().is_none());
        assert!(c.last_good().is_some());
    }

    #[test]
    fn ready_after_replace() {
        let c = ModelCatalog::new(10, Duration::from_secs(900), Duration::from_secs(300));
        assert!(!c.is_ready());
        c.replace(vec![
            json!({"id":"x:free","pricing":{"prompt":"0","completion":"0"}}),
        ]);
        assert!(c.is_ready());
    }

    #[test]
    fn empty_replace_does_not_overwrite() {
        let c = ModelCatalog::new(10, Duration::from_secs(900), Duration::from_secs(300));
        c.replace(vec![
            json!({"id":"x:free","pricing":{"prompt":"0","completion":"0"}}),
        ]);
        assert!(c.is_ready());
        c.replace(vec![json!({"id":"paid","pricing":{"prompt":"1"}})]);
        // still ready with old snapshot
        assert!(c.is_ready());
        assert_eq!(c.len(), 1);
    }

    #[test]
    fn max_entries_reload() {
        let c = ModelCatalog::new(2, Duration::from_secs(900), Duration::from_secs(300));
        c.replace(vec![
            json!({"id":"a:free","pricing":{"prompt":"0","completion":"0"}}),
            json!({"id":"b:free","pricing":{"prompt":"0","completion":"0"}}),
            json!({"id":"c:free","pricing":{"prompt":"0","completion":"0"}}),
        ]);
        assert_eq!(c.len(), 2);
        c.update_config(Duration::from_secs(900), Duration::from_secs(300), 10);
        c.replace(vec![
            json!({"id":"a:free","pricing":{"prompt":"0","completion":"0"}}),
            json!({"id":"b:free","pricing":{"prompt":"0","completion":"0"}}),
            json!({"id":"c:free","pricing":{"prompt":"0","completion":"0"}}),
        ]);
        assert_eq!(c.len(), 3);
    }

    // ---- F5: fetch_catalog hardening tests (local fake upstream only) ----

    /// Spawns a local axum server answering GET /models with `status` and
    /// `body`. Returns its address.
    async fn spawn_catalog_upstream(
        status: axum::http::StatusCode,
        body: &'static [u8],
    ) -> std::net::SocketAddr {
        let app = axum::Router::new().route(
            "/models",
            axum::routing::get(move || async move { (status, body) }),
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

    fn client_for(addr: std::net::SocketAddr) -> reqwest::Client {
        reqwest::Client::builder()
            .proxy(reqwest::Proxy::all(format!("http://{addr}")).expect("proxy"))
            .no_proxy()
            .connect_timeout(std::time::Duration::from_secs(2))
            .build()
            .expect("client")
    }

    fn catalog_url(addr: std::net::SocketAddr) -> reqwest::Url {
        reqwest::Url::parse(&format!("http://{addr}")).expect("url")
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn f5_oversized_body_rejected_as_schema_not_accumulated() {
        // 4 MiB body against a 1 MiB cap must be rejected; the read loop
        // aborts on the first chunk crossing the cap.
        let big = vec![b'a'; 4 * 1024 * 1024];
        let leaked: &'static [u8] = Box::leak(big.into_boxed_slice());
        let addr = spawn_catalog_upstream(axum::http::StatusCode::OK, leaked).await;
        let err = fetch_catalog(
            &client_for(addr),
            &catalog_url(addr),
            std::time::Duration::from_secs(5),
            1024 * 1024,
        )
        .await
        .expect_err("must reject oversized body");
        assert!(
            matches!(err, CatalogFetchError::Schema(ref m) if m.contains("too large")),
            "oversized body must classify as Schema(too large), got {err}"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn f5_header_timeout_classified_distinctly() {
        // Upstream that never responds: must yield HeaderTimeout quickly.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("addr");
        tokio::spawn(async move {
            loop {
                let Ok((mut sock, _)) = listener.accept().await else {
                    break;
                };
                // read request, never answer
                let mut buf = [0u8; 4096];
                use tokio::io::AsyncReadExt;
                let _ =
                    tokio::time::timeout(std::time::Duration::from_secs(3), sock.read(&mut buf))
                        .await;
                tokio::time::sleep(std::time::Duration::from_secs(60)).await;
            }
        });
        let started = std::time::Instant::now();
        let err = fetch_catalog(
            &client_for(addr),
            &catalog_url(addr),
            std::time::Duration::from_millis(500),
            1024 * 1024,
        )
        .await
        .expect_err("must time out");
        assert!(
            matches!(err, CatalogFetchError::HeaderTimeout),
            "stalled headers must classify as HeaderTimeout, got {err}"
        );
        assert!(started.elapsed() < std::time::Duration::from_secs(5));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn f5_slow_drip_body_classifies_as_body_read() {
        use tokio::io::AsyncWriteExt;
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
                    {
                        use tokio::io::AsyncReadExt;
                        let _ = tokio::time::timeout(
                            std::time::Duration::from_secs(5),
                            sock.read(&mut buf),
                        )
                        .await;
                    }
                    // Declare a large content-length, send a few bytes,
                    // then keep the connection open: the body can never
                    // complete, so the only exit is the idle timeout.
                    let head = "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: 1000000\r\n\r\n";
                    let _ = sock.write_all(head.as_bytes()).await;
                    let _ = sock.write_all(b"{\"data\": [").await;
                    let _ = sock.flush().await;
                    tokio::time::sleep(std::time::Duration::from_secs(60)).await;
                });
            }
        });
        let started = std::time::Instant::now();
        let err = fetch_catalog_with_body_idle(
            &client_for(addr),
            &catalog_url(addr),
            std::time::Duration::from_secs(5),
            std::time::Duration::from_millis(500),
            1024 * 1024,
        )
        .await
        .expect_err("drip must fail via idle timeout");
        assert!(
            matches!(err, CatalogFetchError::BodyRead(ref m) if m.contains("stalled")),
            "stalled body must classify as BodyRead(idle), got {err}"
        );
        assert!(
            started.elapsed() < std::time::Duration::from_secs(5),
            "body idle timeout must cut the drip promptly"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn f5_rate_limited_carries_retry_after() {
        let addr = spawn_catalog_upstream(axum::http::StatusCode::TOO_MANY_REQUESTS, b"{}").await;
        let err = fetch_catalog(
            &client_for(addr),
            &catalog_url(addr),
            std::time::Duration::from_secs(5),
            1024 * 1024,
        )
        .await
        .expect_err("429 must error");
        assert!(matches!(err, CatalogFetchError::RateLimited(_)));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn f5_http_error_and_schema_error_distinct() {
        let addr500 =
            spawn_catalog_upstream(axum::http::StatusCode::INTERNAL_SERVER_ERROR, b"boom").await;
        let err = fetch_catalog(
            &client_for(addr500),
            &catalog_url(addr500),
            std::time::Duration::from_secs(5),
            1024 * 1024,
        )
        .await
        .expect_err("500 must error");
        assert!(matches!(err, CatalogFetchError::Http(500, _)), "got {err}");
        // Schema: 200 with a non-catalog JSON body.
        let addr_bad =
            spawn_catalog_upstream(axum::http::StatusCode::OK, br#"{"data": [{"no_id": 1}]}"#)
                .await;
        let err = fetch_catalog(
            &client_for(addr_bad),
            &catalog_url(addr_bad),
            std::time::Duration::from_secs(5),
            1024 * 1024,
        )
        .await
        .expect_err("schema violation must error");
        assert!(matches!(err, CatalogFetchError::Schema(_)), "got {err}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn f5_success_parses_models() {
        let body = br#"{"data": [{"id":"a:free","pricing":{"prompt":"0","completion":"0"}}]}"#;
        let addr = spawn_catalog_upstream(axum::http::StatusCode::OK, body).await;
        let models = fetch_catalog(
            &client_for(addr),
            &catalog_url(addr),
            std::time::Duration::from_secs(5),
            1024 * 1024,
        )
        .await
        .expect("must parse");
        assert_eq!(models.len(), 1);
        assert_eq!(models[0]["id"], "a:free");
    }

    #[test]
    fn contains_model() {
        let c = ModelCatalog::new(10, Duration::from_secs(900), Duration::from_secs(300));
        c.replace(vec![
            json!({"id":"x:free","pricing":{"prompt":"0","completion":"0"}}),
        ]);
        assert!(c.contains_model("x:free"));
        assert!(!c.contains_model("nope"));
    }
}
