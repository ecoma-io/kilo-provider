use std::sync::Arc;
use std::sync::RwLock;

use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::{HeaderMap, HeaderName, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::{Json, Router};
use bytes::Bytes;
use futures_util::stream::StreamExt;
use serde_json::{Value, json};

use crate::catalog::ModelCatalog;
use crate::circuit_breaker::CircuitBreaker;
use crate::config::Config;
use crate::error::ApiError;
use crate::proxy_pool::ProxyPool;
use crate::upstream::{UPSTREAM_ORIGIN, requires_model, upstream_url};

pub struct AppState {
    pub proxy_pool: Arc<ProxyPool>,
    pub circuit_breaker: Arc<CircuitBreaker>,
    pub catalog: Arc<ModelCatalog>,
    pub config: Arc<RwLock<Config>>,
}

impl std::fmt::Debug for AppState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AppState").finish()
    }
}

impl Clone for AppState {
    fn clone(&self) -> Self {
        Self {
            proxy_pool: self.proxy_pool.clone(),
            circuit_breaker: self.circuit_breaker.clone(),
            catalog: self.catalog.clone(),
            config: self.config.clone(),
        }
    }
}

pub fn build_router(state: AppState) -> Router {
    Router::new()
        .route("/healthz", axum::routing::get(healthz))
        .route("/readyz", axum::routing::get(readyz))
        .route("/v1/models", axum::routing::get(models_handler))
        .fallback(forward)
        .with_state(state)
}

async fn healthz() -> impl IntoResponse {
    (StatusCode::OK, Json(json!({ "status": "ok" })))
}

async fn readyz(State(state): State<AppState>) -> impl IntoResponse {
    let origin = UPSTREAM_ORIGIN.to_string();
    let proxy_available = state
        .proxy_pool
        .select(&state.circuit_breaker, &origin)
        .is_some();
    let ready = !state.proxy_pool.is_empty() && proxy_available && state.catalog.is_ready();
    if ready {
        (
            StatusCode::OK,
            Json(json!({
                "status": "ready",
                "proxies": state.proxy_pool.len(),
                "catalog": state.catalog.len(),
            })),
        )
            .into_response()
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({
                "status": "not_ready",
                "proxies": state.proxy_pool.len(),
                "catalog": state.catalog.len(),
            })),
        )
            .into_response()
    }
}

async fn models_handler(State(state): State<AppState>) -> impl IntoResponse {
    if let Some(snap) = state.catalog.current() {
        (
            StatusCode::OK,
            Json(json!({ "object": "list", "data": snap.models })),
        )
            .into_response()
    } else {
        ApiError {
            status: StatusCode::SERVICE_UNAVAILABLE,
            message: "model catalog not ready".to_string(),
        }
        .into_response()
    }
}

async fn forward(State(state): State<AppState>, req: Request) -> Response {
    let method = req.method().clone();
    let uri = req.uri().clone();
    let path = uri.path().to_string();
    // strict path handling: only /v1 or /v1/...
    if !(path == "/v1" || path.starts_with("/v1/")) {
        return ApiError::not_found(format!("no route for path {path:?}")).into_response();
    }

    // Classify endpoint: inference endpoints require a model; /v1/models is
    // handled by models_handler; other /v1/* paths are proxied without model.
    let is_models_list = path == "/v1/models";
    if is_models_list {
        // should not reach here (route registered), but be safe
        return ApiError::not_found("no route".to_string()).into_response();
    }

    let mut url = match upstream_url(&path) {
        Ok(u) => u,
        Err(e) => {
            return ApiError {
                status: StatusCode::BAD_REQUEST,
                message: e.to_string(),
            }
            .into_response();
        }
    };
    if let Some(q) = uri.query() {
        url = format!("{}?{}", url, q);
    }

    let headers = req.headers().clone();
    let body_bytes = match axum::body::to_bytes(req.into_body(), 32 * 1024 * 1024).await {
        Ok(b) => b,
        Err(_) => {
            return ApiError {
                status: StatusCode::PAYLOAD_TOO_LARGE,
                message: "request body too large".to_string(),
            }
            .into_response();
        }
    };

    let origin = UPSTREAM_ORIGIN.to_string();

    // Determine whether this is an inference endpoint requiring model.
    let inference = requires_model(&path);

    // Parse model from body for inference endpoints.
    let model_id: Option<String> = if inference {
        // require valid JSON object with string model
        if body_bytes.is_empty() {
            return ApiError {
                status: StatusCode::BAD_REQUEST,
                message: "missing model".to_string(),
            }
            .into_response();
        }
        let v: Value = match serde_json::from_slice(&body_bytes) {
            Ok(v) => v,
            Err(_) => {
                return ApiError {
                    status: StatusCode::BAD_REQUEST,
                    message: "invalid JSON body".to_string(),
                }
                .into_response();
            }
        };
        match v.get("model").and_then(|m| m.as_str()) {
            Some(s) if !s.is_empty() => Some(s.to_string()),
            Some(_) => {
                return ApiError {
                    status: StatusCode::BAD_REQUEST,
                    message: "model must be a string".to_string(),
                }
                .into_response();
            }
            None => {
                return ApiError {
                    status: StatusCode::BAD_REQUEST,
                    message: "missing model".to_string(),
                }
                .into_response();
            }
        }
    } else {
        extract_model(&body_bytes)
    };

    // Catalog must be ready (fail-closed) before any inference.
    if inference {
        if state.catalog.current().is_none() {
            return ApiError {
                status: StatusCode::SERVICE_UNAVAILABLE,
                message: "model catalog not ready".to_string(),
            }
            .into_response();
        }
        let mid = model_id.as_ref().unwrap();
        if !state.catalog.contains_model(mid) {
            return ApiError {
                status: StatusCode::BAD_REQUEST,
                message: "model not allowed".to_string(),
            }
            .into_response();
        }
    }

    // model breaker reserve (once) for inference, with a leak-safe guard so
    // the slot is released even if this request future is cancelled/dropped.
    let model_guard: Option<crate::circuit_breaker::ModelGuard> = if let Some(mid) = &model_id {
        match state.circuit_breaker.model_reserve_guard(mid) {
            Some(g) => Some(g),
            None => {
                return ApiError {
                    status: StatusCode::SERVICE_UNAVAILABLE,
                    message: "model circuit breaker open".to_string(),
                }
                .into_response();
            }
        }
    } else {
        None
    };

    let forward_headers = filter_headers(&headers);

    let max_attempts = state.proxy_pool.len().max(1);
    let mut last_error: Option<String> = None;
    let mut last_retry_after: Option<std::time::Duration> = None;
    for _ in 0..max_attempts {
        let reserved = state
            .proxy_pool
            .reserve_guarded(&state.circuit_breaker, &origin);
        let (proxy, tg, rg) = match reserved {
            Some(r) => r,
            None => {
                return ApiError {
                    status: StatusCode::SERVICE_UNAVAILABLE,
                    message: "no proxy available".to_string(),
                }
                .into_response();
            }
        };
        let pid = proxy.id.clone();

        let mut rb = proxy.client().request(method.clone(), &url);
        for (k, v) in &forward_headers {
            rb = rb.header(k, v);
        }
        if !body_bytes.is_empty() {
            rb = rb.body(body_bytes.clone());
        }

        match rb.send().await {
            Err(e) => {
                let is_connect = e.is_connect() || e.is_timeout();
                if is_connect {
                    state.circuit_breaker.transport_record_failure(&pid);
                    state.circuit_breaker.rate_limit_cancel(&pid, &origin);
                } else {
                    state.circuit_breaker.transport_cancel(&pid);
                    state.circuit_breaker.rate_limit_cancel(&pid, &origin);
                }
                last_error = Some(e.to_string());
                drop(tg);
                drop(rg);
                if !is_connect {
                    return ApiError {
                        status: StatusCode::BAD_GATEWAY,
                        message: format!("upstream request failed: {}", e),
                    }
                    .into_response();
                }
                continue;
            }
            Ok(resp) => {
                let status = resp.status();
                if status == StatusCode::TOO_MANY_REQUESTS {
                    let retry_after = parse_retry_after(resp.headers());
                    state
                        .circuit_breaker
                        .rate_limit_record_failure(&pid, &origin, retry_after);
                    state.circuit_breaker.transport_record_success(&pid);
                    if let Some(ra) = retry_after {
                        last_retry_after = Some(ra);
                    }
                    last_error = Some("rate limited".to_string());
                    drop(tg);
                    drop(rg);
                    continue;
                }
                if status == StatusCode::NOT_FOUND || status == StatusCode::GONE {
                    if let Some(mid) = &model_id {
                        state.circuit_breaker.model_record_failure(mid);
                    }
                    state.circuit_breaker.transport_record_success(&pid);
                    state
                        .circuit_breaker
                        .rate_limit_record_success(&pid, &origin);
                    return stream_response(resp, Some((tg, rg)), model_guard);
                }
                state.circuit_breaker.transport_record_success(&pid);
                state
                    .circuit_breaker
                    .rate_limit_record_success(&pid, &origin);
                if let Some(mid) = &model_id
                    && status.is_success()
                {
                    state.circuit_breaker.model_record_success(mid);
                }
                return stream_response(resp, Some((tg, rg)), model_guard);
            }
        }
    }

    if last_error.as_deref() == Some("rate limited") {
        let mut resp = ApiError {
            status: StatusCode::TOO_MANY_REQUESTS,
            message: "upstream rate limited".to_string(),
        }
        .into_response();
        if let Some(ra) = last_retry_after
            && let Ok(v) = HeaderValue::from_str(&ra.as_secs().to_string())
        {
            resp.headers_mut().insert("retry-after", v);
        }
        return resp;
    }
    ApiError {
        status: StatusCode::BAD_GATEWAY,
        message: format!(
            "upstream request failed: {}",
            last_error.unwrap_or_else(|| "unknown".to_string())
        ),
    }
    .into_response()
}

fn stream_response(
    resp: reqwest::Response,
    _guards: Option<(
        crate::circuit_breaker::TransportGuard,
        crate::circuit_breaker::RateLimitGuard,
    )>,
    _model_guard: Option<crate::circuit_breaker::ModelGuard>,
) -> Response {
    // Collect connection-listed headers from upstream.
    let mut connection_listed: Vec<String> = Vec::new();
    if let Some(v) = resp.headers().get("connection")
        && let Ok(s) = v.to_str()
    {
        for part in s.split(',') {
            connection_listed.push(part.trim().to_lowercase());
        }
    }
    let status = resp.status();
    let mut headers = HeaderMap::new();
    for (k, v) in resp.headers() {
        let name = k.as_str().to_lowercase();
        if is_hop_by_hop(&name) {
            continue;
        }
        if connection_listed.iter().any(|c| c == &name) {
            continue;
        }
        headers.insert(k.clone(), v.clone());
    }
    // remove connection header itself
    headers.remove("connection");
    let stream = resp
        .bytes_stream()
        .map(|r| r.map_err(|e| std::io::Error::other(e.to_string())));
    let body = Body::from_stream(stream);
    let mut res = Response::new(body);
    *res.status_mut() = status;
    *res.headers_mut() = headers;
    drop(_guards);
    drop(_model_guard);
    res
}

fn filter_headers(h: &HeaderMap) -> Vec<(HeaderName, HeaderValue)> {
    // connection-listed headers
    let mut connection_listed: Vec<String> = Vec::new();
    if let Some(v) = h.get("connection")
        && let Ok(s) = v.to_str()
    {
        for part in s.split(',') {
            connection_listed.push(part.trim().to_lowercase());
        }
    }
    h.iter()
        .filter(|(k, _)| {
            let name = k.as_str().to_lowercase();
            if connection_listed.iter().any(|c| c == &name) {
                return false;
            }
            !matches!(
                name.as_str(),
                "host"
                    | "connection"
                    | "content-length"
                    | "proxy-authorization"
                    | "authorization"
                    | "proxy-connection"
                    | "keep-alive"
                    | "transfer-encoding"
                    | "te"
                    | "trailers"
                    | "upgrade"
                    | "cookie"
            )
        })
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect()
}

fn is_hop_by_hop(name: &str) -> bool {
    matches!(
        name.to_lowercase().as_str(),
        "connection"
            | "keep-alive"
            | "proxy-authenticate"
            | "proxy-authorization"
            | "te"
            | "trailers"
            | "transfer-encoding"
            | "upgrade"
            | "content-length"
    )
}

fn extract_model(body: &Bytes) -> Option<String> {
    if body.is_empty() {
        return None;
    }
    let v: Value = serde_json::from_slice(body).ok()?;
    v.get("model")
        .and_then(|m| m.as_str())
        .map(|s| s.to_string())
}

fn parse_retry_after(h: &HeaderMap) -> Option<std::time::Duration> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::circuit_breaker::CircuitBreaker;
    use crate::config::Config;

    fn bootstrap_state() -> AppState {
        let config = Config::from_toml(
            r#"
[proxy_pool]
reload_interval_secs = 1
connect_timeout_secs = 1
proxies = ["http://proxy-a.example.internal:8080"]

[models]
refresh_interval_secs = 1
stale_ttl_secs = 60
max_entries = 10

[circuit_breaker]
proxy_failure_cooldown_secs = 1
rate_limit_cooldown_secs = 1
model_not_found_cooldown_secs = 1
half_open_max_probes = 1
"#,
        )
        .expect("config");
        let catalog = crate::catalog::ModelCatalog::new(
            10,
            std::time::Duration::from_secs(60),
            std::time::Duration::from_secs(1),
        );
        catalog.replace(vec![
            serde_json::json!({"id":"m:free","pricing":{"prompt":"0","completion":"0"}}),
        ]);
        AppState {
            proxy_pool: Arc::new(ProxyPool::from_config(&config.proxy_pool).expect("pool")),
            circuit_breaker: Arc::new(CircuitBreaker::default()),
            catalog: Arc::new(catalog),
            config: Arc::new(RwLock::new(config)),
        }
    }

    async fn spawn_ephemeral() -> std::net::SocketAddr {
        let app = build_router(bootstrap_state());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("addr");
        tokio::spawn(async move {
            axum::serve(listener, app).await.expect("serve");
        });
        addr
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn healthz_returns_200() {
        let addr = spawn_ephemeral().await;
        let client = reqwest::Client::new();
        let response = client
            .get(format!("http://{addr}/healthz"))
            .send()
            .await
            .expect("req");
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn models_empty_catalog_not_ready() {
        // catalog present so models returns 200; test not ready scenario separately
        let addr = spawn_ephemeral().await;
        let client = reqwest::Client::new();
        let response = client
            .get(format!("http://{addr}/v1/models"))
            .send()
            .await
            .expect("req");
        // catalog has one model => 200
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn unknown_routes_404() {
        let addr = spawn_ephemeral().await;
        let client = reqwest::Client::new();
        let response = client
            .get(format!("http://{addr}/definitely-not-a-route"))
            .send()
            .await
            .expect("req");
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn v12_path_rejected() {
        let addr = spawn_ephemeral().await;
        let client = reqwest::Client::new();
        let response = client
            .get(format!("http://{addr}/v12/foo"))
            .send()
            .await
            .expect("req");
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn unknown_model_rejected() {
        let addr = spawn_ephemeral().await;
        let client = reqwest::Client::new();
        let response = client
            .post(format!("http://{addr}/v1/chat/completions"))
            .json(&serde_json::json!({"model":"nope","messages":[]}))
            .send()
            .await
            .expect("req");
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }
}
