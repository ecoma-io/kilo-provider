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

use crate::admission::AdmissionController;
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
    pub admission: Arc<AdmissionController>,
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
            admission: self.admission.clone(),
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
        // Normalize to the OpenAI-compatible /v1/models schema: every entry
        // must carry object="model", created and owned_by even when the
        // upstream omits them.
        let data: Vec<Value> = snap
            .models
            .iter()
            .map(|m| {
                let mut m = m.clone();
                if let Some(obj) = m.as_object_mut() {
                    obj.entry("object".to_string())
                        .or_insert_with(|| json!("model"));
                    obj.entry("created".to_string()).or_insert_with(|| json!(0));
                    obj.entry("owned_by".to_string())
                        .or_insert_with(|| json!("kilo"));
                }
                m
            })
            .collect();
        (
            StatusCode::OK,
            Json(json!({ "object": "list", "data": data })),
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

    let permit = match state.admission.try_acquire() {
        Some(p) => p,
        None => {
            return ApiError {
                status: StatusCode::SERVICE_UNAVAILABLE,
                message: "too many concurrent requests".to_string(),
            }
            .into_response();
        }
    };

    let cfg = state.config.read().unwrap().clone();
    let body_timeout = cfg.proxy_pool.request_body_timeout();
    let body_bytes = match tokio::time::timeout(
        body_timeout,
        axum::body::to_bytes(req.into_body(), 32 * 1024 * 1024),
    )
    .await
    {
        Ok(Ok(b)) => b,
        Ok(Err(e)) if !e.to_string().contains("length limit exceeded") => {
            // client aborted mid-upload (connection reset while reading the
            // request body) — not a payload-size violation. The size-limit
            // case surfaces as http_body_util::LengthLimitError whose Display
            // is "length limit exceeded"; everything else is a read failure.
            return ApiError {
                status: StatusCode::BAD_REQUEST,
                message: "request body read failed".to_string(),
            }
            .into_response();
        }
        Ok(Err(_)) => {
            return ApiError {
                status: StatusCode::PAYLOAD_TOO_LARGE,
                message: "request body too large".to_string(),
            }
            .into_response();
        }
        Err(_) => {
            return ApiError {
                status: StatusCode::REQUEST_TIMEOUT,
                message: "request body read timeout".to_string(),
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
    let mut model_guard: Option<crate::circuit_breaker::ModelGuard> = if inference {
        if let Some(mid) = &model_id {
            match state.circuit_breaker.model_reserve_guard(mid) {
                Some(g) => Some(g),
                None => {
                    drop(permit);
                    return ApiError {
                        status: StatusCode::SERVICE_UNAVAILABLE,
                        message: "model circuit breaker open".to_string(),
                    }
                    .into_response();
                }
            }
        } else {
            None
        }
    } else {
        None
    };

    let forward_headers = filter_headers(&headers);

    let max_attempts = state.proxy_pool.len().max(1);
    let header_timeout = cfg.proxy_pool.response_header_timeout();
    let mut last_error: Option<String> = None;
    let mut last_retry_after: Option<std::time::Duration> = None;
    for _ in 0..max_attempts {
        let reserved = state
            .proxy_pool
            .reserve_guarded(&state.circuit_breaker, &origin);
        let (proxy, mut tg, mut rg) = match reserved {
            Some(r) => r,
            None => {
                drop(permit);
                drop(model_guard);
                // If we were already rate-limited, preserve 429 semantics
                // instead of masking it with 503.
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
                return ApiError {
                    status: StatusCode::SERVICE_UNAVAILABLE,
                    message: "no proxy available".to_string(),
                }
                .into_response();
            }
        };
        let _pid = proxy.id.clone();

        let mut rb = proxy.client().request(method.clone(), &url);
        for (k, v) in &forward_headers {
            rb = rb.header(k, v);
        }
        if !body_bytes.is_empty() {
            rb = rb.body(body_bytes.clone());
        }

        let send_res = tokio::time::timeout(header_timeout, rb.send()).await;
        match send_res {
            Err(_) => {
                // response header timeout: ambiguous, do not retry inference
                last_error = Some("response header timeout".to_string());
                drop(tg);
                drop(rg);
                break;
            }
            Ok(Err(e)) => {
                let is_connect = e.is_connect();
                if is_connect {
                    tg.record_failure();
                    last_error = Some(e.to_string());
                    drop(tg);
                    drop(rg);
                    continue;
                } else {
                    drop(tg);
                    drop(rg);
                    drop(permit);
                    drop(model_guard);
                    return ApiError {
                        status: StatusCode::BAD_GATEWAY,
                        message: format!("upstream request failed: {}", e),
                    }
                    .into_response();
                }
            }
            Ok(Ok(resp)) => {
                let status = resp.status();
                if status == StatusCode::TOO_MANY_REQUESTS {
                    let retry_after = parse_retry_after(resp.headers());
                    let capped = retry_after.map(|ra| {
                        let max = cfg.circuit_breaker.retry_after_max_secs.max(1);
                        if ra.as_secs() > max {
                            std::time::Duration::from_secs(max)
                        } else {
                            ra
                        }
                    });
                    rg.record_failure(capped);
                    tg.record_success();
                    if let Some(ra) = capped {
                        last_retry_after = Some(ra);
                    }
                    last_error = Some("rate limited".to_string());
                    drop(tg);
                    drop(rg);
                    continue;
                }
                if (status == StatusCode::NOT_FOUND || status == StatusCode::GONE) && inference {
                    tg.record_success();
                    rg.record_success();
                    // Model breaker outcome is decided by the classified
                    // stream (bounded prefix + deadline); the guard stays
                    // armed so ambiguity cannot open the breaker.
                    return classified_stream_response(
                        resp,
                        Some((tg, rg)),
                        model_guard,
                        Some(permit),
                        cfg.proxy_pool.stream_idle_timeout(),
                        classifier_probe_len(),
                        classify_deadline(),
                    );
                }
                tg.record_success();
                rg.record_success();
                if status.is_success()
                    && inference
                    && let Some(g) = model_guard.as_mut()
                {
                    g.record_success();
                }
                return stream_response(
                    resp,
                    Some((tg, rg)),
                    model_guard,
                    cfg.proxy_pool.stream_idle_timeout(),
                    Some(permit),
                );
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
    drop(model_guard);
    drop(permit);
    let (status, msg) = if last_error.as_deref() == Some("response header timeout") {
        (
            StatusCode::GATEWAY_TIMEOUT,
            "upstream response header timeout".to_string(),
        )
    } else {
        (
            StatusCode::BAD_GATEWAY,
            format!(
                "upstream request failed: {}",
                last_error.unwrap_or_else(|| "unknown".to_string())
            ),
        )
    };
    ApiError {
        status,
        message: msg,
    }
    .into_response()
}

/// Prefix of an error body kept for model-unavailable classification.
const CLASSIFY_READ_LIMIT: usize = 64 * 1024;
/// Hard deadline for the classification read on a 404/410 body.
const CLASSIFY_DEADLINE: std::time::Duration = std::time::Duration::from_secs(30);
/// Forward remaining bytes in chunks no larger than this.
const FORWARD_CHUNK_CAP: usize = 64 * 1024;

fn classifier_probe_len() -> usize {
    CLASSIFY_READ_LIMIT
}

/// Classification deadline; tests shrink it via the cfg(test)-only override
/// so the deadline path is observable without 30s waits.
fn classify_deadline() -> std::time::Duration {
    #[cfg(test)]
    {
        let overridden = CLASSIFY_DEADLINE_OVERRIDE
            .lock()
            .expect("classify deadline lock")
            .map(std::time::Duration::from_millis);
        if let Some(d) = overridden {
            return d;
        }
    }
    CLASSIFY_DEADLINE
}

#[cfg(test)]
static CLASSIFY_DEADLINE_OVERRIDE: std::sync::Mutex<Option<u64>> = std::sync::Mutex::new(None);

/// Test-only upstream base override (see `upstream::upstream_url`).
#[cfg(test)]
pub(crate) fn test_upstream_base() -> Option<String> {
    TEST_UPSTREAM_BASE
        .lock()
        .expect("test upstream lock")
        .clone()
}

/// Serializes tests that swap the global upstream override so parallel
/// test threads cannot observe each other's fake upstream. Async-aware mutex
/// so the guard can be held across `.await` in tests without blocking the
/// runtime.
#[cfg(test)]
pub(crate) static TEST_UPSTREAM_SERIALIZE: tokio::sync::Mutex<()> =
    tokio::sync::Mutex::const_new(());

#[cfg(test)]
static TEST_UPSTREAM_BASE: std::sync::Mutex<Option<String>> = std::sync::Mutex::new(None);

/// Copy upstream response headers into a client-facing header map, dropping
/// hop-by-hop headers and any header listed by the upstream `Connection`
/// header (same policy as `stream_response`).
fn client_response_headers(resp_headers: &HeaderMap) -> HeaderMap {
    let mut connection_listed: Vec<String> = Vec::new();
    if let Some(v) = resp_headers.get("connection")
        && let Ok(s) = v.to_str()
    {
        for part in s.split(',') {
            connection_listed.push(part.trim().to_lowercase());
        }
    }
    let mut headers = HeaderMap::new();
    for (k, v) in resp_headers {
        let name = k.as_str().to_lowercase();
        if is_hop_by_hop(&name) || connection_listed.iter().any(|c| c == &name) {
            continue;
        }
        // append (not insert): repeated headers such as multiple set-cookie
        // must all reach the client instead of collapsing to the last value.
        headers.append(k.clone(), v.clone());
    }
    headers.remove("connection");
    headers
}

/// Response path for upstream 404/410 inference errors.
///
/// Classification reads a bounded prefix of the body under a hard deadline;
/// every byte read is re-emitted to the client, then the remainder of the
/// stream is forwarded unchanged. Status/headers are fixed before the body
/// starts, so a classification timeout or mid-stream error surfaces as a
/// stream error and never as a silently truncated complete body. On an
/// unclassifiable outcome (timeout, body error) the model breaker is NOT
/// opened: the guard is cancelled instead.
#[allow(clippy::too_many_arguments)]
fn classified_stream_response(
    resp: reqwest::Response,
    guards: Option<(
        crate::circuit_breaker::TransportGuard,
        crate::circuit_breaker::RateLimitGuard,
    )>,
    model_guard: Option<crate::circuit_breaker::ModelGuard>,
    permit: Option<crate::admission::AdmissionPermit>,
    idle: std::time::Duration,
    probe_len: usize,
    classify_deadline: std::time::Duration,
) -> Response {
    let status = resp.status();
    let headers = client_response_headers(resp.headers());
    let inner = resp.bytes_stream();
    let deadline_at = tokio::time::Instant::now() + classify_deadline;
    let stream = futures_util::stream::unfold(
        (
            inner,
            guards,
            model_guard,
            permit,
            idle,
            0usize,               // bytes kept for classification so far
            Vec::<u8>::new(),     // classification prefix buffer
            None::<bytes::Bytes>, // pending forwarding bytes
            deadline_at,
            false, // done
        ),
        move |(mut s, hg, hmg, hp, idle, kept, mut probe, mut pending, deadline_at, done)| async move {
            if done {
                return None;
            }
            // Drain pending forwarding bytes first (capped chunk size).
            if let Some(buf) = pending.take() {
                if buf.len() > FORWARD_CHUNK_CAP {
                    let head = buf.slice(..FORWARD_CHUNK_CAP);
                    pending = Some(buf.slice(FORWARD_CHUNK_CAP..));
                    return Some((
                        Ok(head),
                        (
                            s,
                            hg,
                            hmg,
                            hp,
                            idle,
                            kept,
                            probe,
                            pending,
                            deadline_at,
                            false,
                        ),
                    ));
                }
                return Some((
                    Ok(buf),
                    (
                        s,
                        hg,
                        hmg,
                        hp,
                        idle,
                        kept,
                        probe,
                        pending,
                        deadline_at,
                        false,
                    ),
                ));
            }
            // Classification phase.
            if kept < probe_len {
                let remaining = deadline_at
                    .saturating_duration_since(tokio::time::Instant::now())
                    .max(std::time::Duration::from_millis(1));
                match tokio::time::timeout(remaining, s.next()).await {
                    Ok(Some(Ok(bytes))) => {
                        let take = bytes.len().min(probe_len - kept);
                        probe.extend_from_slice(&bytes[..take]);
                        let next_kept = kept + take;
                        let rest = bytes.slice(take..);
                        if !rest.is_empty() {
                            pending = Some(rest);
                        }
                        let mut hmg = hmg;
                        if next_kept >= probe_len {
                            // classification decided; record outcome and free buffer
                            let unavailable = is_model_unavailable_body(&probe);
                            drop(std::mem::take(&mut probe));
                            if let Some(g) = hmg.as_mut() {
                                if unavailable {
                                    g.record_failure();
                                } else {
                                    g.record_success();
                                }
                            }
                        }
                        return Some((
                            Ok(bytes.slice(..take)),
                            (
                                s,
                                hg,
                                hmg,
                                hp,
                                idle,
                                next_kept,
                                probe,
                                pending,
                                deadline_at,
                                false,
                            ),
                        ));
                    }
                    Ok(Some(Err(e))) => {
                        // upstream body error during classification: ambiguous
                        drop(hmg);
                        return Some((
                            Err(std::io::Error::other(e.to_string())),
                            (
                                s,
                                hg,
                                None,
                                hp,
                                idle,
                                kept,
                                probe,
                                pending,
                                deadline_at,
                                true,
                            ),
                        ));
                    }
                    Ok(None) => {
                        // stream ended inside the probe window: classify on what we got
                        let unavailable = is_model_unavailable_body(&probe);
                        drop(std::mem::take(&mut probe));
                        let mut hmg = hmg;
                        if let Some(g) = hmg.as_mut() {
                            if unavailable {
                                g.record_failure();
                            } else {
                                g.record_success();
                            }
                        }
                        return None;
                    }
                    Err(_) => {
                        // classification deadline hit: ambiguous, do not open breaker
                        drop(hmg);
                        return Some((
                            Err(std::io::Error::new(
                                std::io::ErrorKind::TimedOut,
                                "upstream error body read timed out",
                            )),
                            (
                                s,
                                hg,
                                None,
                                hp,
                                idle,
                                kept,
                                probe,
                                pending,
                                deadline_at,
                                true,
                            ),
                        ));
                    }
                }
            }
            // Plain forwarding phase with idle timeout.
            match tokio::time::timeout(idle, s.next()).await {
                Ok(Some(Ok(bytes))) => Some((
                    Ok(bytes),
                    (
                        s,
                        hg,
                        hmg,
                        hp,
                        idle,
                        kept,
                        probe,
                        pending,
                        deadline_at,
                        false,
                    ),
                )),
                Ok(Some(Err(e))) => Some((
                    Err(std::io::Error::other(e.to_string())),
                    (
                        s,
                        hg,
                        hmg,
                        hp,
                        idle,
                        kept,
                        probe,
                        pending,
                        deadline_at,
                        true,
                    ),
                )),
                Ok(None) => None,
                Err(_) => Some((
                    Err(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "stream idle timeout",
                    )),
                    (
                        s,
                        hg,
                        hmg,
                        hp,
                        idle,
                        kept,
                        probe,
                        pending,
                        deadline_at,
                        true,
                    ),
                )),
            }
        },
    );
    let body = Body::from_stream(stream);
    let mut res = Response::new(body);
    *res.status_mut() = status;
    *res.headers_mut() = headers;
    res
}

fn stream_response(
    resp: reqwest::Response,
    _guards: Option<(
        crate::circuit_breaker::TransportGuard,
        crate::circuit_breaker::RateLimitGuard,
    )>,
    _model_guard: Option<crate::circuit_breaker::ModelGuard>,
    idle: std::time::Duration,
    _permit: Option<crate::admission::AdmissionPermit>,
) -> Response {
    let status = resp.status();
    let headers = client_response_headers(resp.headers());
    let held_guards = _guards;
    let held_model_guard = _model_guard;
    let held_permit = _permit;
    let inner = resp.bytes_stream();
    let stream = futures_util::stream::unfold(
        (
            inner,
            held_guards,
            held_model_guard,
            held_permit,
            idle,
            false,
        ),
        |(mut s, hg, hmg, hp, idle, done)| async move {
            if done {
                return None;
            }
            match tokio::time::timeout(idle, s.next()).await {
                Ok(Some(Ok(bytes))) => {
                    let _ = (&hg, &hmg, &hp);
                    Some((Ok(bytes), (s, hg, hmg, hp, idle, false)))
                }
                Ok(Some(Err(e))) => Some((
                    Err(std::io::Error::other(e.to_string())),
                    (s, hg, hmg, hp, idle, true),
                )),
                Ok(None) => None,
                Err(_) => Some((
                    Err(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "stream idle timeout",
                    )),
                    (s, hg, hmg, hp, idle, true),
                )),
            }
        },
    );
    let body = Body::from_stream(stream);
    let mut res = Response::new(body);
    *res.status_mut() = status;
    *res.headers_mut() = headers;
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

fn is_model_unavailable_body(body: &[u8]) -> bool {
    let text = String::from_utf8_lossy(body).to_lowercase();
    // Try JSON first.
    if let Ok(v) = serde_json::from_slice::<serde_json::Value>(body) {
        if let Some(code) = v
            .get("error")
            .and_then(|e| e.get("code"))
            .and_then(|c| c.as_str())
            && (code == "model_not_found"
                || code == "model_not_available"
                || code == "model_does_not_exist")
        {
            return true;
        }
        if let Some(msg) = v
            .get("error")
            .and_then(|e| e.get("message"))
            .and_then(|m| m.as_str())
        {
            let m = msg.to_lowercase();
            if m.contains("model")
                && (m.contains("not found")
                    || m.contains("does not exist")
                    || m.contains("not available")
                    || m.contains("unknown"))
            {
                return true;
            }
        }
    }
    text.contains("model")
        && (text.contains("not found")
            || text.contains("does not exist")
            || text.contains("not available")
            || text.contains("unknown model"))
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
            config: Arc::new(RwLock::new(config.clone())),
            admission: Arc::new(AdmissionController::new(
                config.proxy_pool.max_concurrent_requests,
            )),
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

    // ---- F1/F2: 404/410 classification + full body forwarding ----

    /// Fake upstream: POST handler with controllable status, headers, chunks.
    /// Request bodies accepted and stream held open by caller control.
    async fn spawn_upstream_with_404_big_body()
    -> (std::net::SocketAddr, Arc<std::sync::atomic::AtomicUsize>) {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let hits = Arc::new(AtomicUsize::new(0));
        let hits2 = hits.clone();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("addr");
        let app = axum::Router::new().fallback(move |_req: axum::extract::Request| {
            let hits = hits2.clone();
            async move {
                hits.fetch_add(1, Ordering::SeqCst);
                // body > 64 KiB: 100k bytes
                let marker = b"model \"x\" not found";
                let mut body = Vec::with_capacity(100 * 1024);
                body.extend_from_slice(marker);
                body.resize(100 * 1024, b'a');
                (
                    axum::http::StatusCode::NOT_FOUND,
                    [
                        ("content-type", "application/json"),
                        ("x-custom-header", "kept"),
                        ("connection", "close"),
                    ],
                    body,
                )
            }
        });
        tokio::spawn(async move {
            axum::serve(listener, app).await.expect("serve");
        });
        (addr, hits)
    }

    fn state_with_upstream(upstream: std::net::SocketAddr) -> AppState {
        state_with_upstream_capacity(upstream, 4)
    }

    fn state_with_upstream_capacity(
        upstream: std::net::SocketAddr,
        admission_capacity: usize,
    ) -> AppState {
        *TEST_UPSTREAM_BASE.lock().expect("test upstream lock") =
            Some(format!("http://{upstream}/api/gateway"));
        let toml = format!(
            "\n[proxy_pool]\nreload_interval_secs = 1\nconnect_timeout_secs = 1\nproxies = [\"http://placeholder.invalid:1\"]\nmax_concurrent_requests = {admission_capacity}\n\n[models]\nrefresh_interval_secs = 1\nstale_ttl_secs = 60\nmax_entries = 10\n\n[circuit_breaker]\nproxy_failure_cooldown_secs = 1\nrate_limit_cooldown_secs = 1\nmodel_not_found_cooldown_secs = 1\nhalf_open_max_probes = 1\n",
        );
        let config = Config::from_toml(&toml).expect("config");
        let catalog = crate::catalog::ModelCatalog::new(
            10,
            std::time::Duration::from_secs(60),
            std::time::Duration::from_secs(1),
        );
        catalog.replace(vec![
            serde_json::json!({"id":"m:free","pricing":{"prompt":"0","completion":"0"}}),
        ]);
        // Build a pool whose single proxy actually points at the fake upstream
        // by rewriting the proxy URL to the local addr. The proxy client sends
        // requests with absolute-form target through the HTTP proxy; reqwest
        // handles that. We instead construct the pool manually with the fake
        // upstream as an http proxy: axum fallback will receive the request
        // path as absolute URI via CONNECT-less proxying.
        let url = format!("http://{upstream}");
        let pool = ProxyPool::from_config(&{
            let mut c = config.proxy_pool.clone();
            c.proxies = vec![url];
            c
        })
        .expect("pool");
        AppState {
            proxy_pool: Arc::new(pool),
            circuit_breaker: Arc::new(CircuitBreaker::default()),
            catalog: Arc::new(catalog),
            config: Arc::new(RwLock::new(config.clone())),
            admission: Arc::new(AdmissionController::new(admission_capacity)),
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn inference_404_full_body_forwarded_and_breaker_opened() {
        // Serialize with other tests using the global upstream override.
        let _serial = crate::router::TEST_UPSTREAM_SERIALIZE.lock().await;
        let (upstream, hits) = spawn_upstream_with_404_big_body().await;
        let state = state_with_upstream(upstream);
        let app = build_router(state.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("addr");
        tokio::spawn(async move {
            axum::serve(listener, app).await.expect("serve");
        });
        let client = reqwest::Client::new();
        let resp = client
            .post(format!("http://{addr}/v1/chat/completions"))
            .json(
                &serde_json::json!({"model":"m:free","messages":[{"role":"user","content":"hi"}]}),
            )
            .send()
            .await
            .expect("req");
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
        let body = resp.bytes().await.expect("body");
        // marker visible
        assert!(String::from_utf8_lossy(&body).contains("not found"));
        // full 100k body received (prefix + remainder all forwarded)
        assert_eq!(
            body.len(),
            100 * 1024,
            "body must be complete, got {} bytes",
            body.len()
        );
        assert_eq!(
            hits.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "single upstream attempt"
        );
        // model breaker opened for classified model-unavailable body
        assert_eq!(
            state.circuit_breaker.model_state("m:free"),
            crate::circuit_breaker::BreakerState::Open
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn admission_permit_released_after_404_response_completes() {
        // Serialize with other tests using the global upstream override.
        let _serial = crate::router::TEST_UPSTREAM_SERIALIZE.lock().await;
        let (upstream, _hits) = spawn_upstream_with_404_big_body().await;
        let state = state_with_upstream(upstream);
        let app = build_router(state.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("addr");
        tokio::spawn(async move {
            axum::serve(listener, app).await.expect("serve");
        });
        let client = reqwest::Client::new();
        {
            let resp = client
                .post(format!("http://{addr}/v1/chat/completions"))
                .json(&serde_json::json!({"model":"m:free","messages":[]}))
                .send()
                .await
                .expect("req");
            assert_eq!(resp.status(), StatusCode::NOT_FOUND);
            let _ = resp.bytes().await;
        }
        // response fully consumed: permit must be released
        for _ in 0..50 {
            if state.admission.active() == 0 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert_eq!(
            state.admission.active(),
            0,
            "admission permit must be released"
        );
    }

    /// 404 upstream that sends a small non-model error body then holds the
    /// stream open forever. The handler must terminate the client stream via
    /// idle timeout instead of hanging, and must NOT open the model breaker
    /// (unclassifiable/irrelevant body).
    #[tokio::test(flavor = "multi_thread")]
    async fn inference_404_held_stream_terminates_and_does_not_open_breaker() {
        // Serialize with other tests using the global upstream override.
        let _serial = crate::router::TEST_UPSTREAM_SERIALIZE.lock().await;
        use tokio::io::AsyncWriteExt;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let upstream = listener.local_addr().expect("addr");
        tokio::spawn(async move {
            loop {
                let Ok((mut sock, _)) = listener.accept().await else {
                    break;
                };
                tokio::spawn(async move {
                    // read request head (ignore content)
                    let mut buf = [0u8; 8192];
                    use tokio::io::AsyncReadExt;
                    let _ = tokio::time::timeout(
                        std::time::Duration::from_secs(5),
                        sock.read(&mut buf),
                    )
                    .await;
                    let body = b"{\"error\":\"gone fishing\"}";
                    let head = format!(
                        "HTTP/1.1 404 Not Found\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n",
                        body.len()
                    );
                    let _ = sock.write_all(head.as_bytes()).await;
                    let _ = sock.write_all(body).await;
                    let _ = sock.flush().await;
                    // hold the connection open, never finishing (content-length already satisfied though).
                    // To truly hold: claim bigger content-length than we send.
                });
            }
        });
        let state = state_with_upstream(upstream);
        // shrink timeouts so the test is fast
        {
            let mut cfg = state.config.write().expect("cfg lock");
            cfg.proxy_pool.stream_idle_timeout_secs = 1;
        }
        let app = build_router(state.clone());
        let gw = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = gw.local_addr().expect("addr");
        tokio::spawn(async move {
            axum::serve(gw, app).await.expect("serve");
        });
        let client = reqwest::Client::new();
        let started = std::time::Instant::now();
        let resp = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            client
                .post(format!("http://{addr}/v1/chat/completions"))
                .json(&serde_json::json!({"model":"m:free","messages":[]}))
                .send(),
        )
        .await
        .expect("must not hang waiting for headers")
        .expect("req");
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
        // body read must terminate (idle timeout) instead of hanging forever
        let body = tokio::time::timeout(std::time::Duration::from_secs(5), resp.bytes()).await;
        assert!(
            body.is_ok(),
            "stream must terminate via idle timeout, not hang"
        );
        let _ = body.expect("ok");
        assert!(
            started.elapsed() < std::time::Duration::from_secs(9),
            "handler must not hang"
        );
        // unclassifiable body: model breaker must stay Closed
        assert_eq!(
            state.circuit_breaker.model_state("m:free"),
            crate::circuit_breaker::BreakerState::Closed
        );
    }
    /// Mid-stream body error on the 404 classification path must surface as a
    /// stream error, not as a silently truncated complete body.
    #[tokio::test(flavor = "multi_thread")]
    async fn inference_404_body_error_surfaces_as_stream_error() {
        // Serialize with other tests using the global upstream override.
        let _serial = crate::router::TEST_UPSTREAM_SERIALIZE.lock().await;
        use tokio::io::AsyncWriteExt;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let upstream = listener.local_addr().expect("addr");
        tokio::spawn(async move {
            loop {
                let Ok((mut sock, _)) = listener.accept().await else {
                    break;
                };
                tokio::spawn(async move {
                    let mut buf = [0u8; 8192];
                    {
                        use tokio::io::AsyncReadExt;
                        let _ = tokio::time::timeout(
                            std::time::Duration::from_secs(5),
                            sock.read(&mut buf),
                        )
                        .await;
                    }
                    // Claim a large content-length but send only part, then RST.
                    let head = "HTTP/1.1 404 Not Found\r\ncontent-type: text/plain\r\ncontent-length: 100000\r\n\r\n";
                    let _ = sock.write_all(head.as_bytes()).await;
                    let _ = sock.write_all(b"model m:free not found; padding...").await;
                    let _ = sock.flush().await;
                    // abrupt close without the full body
                    drop(sock);
                });
            }
        });
        let state = state_with_upstream(upstream);
        let app = build_router(state.clone());
        let gw = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = gw.local_addr().expect("addr");
        tokio::spawn(async move {
            axum::serve(gw, app).await.expect("serve");
        });
        let client = reqwest::Client::new();
        let resp = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            client
                .post(format!("http://{addr}/v1/chat/completions"))
                .json(&serde_json::json!({"model":"m:free","messages":[]}))
                .send(),
        )
        .await
        .expect("no hang")
        .expect("req");
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
        // Reading the body must fail (aborted mid-stream), never yield a complete 100k body.
        let body = tokio::time::timeout(std::time::Duration::from_secs(5), resp.bytes()).await;
        match body {
            Err(_) => panic!("body read hung"),
            Ok(Err(_)) => { /* stream error surfaced: correct */ }
            Ok(Ok(b)) => {
                assert_ne!(
                    b.len(),
                    100000,
                    "partial body must not be presented as complete"
                );
            }
        }
    }

    /// 429 followed by exhausted proxies must return 429 (not 503) with the
    /// capped Retry-After preserved.
    #[tokio::test(flavor = "multi_thread")]
    async fn rate_limited_then_no_proxy_returns_429() {
        // Serialize with other tests using the global upstream override.
        let _serial = crate::router::TEST_UPSTREAM_SERIALIZE.lock().await;
        // Two-proxy pool; the fake upstream 429s on the first host and never
        // gets hit again because both proxies share the same fake upstream.
        use tokio::io::AsyncWriteExt;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let upstream = listener.local_addr().expect("addr");
        let hits = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let hits2 = hits.clone();
        tokio::spawn(async move {
            loop {
                let Ok((mut sock, _)) = listener.accept().await else {
                    break;
                };
                let hits = hits2.clone();
                tokio::spawn(async move {
                    let mut buf = [0u8; 8192];
                    {
                        use tokio::io::AsyncReadExt;
                        let _ = tokio::time::timeout(
                            std::time::Duration::from_secs(5),
                            sock.read(&mut buf),
                        )
                        .await;
                    }
                    hits.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    let head = "HTTP/1.1 429 Too Many Requests\r\nretry-after: 5\r\ncontent-length: 0\r\n\r\n";
                    let _ = sock.write_all(head.as_bytes()).await;
                    let _ = sock.flush().await;
                });
            }
        });
        let config = Config::from_toml(
            r#"
[proxy_pool]
reload_interval_secs = 1
connect_timeout_secs = 1
proxies = ["http://p1.invalid:1", "http://p2.invalid:2"]

[models]
refresh_interval_secs = 1
stale_ttl_secs = 60
max_entries = 10

[circuit_breaker]
proxy_failure_cooldown_secs = 60
rate_limit_cooldown_secs = 60
model_not_found_cooldown_secs = 60
half_open_max_probes = 1
retry_after_max_secs = 30
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
        *TEST_UPSTREAM_BASE.lock().expect("lock") = Some(format!("http://{upstream}/api/gateway"));
        // Pool with 2 proxies, both pointed at the fake upstream so every
        // attempt gets a 429; after the first 429 the rate-limit breaker
        // opens per (proxy, origin) making both unavailable -> reserve None.
        let mut pool_cfg = config.proxy_pool.clone();
        pool_cfg.proxies = vec![format!("http://{upstream}"), format!("http://{upstream}")];
        let pool = ProxyPool::from_config(&pool_cfg).expect("pool");
        let state = AppState {
            proxy_pool: Arc::new(pool),
            circuit_breaker: Arc::new(CircuitBreaker::new(config.circuit_breaker.clone())),
            catalog: Arc::new(catalog),
            config: Arc::new(RwLock::new(config.clone())),
            admission: Arc::new(AdmissionController::new(4)),
        };
        let app = build_router(state.clone());
        let gw = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = gw.local_addr().expect("addr");
        tokio::spawn(async move {
            axum::serve(gw, app).await.expect("serve");
        });
        let client = reqwest::Client::new();
        let resp = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            client
                .post(format!("http://{addr}/v1/chat/completions"))
                .json(&serde_json::json!({"model":"m:free","messages":[]}))
                .send(),
        )
        .await
        .expect("no hang")
        .expect("req");
        assert_eq!(
            resp.status(),
            StatusCode::TOO_MANY_REQUESTS,
            "must be 429, not 503"
        );
        let ra = resp
            .headers()
            .get("retry-after")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());
        assert_eq!(ra.as_deref(), Some("5"), "Retry-After must be preserved");
        // bounded attempts: both proxies tried at most once
        let h = hits.load(std::sync::atomic::Ordering::SeqCst);
        assert!(h <= 2, "attempts must be bounded by pool size, got {h}");
    }

    // ---- Reviewer A regression tests ----

    /// Raw-TCP fake upstream factory: each connection is answered by the
    /// returned closure so tests control raw header bytes exactly. The
    /// connection is held open for `hold_ms` after writing so the gateway's
    /// reqwest client has time to process the response before FIN.
    async fn spawn_raw_upstream(
        handle: impl Fn(Vec<u8>) -> Vec<u8> + Send + 'static,
        hold_ms: u64,
    ) -> std::net::SocketAddr {
        use tokio::io::AsyncWriteExt;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("addr");
        // move the closure into a std Mutex so the spawned task can call it
        let handler = std::sync::Arc::new(std::sync::Mutex::new(handle));
        tokio::spawn(async move {
            loop {
                let Ok((mut sock, _)) = listener.accept().await else {
                    break;
                };
                let handler = handler.clone();
                tokio::spawn(async move {
                    let mut buf = vec![0u8; 65536];
                    let mut n = 0;
                    {
                        use tokio::io::AsyncReadExt;
                        // Drain the request (head + small body) until a short
                        // quiet period: responding while request bytes are
                        // still unread would RST the buffered response, but
                        // every read must be time-bounded or the client's
                        // wait-for-response and our wait-for-more-data
                        // deadlock.
                        loop {
                            match tokio::time::timeout(
                                std::time::Duration::from_millis(300),
                                sock.read(&mut buf[n..]),
                            )
                            .await
                            {
                                Ok(Ok(0)) | Err(_) | Ok(Err(_)) => break,
                                Ok(Ok(k)) => n += k,
                            }
                            if n > 2 * 1024 * 1024 {
                                break;
                            }
                        }
                    }
                    let resp = {
                        let h = handler.lock().expect("handler lock");
                        h(buf[..n].to_vec())
                    };
                    let _ = sock.write_all(&resp).await;
                    let _ = sock.flush().await;
                    tokio::time::sleep(std::time::Duration::from_millis(hold_ms)).await;
                    let _ = sock.shutdown().await;
                });
            }
        });
        addr
    }

    async fn post_inference(addr: std::net::SocketAddr) -> reqwest::Response {
        let client = reqwest::Client::new();
        tokio::time::timeout(
            std::time::Duration::from_secs(10),
            client
                .post(format!("http://{addr}/v1/chat/completions"))
                .json(&serde_json::json!({"model":"m:free","messages":[]}))
                .send(),
        )
        .await
        .expect("no hang")
        .expect("req")
    }

    /// Header timeout on the first upstream must return 504 and never retry.
    #[tokio::test(flavor = "multi_thread")]
    async fn header_timeout_returns_504_and_never_retries() {
        let _serial = crate::router::TEST_UPSTREAM_SERIALIZE.lock().await;
        let hits = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let hits2 = hits.clone();
        let upstream = spawn_raw_upstream(
            move |_req| {
                hits2.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                // never answer: connection stays open until client timeout
                Vec::new()
            },
            // hold the connection open far beyond the 1s header timeout
            60_000,
        )
        .await;
        let state = state_with_upstream(upstream);
        {
            let mut cfg = state.config.write().expect("cfg lock");
            cfg.proxy_pool.response_header_timeout_secs = 1;
        }
        let app = build_router(state.clone());
        let gw = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = gw.local_addr().expect("addr");
        tokio::spawn(async move {
            axum::serve(gw, app).await.expect("serve");
        });
        let started = std::time::Instant::now();
        let resp = post_inference(addr).await;
        assert_eq!(resp.status(), StatusCode::GATEWAY_TIMEOUT);
        // no retry: single upstream hit even though the pool has 1 proxy
        assert_eq!(hits.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert!(started.elapsed() < std::time::Duration::from_secs(9));
    }

    /// A 200 stream must carry the admission permit until the body ends;
    /// while the upstream body is mid-flight the permit is held, and once
    /// the stream terminates (EOF or error) it is released.
    #[tokio::test(flavor = "multi_thread")]
    async fn permit_held_during_success_stream_and_released_at_eof() {
        let _serial = crate::router::TEST_UPSTREAM_SERIALIZE.lock().await;
        // Upstream declares content-length 100 but sends only 5 bytes and
        // holds the connection, so the gateway stream is observably open.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let upstream = listener.local_addr().expect("addr");
        tokio::spawn(async move {
            loop {
                let Ok((mut sock, _)) = listener.accept().await else {
                    break;
                };
                tokio::spawn(async move {
                    use tokio::io::AsyncReadExt;
                    let mut buf = vec![0u8; 65536];
                    loop {
                        match tokio::time::timeout(
                            std::time::Duration::from_millis(300),
                            sock.read(&mut buf),
                        )
                        .await
                        {
                            Ok(Ok(0)) | Err(_) | Ok(Err(_)) => break,
                            Ok(Ok(_)) => {}
                        }
                        if buf.len() > 2 * 1024 * 1024 {
                            break;
                        }
                    }
                    use tokio::io::AsyncWriteExt;
                    let head = "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: 100\r\n\r\n";
                    let _ = sock.write_all(head.as_bytes()).await;
                    let _ = sock.write_all(b"{\"ok\"").await;
                    let _ = sock.flush().await;
                    // hold the stream open until the test releases it
                    tokio::time::sleep(std::time::Duration::from_secs(30)).await;
                });
            }
        });
        let state = state_with_upstream_capacity(upstream, 1);
        let app = build_router(state.clone());
        let gw = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = gw.local_addr().expect("addr");
        tokio::spawn(async move {
            axum::serve(gw, app).await.expect("serve");
        });
        let resp = post_inference(addr).await;
        assert_eq!(resp.status(), StatusCode::OK);
        // headers received while stream still open: permit must be held.
        // The fake upstream holds the connection 2s after replying, but the
        // 11-byte body may already have been flushed end-to-end before we
        // observe, so poll briefly for active()==1 and only fail if it is
        // never observed (race-free enough without sleeps in the common path).
        assert_eq!(
            state.admission.active(),
            1,
            "permit must be held while the upstream body is mid-flight"
        );
        // End the upstream abruptly: the gateway stream must error out and
        // the permit must be released (never leaked).
        // (The fake upstream holds for 30s; dropping the client aborts the
        // gateway connection, and axum drops the stream, releasing the permit.)
        drop(resp);
        for _ in 0..100 {
            if state.admission.active() == 0 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert_eq!(
            state.admission.active(),
            0,
            "permit must be released when the stream ends"
        );
    }

    /// Duplicate response headers (set-cookie) must all reach the client.
    #[tokio::test(flavor = "multi_thread")]
    async fn duplicate_response_headers_are_preserved() {
        let _serial = crate::router::TEST_UPSTREAM_SERIALIZE.lock().await;
        let upstream = spawn_raw_upstream(
            move |_req| {
                let body = b"{\"ok\":true}";
                let mut resp = format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\nset-cookie: a=1\r\nset-cookie: b=2\r\ncontent-length: {}\r\n\r\n",
                    body.len()
                )
                .into_bytes();
                resp.extend_from_slice(body);
                resp
            },
            2_000,
        )
        .await;
        let state = state_with_upstream(upstream);
        let app = build_router(state.clone());
        let gw = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = gw.local_addr().expect("addr");
        tokio::spawn(async move {
            axum::serve(gw, app).await.expect("serve");
        });
        let resp = post_inference(addr).await;
        assert_eq!(resp.status(), StatusCode::OK);
        let cookies = resp
            .headers()
            .get_all("set-cookie")
            .iter()
            .filter_map(|v| v.to_str().ok())
            .collect::<Vec<_>>();
        assert!(
            cookies.contains(&"a=1") && cookies.contains(&"b=2"),
            "both set-cookie values must be forwarded, got {cookies:?}"
        );
    }

    /// The classification deadline (not only content-length) must bound a
    /// slow 404 body: an upstream that sends a non-model 404 prefix then
    /// stalls must terminate via CLASSIFY_DEADLINE / idle timeout and the
    /// breaker must stay closed.
    #[tokio::test(flavor = "multi_thread")]
    async fn classification_deadline_bounds_slow_404_body() {
        let _serial = crate::router::TEST_UPSTREAM_SERIALIZE.lock().await;
        use tokio::io::AsyncWriteExt;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let upstream = listener.local_addr().expect("addr");
        tokio::spawn(async move {
            loop {
                let Ok((mut sock, _)) = listener.accept().await else {
                    break;
                };
                tokio::spawn(async move {
                    let mut buf = [0u8; 8192];
                    {
                        use tokio::io::AsyncReadExt;
                        let _ = tokio::time::timeout(
                            std::time::Duration::from_secs(5),
                            sock.read(&mut buf),
                        )
                        .await;
                    }
                    // claim a large body, send only a chunk, then stall forever
                    let head = "HTTP/1.1 404 Not Found\r\ncontent-type: text/plain\r\ncontent-length: 1000000\r\n\r\n";
                    let _ = sock.write_all(head.as_bytes()).await;
                    let _ = sock.write_all(b"nothing to see here").await;
                    let _ = sock.flush().await;
                    // hold the connection open without ever finishing
                    tokio::time::sleep(std::time::Duration::from_secs(120)).await;
                });
            }
        });
        let state = state_with_upstream(upstream);
        {
            let mut cfg = state.config.write().expect("cfg lock");
            cfg.proxy_pool.stream_idle_timeout_secs = 1;
        }
        // Shrink the classification deadline so the deadline path is
        // observable in test time instead of 30s.
        *CLASSIFY_DEADLINE_OVERRIDE.lock().expect("lock") = Some(1_000);
        let app = build_router(state.clone());
        let gw = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = gw.local_addr().expect("addr");
        tokio::spawn(async move {
            axum::serve(gw, app).await.expect("serve");
        });
        let started = std::time::Instant::now();
        let resp = post_inference(addr).await;
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
        let body = tokio::time::timeout(std::time::Duration::from_secs(5), resp.bytes()).await;
        assert!(
            body.is_ok(),
            "slow 404 body must be bounded by deadline, not hang"
        );
        assert!(
            started.elapsed() < std::time::Duration::from_secs(9),
            "classification must not wait for the full body"
        );
        *CLASSIFY_DEADLINE_OVERRIDE.lock().expect("lock") = None;
        assert_eq!(
            state.circuit_breaker.model_state("m:free"),
            crate::circuit_breaker::BreakerState::Closed,
            "unclassifiable stalled body must not open the breaker"
        );
    }
}
