//! Outbound HTTP client for the Kilo upstream API.
//!
//! The upstream location is a FIXED CODE CONSTANT
//! (`https://api.kilo.ai/api/gateway`) — never configurable via the config
//! file. Upstream access is ANONYMOUS: no `Authorization` header (or any
//! other credential) is attached anywhere, and no system/env proxy settings
//! are honored.
//!
//! Intentional properties of the HTTP client:
//! * `.no_proxy()` — never discovers HTTP(S) proxies from the environment so
//!   anonymous traffic cannot leak through a system proxy; explicit outbound
//!   proxy routing will come from the `[proxy_pool]` in a follow-up PR.
//! * NO `.timeout()` / `.read_timeout()` / `.connect_timeout()` and no
//!   deadline middleware in the axum/tokio/reqwest stack — the bootstrap
//!   scaffold bakes in no hidden timeouts.
//! * Not called at startup; `send_get` is exercised only by tests / later
//!   PRs.

use reqwest::Url;

use crate::error::AppError;

/// The single, fixed upstream base URL for the Kilo gateway.
pub const UPSTREAM_BASE_URL: &str = "https://api.kilo.ai/api/gateway";

/// Factory for upstream requests: one shared HTTP client plus the fixed
/// base URL.
#[derive(Debug, Clone)]
pub struct UpstreamClient {
    base_url: Url,
    http: reqwest::Client,
}

impl UpstreamClient {
    /// Build a client rooted at `base_url` (the constant
    /// [`UPSTREAM_BASE_URL`] in production; a local URL in tests).
    pub fn new(base_url: Url) -> Self {
        let http = reqwest::Client::builder()
            // Anonymous + no proxy leakage: never consult system/env proxy
            // settings. Explicit [proxy_pool] routing is a follow-up concern.
            .no_proxy()
            // SECURITY: no `.default_headers(...)`, no Authorization helpers
            // here or anywhere else in this crate — requests are anonymous.
            // TIME: no `.timeout(...)`, `.read_timeout(...)`, or
            // `.connect_timeout(...)` on purpose (see module docs).
            .build()
            .expect("reqwest client with a plain builder configuration must build");
        Self { base_url, http }
    }

    /// The fixed base URL this client points at.
    pub fn base_url(&self) -> &Url {
        &self.base_url
    }

    /// Build (but do not send) an anonymous GET against `base_url + path`
    /// using RFC 3986 URL join semantics.
    pub fn build_get(&self, path: &str) -> Result<reqwest::Request, AppError> {
        let url = self.base_url.join(path).map_err(|err| {
            AppError::Upstream(format!(
                "cannot join path {path:?} onto the upstream base URL: {err}"
            ))
        })?;
        // Deliberate: nothing attaches an Authorization header here or in any
        // caller — this file is the single place upstream requests are built.
        let request = self.http.get(url).build()?;
        Ok(request)
    }

    /// Placeholder upstream request sender (not invoked at startup).
    ///
    /// here eventually; intentionally unimplemented beyond this simple GET.
    pub async fn send_get(&self, path: &str) -> Result<reqwest::Response, AppError> {
        let request = self.build_get(path)?;
        let response = self.http.execute(request).await?;
        Ok(response)
    }
}

impl From<reqwest::Error> for AppError {
    fn from(err: reqwest::Error) -> Self {
        AppError::Upstream(err.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::{HeaderMap, StatusCode};
    use std::sync::{Arc, Mutex};
    use tokio::net::TcpListener;

    #[test]
    fn constant_base_url_is_the_fixed_kilo_gateway() {
        assert_eq!(UPSTREAM_BASE_URL, "https://api.kilo.ai/api/gateway");
    }

    #[test]
    fn built_request_has_no_authorization_header() {
        let client =
            UpstreamClient::new(Url::parse(UPSTREAM_BASE_URL).expect("constant url is valid"));
        let request = client.build_get("v1/models").expect("valid upstream path");
        assert!(
            !request
                .headers()
                .contains_key(reqwest::header::AUTHORIZATION),
            "outbound upstream requests must never carry an Authorization header"
        );
        // RFC 3986 join: a relative path replaces the BASE URL's last path
        // segment (`gateway`), which is exactly the documented behavior.
        assert_eq!(
            request.url(),
            &client
                .base_url()
                .join("v1/models")
                .expect("relative join is valid")
        );
        assert_eq!(*request.method(), reqwest::Method::GET);
    }

    /// A tiny local axum/std server that captures request headers, allowing
    /// us to assert on the wire that the upstream client sends NO
    /// Authorization header.
    async fn capture_server(path: &'static str) -> (String, Arc<Mutex<Option<HeaderMap>>>) {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("ephemeral bind in tests works");
        let addr = listener
            .local_addr()
            .expect("bound listener has an address");
        let captured: Arc<Mutex<Option<HeaderMap>>> = Arc::new(Mutex::new(None));
        let capture = Arc::clone(&captured);

        let app = axum::Router::new().route(
            path,
            axum::routing::get(move |request: axum::extract::Request| async move {
                *capture.lock().expect("capture lock not poisoned") =
                    Some(request.headers().clone());
                StatusCode::OK
            }),
        );
        tokio::spawn(async move {
            axum::serve(listener, app)
                .await
                .expect("test server must not fail");
        });
        (format!("http://{addr}"), captured)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn wire_request_carries_no_authorization_header() {
        let (base, captured) = capture_server("/upstream-probe").await;
        // Construct the client explicitly against the local capture server —
        // the real constant base URL is for the actual Kilo upstream.
        let client = UpstreamClient::new(Url::parse(&base).expect("local url is valid"));
        let response = client
            .send_get("/upstream-probe")
            .await
            .expect("local probe request must succeed");
        assert_eq!(response.status(), StatusCode::OK);

        let headers = captured
            .lock()
            .expect("capture lock not poisoned")
            .clone()
            .expect("capture server saw the request");
        assert!(
            !headers.contains_key(axum::http::header::AUTHORIZATION),
            "the on-the-wire request must never include an Authorization header, got: {headers:?}"
        );
    }

    #[test]
    fn invalid_join_target_is_rejected() {
        let client =
            UpstreamClient::new(Url::parse(UPSTREAM_BASE_URL).expect("constant url is valid"));
        let err = client
            .build_get("http://[::1")
            .expect_err("invalid join target must fail");
        assert!(err.to_string().contains("http://[::1"));
    }
}
