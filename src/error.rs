//! Startup-time errors. All of them are fatal: the process prints them to
//! stderr and exits non-zero, panic-free, before serving traffic.

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde_json::json;

/// Fatal errors that prevent the process from starting.
#[derive(Debug, thiserror::Error)]
pub enum AppError {
    /// I/O failure while reading the configuration file.
    #[error("configuration file I/O failure: {0}")]
    Io(#[from] std::io::Error),
    /// `CONFIG_PATH` missing/unreadable, TOML parse failure, or validation
    /// failure (e.g. the required `[proxy_pool]` / `proxies` key is missing).
    #[error("configuration error: {0}")]
    Config(String),
    /// `PORT` missing or not a valid port.
    #[error("{0}")]
    Port(String),
    /// An at-runtime issue with building or joining an upstream request.
    #[error("upstream request error: {0}")]
    Upstream(String),
}

/// A minor request-scoped error rendered as `{"error": "..."}` for responses
/// (currently only the JSON 404 fallback).
#[derive(Debug)]
pub struct ApiError {
    pub status: StatusCode,
    pub message: String,
}

impl ApiError {
    pub fn not_found(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            message: message.into(),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.status, axum::Json(json!({ "error": self.message }))).into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn api_error_renders_json_not_found() {
        let response = ApiError::not_found("no route").into_response();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        assert_eq!(
            response
                .headers()
                .get(axum::http::header::CONTENT_TYPE)
                .unwrap(),
            "application/json"
        );
    }
}
