//! `kilo-provider` — an anonymous HTTP provider gateway in front of the Kilo
//! upstream API.

#![forbid(unsafe_code)]

pub mod catalog;
pub mod circuit_breaker;
pub mod config;
pub mod error;
pub mod proxy_pool;
pub mod router;
pub mod upstream;
