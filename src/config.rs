//! Single TOML configuration file schema.

use std::time::Duration;

use serde::Deserialize;

use crate::error::AppError;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub proxy_pool: ProxyPoolConfig,
    pub models: ModelsConfig,
    pub circuit_breaker: CircuitBreakerConfig,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProxyPoolConfig {
    pub reload_interval_secs: u64,
    pub connect_timeout_secs: u64,
    pub proxies: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelsConfig {
    pub refresh_interval_secs: u64,
    pub stale_ttl_secs: u64,
    pub max_entries: usize,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CircuitBreakerConfig {
    pub proxy_failure_cooldown_secs: u64,
    pub rate_limit_cooldown_secs: u64,
    pub model_not_found_cooldown_secs: u64,
    pub half_open_max_probes: usize,
}

impl Config {
    pub fn from_toml(content: &str) -> Result<Self, AppError> {
        let config: Self = toml::from_str(content)
            .map_err(|err| AppError::Config(format!("failed to parse configuration: {err}")))?;
        config.validate()?;
        Ok(config)
    }

    pub fn load_from_path(path: impl AsRef<std::path::Path>) -> Result<Self, AppError> {
        let path = path.as_ref();
        let content = std::fs::read_to_string(path).map_err(|err| {
            AppError::Config(format!(
                "failed to read the configuration file (CONFIG_PATH) at {path:?} (is it readable?): {err}"
            ))
        })?;
        Self::from_toml(&content)
    }

    fn validate(&self) -> Result<(), AppError> {
        if self.proxy_pool.reload_interval_secs == 0 {
            return Err(AppError::Config(
                "proxy_pool.reload_interval_secs must be >= 1".into(),
            ));
        }
        if self.proxy_pool.connect_timeout_secs == 0 {
            return Err(AppError::Config(
                "proxy_pool.connect_timeout_secs must be >= 1".into(),
            ));
        }
        for p in &self.proxy_pool.proxies {
            if p.trim().is_empty() {
                return Err(AppError::Config(
                    "proxy_pool.proxies contains an empty entry".into(),
                ));
            }
        }
        if self.models.refresh_interval_secs == 0 {
            return Err(AppError::Config(
                "models.refresh_interval_secs must be >= 1".into(),
            ));
        }
        if self.models.stale_ttl_secs == 0 {
            return Err(AppError::Config(
                "models.stale_ttl_secs must be >= 1".into(),
            ));
        }
        if self.models.max_entries == 0 {
            return Err(AppError::Config("models.max_entries must be >= 1".into()));
        }
        if self.circuit_breaker.proxy_failure_cooldown_secs == 0 {
            return Err(AppError::Config(
                "circuit_breaker.proxy_failure_cooldown_secs must be >= 1".into(),
            ));
        }
        if self.circuit_breaker.rate_limit_cooldown_secs == 0 {
            return Err(AppError::Config(
                "circuit_breaker.rate_limit_cooldown_secs must be >= 1".into(),
            ));
        }
        if self.circuit_breaker.model_not_found_cooldown_secs == 0 {
            return Err(AppError::Config(
                "circuit_breaker.model_not_found_cooldown_secs must be >= 1".into(),
            ));
        }
        if self.circuit_breaker.half_open_max_probes == 0 {
            return Err(AppError::Config(
                "circuit_breaker.half_open_max_probes must be >= 1".into(),
            ));
        }
        Ok(())
    }
}

impl ProxyPoolConfig {
    pub fn connect_timeout(&self) -> Duration {
        Duration::from_secs(self.connect_timeout_secs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn full_config_parses() {
        let toml = r#"
[proxy_pool]
reload_interval_secs = 1
connect_timeout_secs = 10
proxies = ["http://proxy-a.example.internal:8080"]

[models]
refresh_interval_secs = 300
stale_ttl_secs = 900
max_entries = 2048

[circuit_breaker]
proxy_failure_cooldown_secs = 60
rate_limit_cooldown_secs = 120
model_not_found_cooldown_secs = 300
half_open_max_probes = 1
"#;
        let c = Config::from_toml(toml).unwrap();
        assert_eq!(c.proxy_pool.proxies.len(), 1);
    }

    #[test]
    fn missing_models_rejected() {
        let err = Config::from_toml(
            "[proxy_pool]\nreload_interval_secs=1\nconnect_timeout_secs=1\nproxies=[]\n",
        )
        .unwrap_err();
        assert!(err.to_string().contains("models"));
    }

    #[test]
    fn unknown_field_rejected() {
        let toml = r#"
[proxy_pool]
reload_interval_secs = 1
connect_timeout_secs = 1
proxies = []
bogus = 1

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
        let err = Config::from_toml(toml).unwrap_err();
        assert!(err.to_string().contains("unknown field"));
    }

    #[test]
    fn zero_timeout_rejected() {
        let toml = r#"
[proxy_pool]
reload_interval_secs = 1
connect_timeout_secs = 0
proxies = ["http://x:8080"]

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
        let err = Config::from_toml(toml).unwrap_err();
        assert!(err.to_string().contains("connect_timeout_secs"));
    }
}
