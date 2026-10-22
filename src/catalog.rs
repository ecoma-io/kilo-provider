//! Model catalog: fetch from upstream, filter free models, cache with stale TTL.

use std::collections::HashSet;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use serde_json::Value;

use crate::error::AppError;

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

    pub fn replace(&self, models: Vec<Value>) {
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
            return;
        }
        let snap = Arc::new(CatalogSnapshot {
            models: deduped,
            fetched_at: Instant::now(),
        });
        let mut st = self.state.write().unwrap();
        st.snapshot = Some(snap);
        st.last_error = None;
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

fn is_free_model(id: &str, meta: &Value) -> bool {
    // Verified aliases (must still satisfy pricing evidence below).
    let _ = id;
    let pricing = match meta.get("pricing") {
        Some(p) => p,
        None => return false, // no pricing metadata => fail closed
    };
    // pricing must be object non-empty
    let obj = match pricing.as_object() {
        Some(o) if !o.is_empty() => o,
        _ => return false,
    };
    // all pricing fields must parse to number and be exactly 0 (no negatives, no unparseable)
    for val in obj.values() {
        let n = match val {
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
    // if isFree present, must be true
    if let Some(is_free) = meta.get("isFree")
        && is_free != &Value::Bool(true)
    {
        return false;
    }
    true
}

pub async fn fetch_catalog(
    client: &reqwest::Client,
    base_url: &reqwest::Url,
    _proxy_url: Option<&str>,
) -> Result<Vec<Value>, AppError> {
    let mut url = base_url.clone();
    let base_path = url.path().trim_end_matches('/');
    url.set_path(&format!("{}/models", base_path));

    let resp = client
        .get(url.clone())
        .send()
        .await
        .map_err(|e| AppError::Upstream(format!("catalog request failed: {e}")))?;
    if !resp.status().is_success() {
        return Err(AppError::Upstream(format!(
            "catalog fetch failed: HTTP {}",
            resp.status()
        )));
    }
    // content-type check (optional but helpful)
    // read bytes
    let bytes = resp
        .bytes()
        .await
        .map_err(|e| AppError::Upstream(format!("catalog body read failed: {e}")))?;
    let v: Value = serde_json::from_slice(&bytes)
        .map_err(|e| AppError::Upstream(format!("catalog JSON parse failed: {e}")))?;
    let data = v
        .get("data")
        .and_then(|d| d.as_array())
        .ok_or_else(|| AppError::Upstream("catalog response missing 'data' array".to_string()))?;
    // validate each entry has string id
    for m in data {
        if m.get("id").and_then(|x| x.as_str()).is_none() {
            return Err(AppError::Upstream(
                "catalog entry missing string 'id'".to_string(),
            ));
        }
    }
    if data.is_empty() {
        return Err(AppError::Upstream(
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
        c.replace(vec![json!({"id":"x:free","pricing":{"prompt":"0"}})]);
        assert!(c.current().is_none());
        assert!(c.last_good().is_some());
    }

    #[test]
    fn ready_after_replace() {
        let c = ModelCatalog::new(10, Duration::from_secs(900), Duration::from_secs(300));
        assert!(!c.is_ready());
        c.replace(vec![json!({"id":"x:free","pricing":{"prompt":"0"}})]);
        assert!(c.is_ready());
    }

    #[test]
    fn empty_replace_does_not_overwrite() {
        let c = ModelCatalog::new(10, Duration::from_secs(900), Duration::from_secs(300));
        c.replace(vec![json!({"id":"x:free","pricing":{"prompt":"0"}})]);
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
            json!({"id":"a:free","pricing":{"prompt":"0"}}),
            json!({"id":"b:free","pricing":{"prompt":"0"}}),
            json!({"id":"c:free","pricing":{"prompt":"0"}}),
        ]);
        assert_eq!(c.len(), 2);
        c.update_config(Duration::from_secs(900), Duration::from_secs(300), 10);
        c.replace(vec![
            json!({"id":"a:free","pricing":{"prompt":"0"}}),
            json!({"id":"b:free","pricing":{"prompt":"0"}}),
            json!({"id":"c:free","pricing":{"prompt":"0"}}),
        ]);
        assert_eq!(c.len(), 3);
    }

    #[test]
    fn contains_model() {
        let c = ModelCatalog::new(10, Duration::from_secs(900), Duration::from_secs(300));
        c.replace(vec![json!({"id":"x:free","pricing":{"prompt":"0"}})]);
        assert!(c.contains_model("x:free"));
        assert!(!c.contains_model("nope"));
    }
}
