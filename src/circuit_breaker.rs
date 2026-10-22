//! Circuit breakers with three independent scopes.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use crate::config::CircuitBreakerConfig;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BreakerState {
    Closed,
    Open,
    HalfOpen,
}

impl BreakerState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Closed => "closed",
            Self::Open => "open",
            Self::HalfOpen => "half-open",
        }
    }
}

#[derive(Debug, Clone)]
struct Entry {
    state: BreakerState,
    opened_at: Option<Instant>,
    cooldown: Duration,
    half_open_inflight: usize,
}

impl Entry {
    fn new() -> Self {
        Self {
            state: BreakerState::Closed,
            opened_at: None,
            cooldown: Duration::ZERO,
            half_open_inflight: 0,
        }
    }
}

/// RAII lease for a transport half-open reservation. Cancels the
/// reservation (releasing the probe slot) when dropped, including when the
/// owning future is cancelled/dropped mid-flight.
pub struct TransportGuard {
    cb: Arc<CircuitBreaker>,
    proxy_id: String,
}

impl Drop for TransportGuard {
    fn drop(&mut self) {
        self.cb.transport_cancel(&self.proxy_id);
    }
}

impl TransportGuard {
    pub fn proxy_id(&self) -> &str {
        &self.proxy_id
    }
}

pub struct RateLimitGuard {
    cb: Arc<CircuitBreaker>,
    proxy_id: String,
    origin: String,
}

impl Drop for RateLimitGuard {
    fn drop(&mut self) {
        self.cb.rate_limit_cancel(&self.proxy_id, &self.origin);
    }
}

impl RateLimitGuard {
    pub fn proxy_id(&self) -> &str {
        &self.proxy_id
    }
    pub fn origin(&self) -> &str {
        &self.origin
    }
}

pub struct ModelGuard {
    cb: Arc<CircuitBreaker>,
    model_id: String,
}

impl Drop for ModelGuard {
    fn drop(&mut self) {
        self.cb.model_cancel(&self.model_id);
    }
}

impl ModelGuard {
    pub fn model_id(&self) -> &str {
        &self.model_id
    }
}

pub struct CircuitBreaker {
    transport: Mutex<HashMap<String, Entry>>,
    rate_limit: Mutex<HashMap<(String, String), Entry>>,
    model: Mutex<HashMap<String, Entry>>,
    config: Mutex<CircuitBreakerConfig>,
}

impl Default for CircuitBreaker {
    fn default() -> Self {
        Self::new(CircuitBreakerConfig {
            proxy_failure_cooldown_secs: 60,
            rate_limit_cooldown_secs: 120,
            model_not_found_cooldown_secs: 300,
            half_open_max_probes: 1,
        })
    }
}

impl CircuitBreaker {
    pub fn new(config: CircuitBreakerConfig) -> Self {
        Self {
            transport: Mutex::new(HashMap::new()),
            rate_limit: Mutex::new(HashMap::new()),
            model: Mutex::new(HashMap::new()),
            config: Mutex::new(config),
        }
    }

    pub fn update_config(&self, config: &CircuitBreakerConfig) {
        *self.config.lock().unwrap() = config.clone();
    }

    pub fn config(&self) -> CircuitBreakerConfig {
        self.config.lock().unwrap().clone()
    }

    fn prune_map<K: std::cmp::Eq + std::hash::Hash>(map: &mut HashMap<K, Entry>) {
        let now = Instant::now();
        map.retain(|_, e| match e.state {
            BreakerState::Closed => true,
            BreakerState::Open => e
                .opened_at
                .map(|o| {
                    now.duration_since(o) < e.cooldown.saturating_mul(2).max(Duration::from_secs(1))
                })
                .unwrap_or(false),
            BreakerState::HalfOpen => true,
        });
    }

    // transport
    pub fn transport_check(&self, proxy_id: &str) -> bool {
        let cfg = self.config();
        let mut map = self.transport.lock().unwrap();
        Self::prune_map(&mut map);
        let e = map.entry(proxy_id.to_string()).or_insert_with(Entry::new);
        check_entry(e, cfg.half_open_max_probes)
    }
    pub fn transport_allow(&self, proxy_id: &str) -> bool {
        self.transport_check(proxy_id)
    }
    pub fn transport_is_available(&self, proxy_id: &str) -> bool {
        self.transport_check(proxy_id)
    }

    pub fn transport_reserve(&self, proxy_id: &str) -> bool {
        let cfg = self.config();
        let mut map = self.transport.lock().unwrap();
        Self::prune_map(&mut map);
        let e = map.entry(proxy_id.to_string()).or_insert_with(Entry::new);
        reserve_entry(e, cfg.half_open_max_probes)
    }
    pub fn transport_try_acquire(&self, proxy_id: &str) -> bool {
        self.transport_reserve(proxy_id)
    }
    pub fn transport_acquire(&self, proxy_id: &str) -> bool {
        self.transport_reserve(proxy_id)
    }

    pub fn transport_cancel(&self, proxy_id: &str) {
        let mut map = self.transport.lock().unwrap();
        if let Some(e) = map.get_mut(proxy_id) {
            cancel_entry(e);
        }
    }
    pub fn transport_release(&self, proxy_id: &str) {
        self.transport_cancel(proxy_id);
    }
    pub fn transport_abort(&self, proxy_id: &str) {
        self.transport_cancel(proxy_id);
    }

    pub fn transport_record_success(&self, proxy_id: &str) {
        let mut map = self.transport.lock().unwrap();
        if let Some(e) = map.get_mut(proxy_id) {
            e.state = BreakerState::Closed;
            e.opened_at = None;
            e.cooldown = Duration::ZERO;
            e.half_open_inflight = 0;
        }
    }

    pub fn transport_record_failure(&self, proxy_id: &str) {
        let cfg = self.config();
        let cooldown = Duration::from_secs(cfg.proxy_failure_cooldown_secs);
        let mut map = self.transport.lock().unwrap();
        let e = map.entry(proxy_id.to_string()).or_insert_with(Entry::new);
        e.state = BreakerState::Open;
        e.opened_at = Some(Instant::now());
        e.cooldown = cooldown;
        e.half_open_inflight = 0;
    }

    // rate limit
    pub fn rate_limit_check(&self, proxy_id: &str, origin: &str) -> bool {
        let cfg = self.config();
        let mut map = self.rate_limit.lock().unwrap();
        Self::prune_map(&mut map);
        let e = map
            .entry((proxy_id.to_string(), origin.to_string()))
            .or_insert_with(Entry::new);
        check_entry(e, cfg.half_open_max_probes)
    }
    pub fn rate_limit_allow(&self, proxy_id: &str, origin: &str) -> bool {
        self.rate_limit_check(proxy_id, origin)
    }
    pub fn rate_limit_is_available(&self, proxy_id: &str, origin: &str) -> bool {
        self.rate_limit_check(proxy_id, origin)
    }

    pub fn rate_limit_reserve(&self, proxy_id: &str, origin: &str) -> bool {
        let cfg = self.config();
        let mut map = self.rate_limit.lock().unwrap();
        Self::prune_map(&mut map);
        let e = map
            .entry((proxy_id.to_string(), origin.to_string()))
            .or_insert_with(Entry::new);
        reserve_entry(e, cfg.half_open_max_probes)
    }
    pub fn rate_limit_try_acquire(&self, proxy_id: &str, origin: &str) -> bool {
        self.rate_limit_reserve(proxy_id, origin)
    }
    pub fn rate_limit_acquire(&self, proxy_id: &str, origin: &str) -> bool {
        self.rate_limit_reserve(proxy_id, origin)
    }

    pub fn rate_limit_cancel(&self, proxy_id: &str, origin: &str) {
        let mut map = self.rate_limit.lock().unwrap();
        if let Some(e) = map.get_mut(&(proxy_id.to_string(), origin.to_string())) {
            cancel_entry(e);
        }
    }
    pub fn rate_limit_release(&self, proxy_id: &str, origin: &str) {
        self.rate_limit_cancel(proxy_id, origin);
    }
    pub fn rate_limit_abort(&self, proxy_id: &str, origin: &str) {
        self.rate_limit_cancel(proxy_id, origin);
    }

    pub fn rate_limit_record_success(&self, proxy_id: &str, origin: &str) {
        let mut map = self.rate_limit.lock().unwrap();
        if let Some(e) = map.get_mut(&(proxy_id.to_string(), origin.to_string())) {
            e.state = BreakerState::Closed;
            e.opened_at = None;
            e.cooldown = Duration::ZERO;
            e.half_open_inflight = 0;
        }
    }

    pub fn rate_limit_record_failure(
        &self,
        proxy_id: &str,
        origin: &str,
        retry_after: Option<Duration>,
    ) {
        let cfg = self.config();
        let cooldown =
            retry_after.unwrap_or_else(|| Duration::from_secs(cfg.rate_limit_cooldown_secs));
        let mut map = self.rate_limit.lock().unwrap();
        let e = map
            .entry((proxy_id.to_string(), origin.to_string()))
            .or_insert_with(Entry::new);
        e.state = BreakerState::Open;
        e.opened_at = Some(Instant::now());
        e.cooldown = cooldown;
        e.half_open_inflight = 0;
    }

    // model
    pub fn model_check(&self, model_id: &str) -> bool {
        let cfg = self.config();
        let mut map = self.model.lock().unwrap();
        Self::prune_map(&mut map);
        let e = map.entry(model_id.to_string()).or_insert_with(Entry::new);
        check_entry(e, cfg.half_open_max_probes)
    }
    pub fn model_allow(&self, model_id: &str) -> bool {
        self.model_check(model_id)
    }
    pub fn model_is_available(&self, model_id: &str) -> bool {
        self.model_check(model_id)
    }

    pub fn model_reserve(&self, model_id: &str) -> bool {
        let cfg = self.config();
        let mut map = self.model.lock().unwrap();
        Self::prune_map(&mut map);
        let e = map.entry(model_id.to_string()).or_insert_with(Entry::new);
        reserve_entry(e, cfg.half_open_max_probes)
    }
    pub fn model_try_acquire(&self, model_id: &str) -> bool {
        self.model_reserve(model_id)
    }
    pub fn model_acquire(&self, model_id: &str) -> bool {
        self.model_reserve(model_id)
    }

    pub fn model_cancel(&self, model_id: &str) {
        let mut map = self.model.lock().unwrap();
        if let Some(e) = map.get_mut(model_id) {
            cancel_entry(e);
        }
    }
    pub fn model_release(&self, model_id: &str) {
        self.model_cancel(model_id);
    }
    pub fn model_abort(&self, model_id: &str) {
        self.model_cancel(model_id);
    }

    pub fn model_record_success(&self, model_id: &str) {
        let mut map = self.model.lock().unwrap();
        if let Some(e) = map.get_mut(model_id) {
            e.state = BreakerState::Closed;
            e.opened_at = None;
            e.cooldown = Duration::ZERO;
            e.half_open_inflight = 0;
        }
    }

    pub fn model_record_failure(&self, model_id: &str) {
        let cfg = self.config();
        let cooldown = Duration::from_secs(cfg.model_not_found_cooldown_secs);
        let mut map = self.model.lock().unwrap();
        let e = map.entry(model_id.to_string()).or_insert_with(Entry::new);
        e.state = BreakerState::Open;
        e.opened_at = Some(Instant::now());
        e.cooldown = cooldown;
        e.half_open_inflight = 0;
    }

    // RAII guards (leak-safe on cancellation/drop).
    pub fn transport_reserve_guard(self: &Arc<Self>, proxy_id: &str) -> Option<TransportGuard> {
        if self.transport_reserve(proxy_id) {
            Some(TransportGuard {
                cb: self.clone(),
                proxy_id: proxy_id.to_string(),
            })
        } else {
            None
        }
    }
    pub fn transport_lease(self: &Arc<Self>, proxy_id: &str) -> Option<TransportGuard> {
        self.transport_reserve_guard(proxy_id)
    }
    pub fn transport_try_acquire_guard(self: &Arc<Self>, proxy_id: &str) -> Option<TransportGuard> {
        self.transport_reserve_guard(proxy_id)
    }
    pub fn transport_acquire_guard(self: &Arc<Self>, proxy_id: &str) -> Option<TransportGuard> {
        self.transport_reserve_guard(proxy_id)
    }

    pub fn rate_limit_reserve_guard(
        self: &Arc<Self>,
        proxy_id: &str,
        origin: &str,
    ) -> Option<RateLimitGuard> {
        if self.rate_limit_reserve(proxy_id, origin) {
            Some(RateLimitGuard {
                cb: self.clone(),
                proxy_id: proxy_id.to_string(),
                origin: origin.to_string(),
            })
        } else {
            None
        }
    }
    pub fn rate_limit_lease(
        self: &Arc<Self>,
        proxy_id: &str,
        origin: &str,
    ) -> Option<RateLimitGuard> {
        self.rate_limit_reserve_guard(proxy_id, origin)
    }

    pub fn model_reserve_guard(self: &Arc<Self>, model_id: &str) -> Option<ModelGuard> {
        if self.model_reserve(model_id) {
            Some(ModelGuard {
                cb: self.clone(),
                model_id: model_id.to_string(),
            })
        } else {
            None
        }
    }
    pub fn model_lease(self: &Arc<Self>, model_id: &str) -> Option<ModelGuard> {
        self.model_reserve_guard(model_id)
    }

    // helpers
    pub fn transport_state(&self, proxy_id: &str) -> BreakerState {
        self.transport
            .lock()
            .unwrap()
            .get(proxy_id)
            .map(|e| e.state)
            .unwrap_or(BreakerState::Closed)
    }
    pub fn model_state(&self, model_id: &str) -> BreakerState {
        self.model
            .lock()
            .unwrap()
            .get(model_id)
            .map(|e| e.state)
            .unwrap_or(BreakerState::Closed)
    }
    pub fn rate_limit_state(&self, proxy_id: &str, origin: &str) -> BreakerState {
        self.rate_limit
            .lock()
            .unwrap()
            .get(&(proxy_id.to_string(), origin.to_string()))
            .map(|e| e.state)
            .unwrap_or(BreakerState::Closed)
    }
    pub fn half_open_inflight_transport(&self, proxy_id: &str) -> usize {
        self.transport
            .lock()
            .unwrap()
            .get(proxy_id)
            .map(|e| e.half_open_inflight)
            .unwrap_or(0)
    }
}

fn check_entry(e: &mut Entry, half_open_max: usize) -> bool {
    match e.state {
        BreakerState::Closed => true,
        BreakerState::Open => {
            if let Some(opened) = e.opened_at
                && opened.elapsed() >= e.cooldown
            {
                true
            } else {
                false
            }
        }
        BreakerState::HalfOpen => e.half_open_inflight < half_open_max,
    }
}

fn reserve_entry(e: &mut Entry, half_open_max: usize) -> bool {
    match e.state {
        BreakerState::Closed => true,
        BreakerState::Open => {
            if let Some(opened) = e.opened_at
                && opened.elapsed() >= e.cooldown
            {
                e.state = BreakerState::HalfOpen;
                e.half_open_inflight = 0;
                if e.half_open_inflight < half_open_max {
                    e.half_open_inflight += 1;
                    return true;
                }
                false
            } else {
                false
            }
        }
        BreakerState::HalfOpen => {
            if e.half_open_inflight < half_open_max {
                e.half_open_inflight += 1;
                true
            } else {
                false
            }
        }
    }
}

fn cancel_entry(e: &mut Entry) {
    if e.state == BreakerState::HalfOpen && e.half_open_inflight > 0 {
        e.half_open_inflight -= 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::CircuitBreakerConfig;

    fn cfg() -> CircuitBreakerConfig {
        CircuitBreakerConfig {
            proxy_failure_cooldown_secs: 60,
            rate_limit_cooldown_secs: 120,
            model_not_found_cooldown_secs: 300,
            half_open_max_probes: 1,
        }
    }

    #[test]
    fn transport_quarantine() {
        let cb = CircuitBreaker::new(cfg());
        assert!(cb.transport_allow("p0"));
        cb.transport_record_failure("p0");
        assert!(!cb.transport_allow("p0"));
        assert_eq!(cb.transport_state("p0"), BreakerState::Open);
        cb.transport_record_success("p0");
        assert!(cb.transport_allow("p0"));
    }

    #[test]
    fn rate_limit_scoped() {
        let cb = CircuitBreaker::new(cfg());
        cb.rate_limit_record_failure("p0", "originA", None);
        assert!(!cb.rate_limit_allow("p0", "originA"));
        assert!(cb.rate_limit_allow("p0", "originB"));
        assert!(cb.rate_limit_allow("p1", "originA"));
    }

    #[test]
    fn model_scoped_all_proxies() {
        let cb = CircuitBreaker::new(cfg());
        cb.model_record_failure("m1");
        assert!(!cb.model_allow("m1"));
        assert!(cb.transport_allow("p0"));
    }

    #[test]
    fn retry_after_respected() {
        let cb = CircuitBreaker::new(cfg());
        cb.rate_limit_record_failure("p0", "o", Some(Duration::from_secs(10)));
        let map = cb.rate_limit.lock().unwrap();
        let e = map.get(&("p0".to_string(), "o".to_string())).unwrap();
        assert_eq!(e.cooldown, Duration::from_secs(10));
    }

    #[test]
    fn half_open_probe_limit() {
        let cb = CircuitBreaker::new(CircuitBreakerConfig {
            proxy_failure_cooldown_secs: 0,
            ..cfg()
        });
        cb.transport_record_failure("p0");
        std::thread::sleep(Duration::from_millis(5));
        assert!(cb.transport_reserve("p0"));
        assert!(!cb.transport_reserve("p0"));
        cb.transport_record_success("p0");
        assert!(cb.transport_allow("p0"));
    }

    #[test]
    fn half_open_time_based() {
        let cb = CircuitBreaker::new(CircuitBreakerConfig {
            proxy_failure_cooldown_secs: 0,
            half_open_max_probes: 1,
            ..cfg()
        });
        cb.transport_record_failure("p0");
        std::thread::sleep(Duration::from_millis(5));
        assert!(cb.transport_reserve("p0"));
        cb.transport_record_failure("p0");
        std::thread::sleep(Duration::from_millis(5));
        assert!(cb.transport_reserve("p0"));
    }

    #[test]
    fn cancel_releases_slot() {
        let cb = CircuitBreaker::new(CircuitBreakerConfig {
            proxy_failure_cooldown_secs: 0,
            half_open_max_probes: 1,
            ..cfg()
        });
        cb.transport_record_failure("p0");
        std::thread::sleep(Duration::from_millis(5));
        assert!(cb.transport_reserve("p0"));
        cb.transport_cancel("p0");
        cb.transport_record_failure("p0");
        std::thread::sleep(Duration::from_millis(5));
        assert!(cb.transport_reserve("p0"));
    }

    #[test]
    fn concurrency_one_probe() {
        use std::sync::Arc;
        let cb = Arc::new(CircuitBreaker::new(CircuitBreakerConfig {
            proxy_failure_cooldown_secs: 0,
            half_open_max_probes: 1,
            ..cfg()
        }));
        cb.transport_record_failure("p0");
        std::thread::sleep(Duration::from_millis(5));
        let cb2 = cb.clone();
        let h = std::thread::spawn(move || cb2.transport_reserve("p0"));
        let r1 = cb.transport_reserve("p0");
        let r2 = h.join().unwrap();
        assert!(!(r1 && r2));
    }
}
