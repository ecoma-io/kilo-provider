//! Circuit breakers with three independent scopes.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
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
    generation: u64,
    active: HashSet<u64>,
    probes: HashSet<u64>,
    lease_gen: HashMap<u64, u64>,
    last_touched: Instant,
}

impl Entry {
    fn new() -> Self {
        Self {
            state: BreakerState::Closed,
            opened_at: None,
            cooldown: Duration::ZERO,
            generation: 0,
            active: HashSet::new(),
            probes: HashSet::new(),
            lease_gen: HashMap::new(),
            last_touched: Instant::now(),
        }
    }
}

static NEXT_LEASE: AtomicU64 = AtomicU64::new(1);

fn next_lease_id() -> u64 {
    NEXT_LEASE.fetch_add(1, Ordering::Relaxed)
}

pub struct TransportGuard {
    cb: Arc<CircuitBreaker>,
    proxy_id: String,
    lease_id: u64,
    pub armed: bool,
}

impl Drop for TransportGuard {
    fn drop(&mut self) {
        if self.armed {
            self.cb
                .transport_cancel_lease(&self.proxy_id, self.lease_id);
        }
    }
}

impl TransportGuard {
    pub fn proxy_id(&self) -> &str {
        &self.proxy_id
    }
    pub fn lease_id(&self) -> u64 {
        self.lease_id
    }
    pub fn record_success(&mut self) {
        if self.armed {
            let _ = self
                .cb
                .transport_record_success_lease(&self.proxy_id, self.lease_id);
            self.armed = false;
        }
    }
    pub fn record_failure(&mut self) {
        if self.armed {
            let _ = self
                .cb
                .transport_record_failure_lease(&self.proxy_id, self.lease_id);
            self.armed = false;
        }
    }
}

pub struct RateLimitGuard {
    cb: Arc<CircuitBreaker>,
    proxy_id: String,
    origin: String,
    lease_id: u64,
    pub armed: bool,
}

impl Drop for RateLimitGuard {
    fn drop(&mut self) {
        if self.armed {
            self.cb
                .rate_limit_cancel_lease(&self.proxy_id, &self.origin, self.lease_id);
        }
    }
}

impl RateLimitGuard {
    pub fn proxy_id(&self) -> &str {
        &self.proxy_id
    }
    pub fn origin(&self) -> &str {
        &self.origin
    }
    pub fn lease_id(&self) -> u64 {
        self.lease_id
    }
    pub fn record_success(&mut self) {
        if self.armed {
            let _ = self.cb.rate_limit_record_success_lease(
                &self.proxy_id,
                &self.origin,
                self.lease_id,
            );
            self.armed = false;
        }
    }
    pub fn record_failure(&mut self, retry_after: Option<Duration>) {
        if self.armed {
            let _ = self.cb.rate_limit_record_failure_lease(
                &self.proxy_id,
                &self.origin,
                self.lease_id,
                retry_after,
            );
            self.armed = false;
        }
    }
}

pub struct ModelGuard {
    cb: Arc<CircuitBreaker>,
    model_id: String,
    lease_id: u64,
    pub armed: bool,
}

impl Drop for ModelGuard {
    fn drop(&mut self) {
        if self.armed {
            self.cb.model_cancel_lease(&self.model_id, self.lease_id);
        }
    }
}

impl ModelGuard {
    pub fn model_id(&self) -> &str {
        &self.model_id
    }
    pub fn lease_id(&self) -> u64 {
        self.lease_id
    }
    pub fn record_success(&mut self) {
        if self.armed {
            let _ = self
                .cb
                .model_record_success_lease(&self.model_id, self.lease_id);
            self.armed = false;
        }
    }
    pub fn record_failure(&mut self) {
        if self.armed {
            let _ = self
                .cb
                .model_record_failure_lease(&self.model_id, self.lease_id);
            self.armed = false;
        }
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
            retry_after_max_secs: 300,
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
            BreakerState::Closed => {
                !e.active.is_empty()
                    || now.duration_since(e.last_touched) <= Duration::from_secs(300)
            }
            BreakerState::Open => true,
            BreakerState::HalfOpen => true,
        });
    }

    fn cfg(&self) -> CircuitBreakerConfig {
        self.config()
    }

    // transport
    pub fn transport_check(&self, proxy_id: &str) -> bool {
        let cfg = self.cfg();
        let mut map = self.transport.lock().unwrap();
        Self::prune_map(&mut map);
        let e = map.entry(proxy_id.to_string()).or_insert_with(Entry::new);
        e.last_touched = Instant::now();
        check_entry(e, cfg.half_open_max_probes)
    }
    pub fn transport_allow(&self, proxy_id: &str) -> bool {
        self.transport_check(proxy_id)
    }
    pub fn transport_is_available(&self, proxy_id: &str) -> bool {
        self.transport_check(proxy_id)
    }

    pub fn transport_reserve(&self, proxy_id: &str) -> bool {
        self.transport_reserve_lease(proxy_id).is_some()
    }
    pub fn transport_reserve_lease(&self, proxy_id: &str) -> Option<u64> {
        let cfg = self.cfg();
        let lease_id = next_lease_id();
        let mut map = self.transport.lock().unwrap();
        Self::prune_map(&mut map);
        let e = map.entry(proxy_id.to_string()).or_insert_with(Entry::new);
        e.last_touched = Instant::now();
        if reserve_entry(e, cfg.half_open_max_probes, lease_id) {
            Some(lease_id)
        } else {
            None
        }
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
            e.active.clear();
            e.probes.clear();
            e.lease_gen.clear();
            e.last_touched = Instant::now();
        }
    }
    pub fn transport_cancel_lease(&self, proxy_id: &str, lease_id: u64) {
        let mut map = self.transport.lock().unwrap();
        if let Some(e) = map.get_mut(proxy_id) {
            // only cancel if this lease owns a reservation in current lease_generation
            if let Some(lease_generation) = e.lease_gen.get(&lease_id).copied()
                && lease_generation == e.generation
            {
                e.active.remove(&lease_id);
                e.probes.remove(&lease_id);
                e.lease_gen.remove(&lease_id);
            }
            e.last_touched = Instant::now();
        }
    }
    pub fn transport_release(&self, proxy_id: &str) {
        self.transport_cancel(proxy_id);
    }
    pub fn transport_abort(&self, proxy_id: &str) {
        self.transport_cancel(proxy_id);
    }

    /// Legacy unconditioned success: closes the circuit regardless of any
    /// generation/lease context. Test-only callers; production paths must use
    /// the lease-aware API (`TransportGuard::record_success`).
    #[cfg(any(test, doc))]
    pub fn transport_record_success(&self, proxy_id: &str) {
        let mut map = self.transport.lock().unwrap();
        Self::prune_map(&mut map);
        if let Some(e) = map.get_mut(proxy_id) {
            e.state = BreakerState::Closed;
            e.opened_at = None;
            e.cooldown = Duration::ZERO;
            e.active.clear();
            e.probes.clear();
            e.lease_gen.clear();
            e.generation = e.generation.wrapping_add(1);
            e.last_touched = Instant::now();
        }
    }
    pub fn transport_record_success_lease(&self, proxy_id: &str, lease_id: u64) -> bool {
        let mut map = self.transport.lock().unwrap();
        Self::prune_map(&mut map);
        if let Some(e) = map.get_mut(proxy_id) {
            e.last_touched = Instant::now();
            let lease_generation = match e.lease_gen.get(&lease_id).copied() {
                Some(g) => g,
                None => return false,
            };
            if lease_generation != e.generation {
                return false;
            }
            if !e.active.remove(&lease_id) {
                return false;
            }
            let was_probe = e.probes.remove(&lease_id);
            e.lease_gen.remove(&lease_id);
            if e.state == BreakerState::HalfOpen && was_probe {
                // close circuit; new generation invalidates in-flight leases
                e.state = BreakerState::Closed;
                e.opened_at = None;
                e.cooldown = Duration::ZERO;
                e.active.clear();
                e.probes.clear();
                e.lease_gen.clear();
                e.generation = e.generation.wrapping_add(1);
            }
            true
        } else {
            false
        }
    }

    /// Legacy unconditioned failure: opens the circuit and invalidates every
    /// in-flight lease. Test-only callers; production paths must use
    /// `TransportGuard::record_failure`.
    #[cfg(any(test, doc))]
    pub fn transport_record_failure(&self, proxy_id: &str) {
        let cfg = self.cfg();
        let cooldown = Duration::from_secs(cfg.proxy_failure_cooldown_secs);
        let mut map = self.transport.lock().unwrap();
        Self::prune_map(&mut map);
        let e = map.entry(proxy_id.to_string()).or_insert_with(Entry::new);
        e.state = BreakerState::Open;
        e.opened_at = Some(Instant::now());
        e.cooldown = cooldown;
        e.active.clear();
        e.probes.clear();
        e.lease_gen.clear();
        e.generation = e.generation.wrapping_add(1);
        e.last_touched = Instant::now();
    }
    pub fn transport_record_failure_lease(&self, proxy_id: &str, lease_id: u64) -> bool {
        let cfg = self.cfg();
        let cooldown = Duration::from_secs(cfg.proxy_failure_cooldown_secs);
        let mut map = self.transport.lock().unwrap();
        Self::prune_map(&mut map);
        if let Some(e) = map.get_mut(proxy_id) {
            e.last_touched = Instant::now();
            let lease_generation = match e.lease_gen.get(&lease_id).copied() {
                Some(g) => g,
                None => return false,
            };
            if lease_generation != e.generation {
                return false;
            }
            if !e.active.remove(&lease_id) {
                return false;
            }
            let was_probe = e.probes.remove(&lease_id);
            e.lease_gen.remove(&lease_id);
            // open circuit on failure (probe or normal closed request)
            if e.state == BreakerState::Closed || (e.state == BreakerState::HalfOpen && was_probe) {
                e.state = BreakerState::Open;
                e.opened_at = Some(Instant::now());
                e.cooldown = cooldown;
                e.active.clear();
                e.probes.clear();
                e.lease_gen.clear();
                e.generation = e.generation.wrapping_add(1);
                return true;
            }
            false
        } else {
            false
        }
    }

    // outcome helpers (lease-aware, no unconditional overwrite)
    pub fn transport_record_failure_outcome(&self, proxy_id: &str, lease_id: u64) -> bool {
        if lease_id != 0 {
            return self.transport_record_failure_lease(proxy_id, lease_id);
        }
        false
    }
    pub fn transport_record_success_outcome(&self, proxy_id: &str, lease_id: u64) -> bool {
        if lease_id != 0 {
            return self.transport_record_success_lease(proxy_id, lease_id);
        }
        false
    }

    // rate limit
    pub fn rate_limit_check(&self, proxy_id: &str, origin: &str) -> bool {
        let cfg = self.cfg();
        let mut map = self.rate_limit.lock().unwrap();
        Self::prune_map(&mut map);
        let e = map
            .entry((proxy_id.to_string(), origin.to_string()))
            .or_insert_with(Entry::new);
        e.last_touched = Instant::now();
        check_entry(e, cfg.half_open_max_probes)
    }
    pub fn rate_limit_allow(&self, proxy_id: &str, origin: &str) -> bool {
        self.rate_limit_check(proxy_id, origin)
    }
    pub fn rate_limit_is_available(&self, proxy_id: &str, origin: &str) -> bool {
        self.rate_limit_check(proxy_id, origin)
    }

    pub fn rate_limit_reserve(&self, proxy_id: &str, origin: &str) -> bool {
        self.rate_limit_reserve_lease(proxy_id, origin).is_some()
    }
    pub fn rate_limit_reserve_lease(&self, proxy_id: &str, origin: &str) -> Option<u64> {
        let cfg = self.cfg();
        let lease_id = next_lease_id();
        let mut map = self.rate_limit.lock().unwrap();
        Self::prune_map(&mut map);
        let e = map
            .entry((proxy_id.to_string(), origin.to_string()))
            .or_insert_with(Entry::new);
        e.last_touched = Instant::now();
        if reserve_entry(e, cfg.half_open_max_probes, lease_id) {
            Some(lease_id)
        } else {
            None
        }
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
            e.active.clear();
            e.probes.clear();
            e.lease_gen.clear();
            e.last_touched = Instant::now();
        }
    }
    pub fn rate_limit_cancel_lease(&self, proxy_id: &str, origin: &str, lease_id: u64) {
        let mut map = self.rate_limit.lock().unwrap();
        if let Some(e) = map.get_mut(&(proxy_id.to_string(), origin.to_string())) {
            if let Some(lease_generation) = e.lease_gen.get(&lease_id).copied()
                && lease_generation == e.generation
            {
                e.active.remove(&lease_id);
                e.probes.remove(&lease_id);
                e.lease_gen.remove(&lease_id);
            }
            e.last_touched = Instant::now();
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
        Self::prune_map(&mut map);
        if let Some(e) = map.get_mut(&(proxy_id.to_string(), origin.to_string())) {
            e.state = BreakerState::Closed;
            e.opened_at = None;
            e.cooldown = Duration::ZERO;
            e.active.clear();
            e.probes.clear();
            e.lease_gen.clear();
            e.generation = e.generation.wrapping_add(1);
            e.last_touched = Instant::now();
        }
    }
    pub fn rate_limit_record_success_lease(
        &self,
        proxy_id: &str,
        origin: &str,
        lease_id: u64,
    ) -> bool {
        let mut map = self.rate_limit.lock().unwrap();
        Self::prune_map(&mut map);
        if let Some(e) = map.get_mut(&(proxy_id.to_string(), origin.to_string())) {
            e.last_touched = Instant::now();
            let lease_generation = match e.lease_gen.get(&lease_id).copied() {
                Some(g) => g,
                None => return false,
            };
            if lease_generation != e.generation {
                return false;
            }
            if !e.active.remove(&lease_id) {
                return false;
            }
            let was_probe = e.probes.remove(&lease_id);
            e.lease_gen.remove(&lease_id);
            if e.state == BreakerState::HalfOpen && was_probe {
                e.state = BreakerState::Closed;
                e.opened_at = None;
                e.cooldown = Duration::ZERO;
                e.active.clear();
                e.probes.clear();
                e.lease_gen.clear();
                e.generation = e.generation.wrapping_add(1);
            }
            true
        } else {
            false
        }
    }

    pub fn rate_limit_record_failure(
        &self,
        proxy_id: &str,
        origin: &str,
        retry_after: Option<Duration>,
    ) {
        let cfg = self.cfg();
        let mut cooldown =
            retry_after.unwrap_or_else(|| Duration::from_secs(cfg.rate_limit_cooldown_secs));
        let max = Duration::from_secs(cfg.retry_after_max_secs.max(1));
        if cooldown > max {
            cooldown = max;
        }
        let mut map = self.rate_limit.lock().unwrap();
        Self::prune_map(&mut map);
        let e = map
            .entry((proxy_id.to_string(), origin.to_string()))
            .or_insert_with(Entry::new);
        e.state = BreakerState::Open;
        e.opened_at = Some(Instant::now());
        e.cooldown = cooldown;
        e.active.clear();
        e.probes.clear();
        e.lease_gen.clear();
        e.generation = e.generation.wrapping_add(1);
        e.last_touched = Instant::now();
    }
    pub fn rate_limit_record_failure_lease(
        &self,
        proxy_id: &str,
        origin: &str,
        lease_id: u64,
        retry_after: Option<Duration>,
    ) -> bool {
        let cfg = self.cfg();
        let mut cooldown =
            retry_after.unwrap_or_else(|| Duration::from_secs(cfg.rate_limit_cooldown_secs));
        let max = Duration::from_secs(cfg.retry_after_max_secs.max(1));
        if cooldown > max {
            cooldown = max;
        }
        let mut map = self.rate_limit.lock().unwrap();
        Self::prune_map(&mut map);
        if let Some(e) = map.get_mut(&(proxy_id.to_string(), origin.to_string())) {
            e.last_touched = Instant::now();
            let lease_generation = match e.lease_gen.get(&lease_id).copied() {
                Some(g) => g,
                None => return false,
            };
            if lease_generation != e.generation {
                return false;
            }
            if !e.active.remove(&lease_id) {
                return false;
            }
            let was_probe = e.probes.remove(&lease_id);
            e.lease_gen.remove(&lease_id);
            if e.state == BreakerState::Closed || (e.state == BreakerState::HalfOpen && was_probe) {
                e.state = BreakerState::Open;
                e.opened_at = Some(Instant::now());
                e.cooldown = cooldown;
                e.active.clear();
                e.probes.clear();
                e.lease_gen.clear();
                e.generation = e.generation.wrapping_add(1);
                return true;
            }
            false
        } else {
            false
        }
    }

    pub fn rate_limit_record_failure_outcome(
        &self,
        proxy_id: &str,
        origin: &str,
        lease_id: u64,
        retry_after: Option<Duration>,
    ) -> bool {
        if lease_id != 0 {
            return self.rate_limit_record_failure_lease(proxy_id, origin, lease_id, retry_after);
        }
        false
    }
    pub fn rate_limit_record_success_outcome(
        &self,
        proxy_id: &str,
        origin: &str,
        lease_id: u64,
    ) -> bool {
        if lease_id != 0 {
            return self.rate_limit_record_success_lease(proxy_id, origin, lease_id);
        }
        false
    }

    // model
    pub fn model_check(&self, model_id: &str) -> bool {
        let cfg = self.cfg();
        let mut map = self.model.lock().unwrap();
        Self::prune_map(&mut map);
        let e = map.entry(model_id.to_string()).or_insert_with(Entry::new);
        e.last_touched = Instant::now();
        check_entry(e, cfg.half_open_max_probes)
    }
    pub fn model_allow(&self, model_id: &str) -> bool {
        self.model_check(model_id)
    }
    pub fn model_is_available(&self, model_id: &str) -> bool {
        self.model_check(model_id)
    }

    pub fn model_reserve(&self, model_id: &str) -> bool {
        self.model_reserve_lease(model_id).is_some()
    }
    pub fn model_reserve_lease(&self, model_id: &str) -> Option<u64> {
        let cfg = self.cfg();
        let lease_id = next_lease_id();
        let mut map = self.model.lock().unwrap();
        Self::prune_map(&mut map);
        let e = map.entry(model_id.to_string()).or_insert_with(Entry::new);
        e.last_touched = Instant::now();
        if reserve_entry(e, cfg.half_open_max_probes, lease_id) {
            Some(lease_id)
        } else {
            None
        }
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
            e.active.clear();
            e.probes.clear();
            e.lease_gen.clear();
            e.last_touched = Instant::now();
        }
    }
    pub fn model_cancel_lease(&self, model_id: &str, lease_id: u64) {
        let mut map = self.model.lock().unwrap();
        if let Some(e) = map.get_mut(model_id) {
            if let Some(lease_generation) = e.lease_gen.get(&lease_id).copied()
                && lease_generation == e.generation
            {
                e.active.remove(&lease_id);
                e.probes.remove(&lease_id);
                e.lease_gen.remove(&lease_id);
            }
            e.last_touched = Instant::now();
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
        Self::prune_map(&mut map);
        if let Some(e) = map.get_mut(model_id) {
            e.state = BreakerState::Closed;
            e.opened_at = None;
            e.cooldown = Duration::ZERO;
            e.active.clear();
            e.probes.clear();
            e.lease_gen.clear();
            e.generation = e.generation.wrapping_add(1);
            e.last_touched = Instant::now();
        }
    }
    pub fn model_record_success_lease(&self, model_id: &str, lease_id: u64) -> bool {
        let mut map = self.model.lock().unwrap();
        Self::prune_map(&mut map);
        if let Some(e) = map.get_mut(model_id) {
            e.last_touched = Instant::now();
            let lease_generation = match e.lease_gen.get(&lease_id).copied() {
                Some(g) => g,
                None => return false,
            };
            if lease_generation != e.generation {
                return false;
            }
            if !e.active.remove(&lease_id) {
                return false;
            }
            let was_probe = e.probes.remove(&lease_id);
            e.lease_gen.remove(&lease_id);
            if e.state == BreakerState::HalfOpen && was_probe {
                e.state = BreakerState::Closed;
                e.opened_at = None;
                e.cooldown = Duration::ZERO;
                e.active.clear();
                e.probes.clear();
                e.lease_gen.clear();
                e.generation = e.generation.wrapping_add(1);
            }
            true
        } else {
            false
        }
    }

    pub fn model_record_failure(&self, model_id: &str) {
        let cfg = self.cfg();
        let cooldown = Duration::from_secs(cfg.model_not_found_cooldown_secs);
        let mut map = self.model.lock().unwrap();
        Self::prune_map(&mut map);
        let e = map.entry(model_id.to_string()).or_insert_with(Entry::new);
        e.state = BreakerState::Open;
        e.opened_at = Some(Instant::now());
        e.cooldown = cooldown;
        e.active.clear();
        e.probes.clear();
        e.lease_gen.clear();
        e.generation = e.generation.wrapping_add(1);
        e.last_touched = Instant::now();
    }
    pub fn model_record_failure_lease(&self, model_id: &str, lease_id: u64) -> bool {
        let cfg = self.cfg();
        let cooldown = Duration::from_secs(cfg.model_not_found_cooldown_secs);
        let mut map = self.model.lock().unwrap();
        Self::prune_map(&mut map);
        if let Some(e) = map.get_mut(model_id) {
            e.last_touched = Instant::now();
            let lease_generation = match e.lease_gen.get(&lease_id).copied() {
                Some(g) => g,
                None => return false,
            };
            if lease_generation != e.generation {
                return false;
            }
            if !e.active.remove(&lease_id) {
                return false;
            }
            let was_probe = e.probes.remove(&lease_id);
            e.lease_gen.remove(&lease_id);
            if e.state == BreakerState::Closed || (e.state == BreakerState::HalfOpen && was_probe) {
                e.state = BreakerState::Open;
                e.opened_at = Some(Instant::now());
                e.cooldown = cooldown;
                e.active.clear();
                e.probes.clear();
                e.lease_gen.clear();
                e.generation = e.generation.wrapping_add(1);
                return true;
            }
            false
        } else {
            false
        }
    }
    pub fn model_record_failure_outcome(&self, model_id: &str, lease_id: u64) -> bool {
        if lease_id != 0 {
            return self.model_record_failure_lease(model_id, lease_id);
        }
        false
    }
    pub fn model_record_success_outcome(&self, model_id: &str, lease_id: u64) -> bool {
        if lease_id != 0 {
            return self.model_record_success_lease(model_id, lease_id);
        }
        false
    }

    // RAII guards
    pub fn transport_reserve_guard(self: &Arc<Self>, proxy_id: &str) -> Option<TransportGuard> {
        self.transport_reserve_lease(proxy_id)
            .map(|lease_id| TransportGuard {
                cb: self.clone(),
                proxy_id: proxy_id.to_string(),
                lease_id,
                armed: true,
            })
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
        self.rate_limit_reserve_lease(proxy_id, origin)
            .map(|lease_id| RateLimitGuard {
                cb: self.clone(),
                proxy_id: proxy_id.to_string(),
                origin: origin.to_string(),
                lease_id,
                armed: true,
            })
    }
    pub fn rate_limit_lease(
        self: &Arc<Self>,
        proxy_id: &str,
        origin: &str,
    ) -> Option<RateLimitGuard> {
        self.rate_limit_reserve_guard(proxy_id, origin)
    }

    pub fn model_reserve_guard(self: &Arc<Self>, model_id: &str) -> Option<ModelGuard> {
        self.model_reserve_lease(model_id)
            .map(|lease_id| ModelGuard {
                cb: self.clone(),
                model_id: model_id.to_string(),
                lease_id,
                armed: true,
            })
    }
    pub fn model_lease(self: &Arc<Self>, model_id: &str) -> Option<ModelGuard> {
        self.model_reserve_guard(model_id)
    }

    pub fn transport_guard(self: &Arc<Self>, proxy_id: &str) -> TransportGuard {
        TransportGuard {
            cb: self.clone(),
            proxy_id: proxy_id.to_string(),
            lease_id: 0,
            armed: false,
        }
    }
    pub fn rate_limit_guard(self: &Arc<Self>, proxy_id: &str, origin: &str) -> RateLimitGuard {
        RateLimitGuard {
            cb: self.clone(),
            proxy_id: proxy_id.to_string(),
            origin: origin.to_string(),
            lease_id: 0,
            armed: false,
        }
    }
    pub fn model_guard(self: &Arc<Self>, model_id: &str) -> ModelGuard {
        ModelGuard {
            cb: self.clone(),
            model_id: model_id.to_string(),
            lease_id: 0,
            armed: false,
        }
    }

    // cleanup
    pub fn retain_proxies(&self, active: &HashSet<String>) {
        {
            let mut map = self.transport.lock().unwrap();
            map.retain(|k, e| active.contains(k) || !e.active.is_empty() || !e.probes.is_empty());
        }
        {
            let mut map = self.rate_limit.lock().unwrap();
            map.retain(|(pid, _), e| {
                active.contains(pid) || !e.active.is_empty() || !e.probes.is_empty()
            });
        }
    }
    pub fn retain_models(&self, active: &HashSet<String>) {
        let mut map = self.model.lock().unwrap();
        map.retain(|k, e| active.contains(k) || !e.active.is_empty() || !e.probes.is_empty());
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
            .map(|e| e.probes.len())
            .unwrap_or(0)
    }
    pub fn half_open_inflight_rate_limit(&self, proxy_id: &str, origin: &str) -> usize {
        self.rate_limit
            .lock()
            .unwrap()
            .get(&(proxy_id.to_string(), origin.to_string()))
            .map(|e| e.probes.len())
            .unwrap_or(0)
    }
    pub fn half_open_inflight_model(&self, model_id: &str) -> usize {
        self.model
            .lock()
            .unwrap()
            .get(model_id)
            .map(|e| e.probes.len())
            .unwrap_or(0)
    }
    pub fn generation_transport(&self, proxy_id: &str) -> u64 {
        self.transport
            .lock()
            .unwrap()
            .get(proxy_id)
            .map(|e| e.generation)
            .unwrap_or(0)
    }
    pub fn generation_model(&self, model_id: &str) -> u64 {
        self.model
            .lock()
            .unwrap()
            .get(model_id)
            .map(|e| e.generation)
            .unwrap_or(0)
    }
}

fn check_entry(e: &mut Entry, half_open_max: usize) -> bool {
    e.last_touched = Instant::now();
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
        BreakerState::HalfOpen => e.probes.len() < half_open_max,
    }
}

fn reserve_entry(e: &mut Entry, half_open_max: usize, lease_id: u64) -> bool {
    e.last_touched = Instant::now();
    match e.state {
        BreakerState::Closed => {
            e.active.insert(lease_id);
            e.lease_gen.insert(lease_id, e.generation);
            true
        }
        BreakerState::Open => {
            if let Some(opened) = e.opened_at
                && opened.elapsed() >= e.cooldown
            {
                e.state = BreakerState::HalfOpen;
                e.opened_at = None;
                e.active.clear();
                e.probes.clear();
                e.lease_gen.clear();
                e.generation = e.generation.wrapping_add(1);
                if e.probes.len() < half_open_max {
                    e.active.insert(lease_id);
                    e.probes.insert(lease_id);
                    e.lease_gen.insert(lease_id, e.generation);
                    return true;
                }
                false
            } else {
                false
            }
        }
        BreakerState::HalfOpen => {
            if e.probes.len() < half_open_max {
                e.active.insert(lease_id);
                e.probes.insert(lease_id);
                e.lease_gen.insert(lease_id, e.generation);
                true
            } else {
                false
            }
        }
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
            retry_after_max_secs: 300,
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
        // release slot via cancel to allow close path test
        cb.transport_cancel("p0");
    }

    #[test]
    fn stale_success_does_not_close() {
        use std::sync::Arc;
        let cb = Arc::new(CircuitBreaker::new(CircuitBreakerConfig {
            proxy_failure_cooldown_secs: 0,
            ..cfg()
        }));
        // request A reserves in closed? Actually closed reserve active no probe
        let lease_a = cb.transport_reserve_lease("p0").unwrap();
        // failure via another lease opens circuit
        let lease_b = cb.transport_reserve_lease("p0").unwrap();
        assert!(cb.transport_record_failure_lease("p0", lease_b));
        // stale success from A must not close
        assert!(!cb.transport_record_success_lease("p0", lease_a));
        assert_eq!(cb.transport_state("p0"), BreakerState::Open);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn async_cancel_releases_half_open_slot() {
        use std::sync::Arc;
        let cb = Arc::new(CircuitBreaker::new(CircuitBreakerConfig {
            proxy_failure_cooldown_secs: 0,
            half_open_max_probes: 1,
            retry_after_max_secs: 300,
            ..cfg()
        }));
        cb.transport_record_failure("p0");
        tokio::time::sleep(Duration::from_millis(5)).await;
        let cb2 = cb.clone();
        let fut = async move {
            let _g = cb2.transport_reserve_guard("p0").expect("probe slot");
            std::future::pending::<()>().await;
        };
        let _ = tokio::time::timeout(Duration::from_millis(20), fut).await;
        assert!(
            cb.transport_reserve("p0"),
            "subsequent probe must acquire after cancellation"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn async_rate_limit_cancel_releases_slot() {
        use std::sync::Arc;
        let cb = Arc::new(CircuitBreaker::new(CircuitBreakerConfig {
            rate_limit_cooldown_secs: 0,
            half_open_max_probes: 1,
            retry_after_max_secs: 300,
            ..cfg()
        }));
        cb.rate_limit_record_failure("p0", "origin", None);
        tokio::time::sleep(Duration::from_millis(5)).await;
        let cb2 = cb.clone();
        let fut = async move {
            let _g = cb2
                .rate_limit_reserve_guard("p0", "origin")
                .expect("probe slot");
            std::future::pending::<()>().await;
        };
        let _ = tokio::time::timeout(Duration::from_millis(20), fut).await;
        assert!(
            cb.rate_limit_reserve("p0", "origin"),
            "subsequent probe must acquire after cancellation"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn async_model_cancel_releases_slot() {
        use std::sync::Arc;
        let cb = Arc::new(CircuitBreaker::new(CircuitBreakerConfig {
            model_not_found_cooldown_secs: 0,
            half_open_max_probes: 1,
            retry_after_max_secs: 300,
            ..cfg()
        }));
        cb.model_record_failure("m0");
        tokio::time::sleep(Duration::from_millis(5)).await;
        let cb2 = cb.clone();
        let fut = async move {
            let _g = cb2.model_reserve_guard("m0").expect("probe slot");
            std::future::pending::<()>().await;
        };
        let _ = tokio::time::timeout(Duration::from_millis(20), fut).await;
        assert!(
            cb.model_reserve("m0"),
            "subsequent probe must acquire after cancellation"
        );
    }

    #[test]
    fn stale_lease_cannot_close_or_open_new_generation() {
        use std::sync::Arc;
        let cb = Arc::new(CircuitBreaker::new(CircuitBreakerConfig {
            proxy_failure_cooldown_secs: 0,
            half_open_max_probes: 1,
            retry_after_max_secs: 300,
            ..cfg()
        }));
        // Request A takes a closed-state lease.
        let lease_a = cb.transport_reserve_lease("p0").expect("lease A");
        // Breaker opens (new generation) via an independent transition.
        cb.transport_record_failure("p0");
        // Probe transitions to half-open (newer generation).
        let lease_b = cb.transport_reserve_lease("p0").expect("probe");
        // Stale success from A must not close the breaker.
        assert!(!cb.transport_record_success_lease("p0", lease_a));
        assert_eq!(cb.transport_state("p0"), BreakerState::HalfOpen);
        // Stale failure from A must not open/overwrite the newer generation.
        assert!(!cb.transport_record_failure_lease("p0", lease_a));
        assert_eq!(cb.transport_state("p0"), BreakerState::HalfOpen);
        // Probe B still valid: closes the circuit.
        assert!(cb.transport_record_success_lease("p0", lease_b));
        assert_eq!(cb.transport_state("p0"), BreakerState::Closed);
        // Cancellation of A must not remove B's (already-consumed) state or
        // affect anything after B finalized.
        cb.transport_cancel_lease("p0", lease_a);
        cb.transport_cancel_lease("p0", lease_b); // idempotent double-release
    }

    #[test]
    fn open_past_two_cooldowns_still_requires_half_open() {
        use std::sync::Arc;
        let cb = Arc::new(CircuitBreaker::new(CircuitBreakerConfig {
            proxy_failure_cooldown_secs: 0,
            half_open_max_probes: 1,
            retry_after_max_secs: 300,
            ..cfg()
        }));
        cb.transport_record_failure("p0");
        std::thread::sleep(Duration::from_millis(5));
        // First cooldown expiry -> half-open probe.
        let lease1 = cb.transport_reserve_lease("p0").expect("probe 1");
        cb.transport_cancel_lease("p0", lease1);
        std::thread::sleep(Duration::from_millis(5));
        // Second cooldown expiry -> must still be half-open probe, not closed.
        let lease2 = cb.transport_reserve_lease("p0").expect("probe 2");
        assert_eq!(cb.transport_state("p0"), BreakerState::HalfOpen);
        cb.transport_cancel_lease("p0", lease2);
        // Without a successful probe the breaker must not be Closed.
        assert_ne!(cb.transport_state("p0"), BreakerState::Closed);
    }

    #[test]
    fn retain_models_keeps_leased_entry_and_drops_when_lease_ends() {
        use std::sync::Arc;
        let cb = Arc::new(CircuitBreaker::new(cfg()));
        // A model with an active lease that is no longer in the catalog.
        let lease = cb.model_reserve_lease("removed-model").expect("lease");
        let mut active = HashSet::new();
        active.insert("kept-model".to_string());
        // Entry must survive cleanup while the lease is active.
        cb.retain_models(&active);
        assert_eq!(cb.model_state("removed-model"), BreakerState::Closed);
        // End the lease; next cleanup drops the entry.
        cb.model_cancel_lease("removed-model", lease);
        cb.retain_models(&active);
        // state helper reports Closed for missing entries; verify map dropped
        // by checking a fresh reserve starts generation at 0 (no stale entry).
        let lease2 = cb.model_reserve_lease("removed-model").expect("fresh");
        assert_eq!(cb.generation_model("removed-model"), 0);
        cb.model_cancel_lease("removed-model", lease2);
    }

    #[test]
    fn model_map_bounded_under_arbitrary_keys() {
        use std::sync::Arc;
        let cb = Arc::new(CircuitBreaker::new(cfg()));
        for i in 0..500 {
            let id = format!("model-{i}");
            let _ = cb.model_reserve_guard(&id); // dropped immediately
        }
        let active = HashSet::new();
        cb.retain_models(&active);
        let map = cb.model.lock().unwrap();
        assert!(
            map.len() <= 1,
            "cleanup must bound model map size, got {}",
            map.len()
        );
    }

    #[test]
    fn lease_outcome_applies_once_breaker_level() {
        let cb = CircuitBreaker::new(CircuitBreakerConfig {
            proxy_failure_cooldown_secs: 0,
            half_open_max_probes: 1,
            retry_after_max_secs: 300,
            ..cfg()
        });
        cb.transport_record_failure("p0");
        std::thread::sleep(Duration::from_millis(5));
        let lease = cb.transport_reserve_lease("p0").expect("probe");
        assert!(cb.transport_record_failure_lease("p0", lease));
        let (opened1, cooldown1) = {
            let map = cb.transport.lock().unwrap();
            let e = map.get("p0").unwrap();
            (e.opened_at, e.cooldown)
        };
        // Second application of the same lease must be rejected.
        assert!(!cb.transport_record_failure_lease("p0", lease));
        let (opened2, cooldown2) = {
            let map = cb.transport.lock().unwrap();
            let e = map.get("p0").unwrap();
            (e.opened_at, e.cooldown)
        };
        assert_eq!(opened1, opened2, "opened_at must not be overwritten");
        assert_eq!(cooldown1, cooldown2);
        assert_eq!(cb.transport_state("p0"), BreakerState::Open);
    }

    #[test]
    fn cancel_lease_stale_generation_noop() {
        let cb = CircuitBreaker::new(CircuitBreakerConfig {
            proxy_failure_cooldown_secs: 0,
            half_open_max_probes: 1,
            retry_after_max_secs: 300,
            ..cfg()
        });
        // Old-generation lease.
        let stale = cb.transport_reserve_lease("p0").expect("lease");
        cb.transport_record_failure("p0");
        std::thread::sleep(Duration::from_millis(5));
        // Current-generation probe.
        let probe = cb.transport_reserve_lease("p0").expect("probe");
        // Cancelling the stale lease must not touch the current probe slot.
        cb.transport_cancel_lease("p0", stale);
        assert_eq!(cb.half_open_inflight_transport("p0"), 1);
        assert_eq!(cb.transport_state("p0"), BreakerState::HalfOpen);
        // Current probe still finalizable.
        assert!(cb.transport_record_success_lease("p0", probe));
        assert_eq!(cb.transport_state("p0"), BreakerState::Closed);
    }

    #[test]
    fn probe_failure_reopens_with_fresh_cooldown() {
        let cb = CircuitBreaker::new(CircuitBreakerConfig {
            proxy_failure_cooldown_secs: 7,
            half_open_max_probes: 1,
            retry_after_max_secs: 300,
            ..cfg()
        });
        cb.transport_record_failure("p0");
        // Expire the 7s cooldown by backdating opened_at.
        {
            let mut map = cb.transport.lock().unwrap();
            let e = map.get_mut("p0").unwrap();
            e.opened_at = Some(Instant::now() - Duration::from_secs(8));
        }
        let lease = cb.transport_reserve_lease("p0").expect("probe");
        assert!(cb.transport_record_failure_lease("p0", lease));
        let map = cb.transport.lock().unwrap();
        let e = map.get("p0").unwrap();
        assert_eq!(e.state, BreakerState::Open);
        assert_eq!(e.cooldown, Duration::from_secs(7));
        assert_eq!(
            e.opened_at.map(|t| t.elapsed() < Duration::from_secs(1)),
            Some(true),
            "probe failure must reopen with a fresh opened_at"
        );
    }

    #[test]
    fn rate_limit_and_model_stale_lease_rejected() {
        let cb = CircuitBreaker::new(CircuitBreakerConfig {
            rate_limit_cooldown_secs: 0,
            model_not_found_cooldown_secs: 0,
            half_open_max_probes: 1,
            retry_after_max_secs: 300,
            ..cfg()
        });
        // Rate limit scope.
        let stale_rl = cb.rate_limit_reserve_lease("p0", "o").expect("lease");
        cb.rate_limit_record_failure("p0", "o", None);
        std::thread::sleep(Duration::from_millis(5));
        let probe_rl = cb.rate_limit_reserve_lease("p0", "o").expect("probe");
        assert!(!cb.rate_limit_record_success_lease("p0", "o", stale_rl));
        assert_eq!(cb.rate_limit_state("p0", "o"), BreakerState::HalfOpen);
        assert!(cb.rate_limit_record_success_lease("p0", "o", probe_rl));
        // Model scope.
        let stale_m = cb.model_reserve_lease("m0").expect("lease");
        cb.model_record_failure("m0");
        std::thread::sleep(Duration::from_millis(5));
        let probe_m = cb.model_reserve_lease("m0").expect("probe");
        assert!(!cb.model_record_failure_lease("m0", stale_m));
        assert_eq!(cb.model_state("m0"), BreakerState::HalfOpen);
        assert!(cb.model_record_success_lease("m0", probe_m));
        assert_eq!(cb.model_state("m0"), BreakerState::Closed);
    }

    #[test]
    fn outcome_wrappers_zero_lease_noop() {
        let cb = CircuitBreaker::new(cfg());
        cb.transport_record_failure("p0");
        assert!(!cb.transport_record_failure_outcome("p0", 0));
        assert!(!cb.transport_record_success_outcome("p0", 0));
        assert!(!cb.rate_limit_record_failure_outcome("p0", "o", 0, None));
        assert!(!cb.rate_limit_record_success_outcome("p0", "o", 0));
        assert!(!cb.model_record_failure_outcome("m0", 0));
        assert!(!cb.model_record_success_outcome("m0", 0));
        assert_eq!(cb.transport_state("p0"), BreakerState::Open);
    }

    #[test]
    fn retry_after_capped_in_lease_failure() {
        let cb = CircuitBreaker::new(CircuitBreakerConfig {
            retry_after_max_secs: 30,
            ..cfg()
        });
        let lease = cb.rate_limit_reserve_lease("p0", "o").expect("lease");
        assert!(cb.rate_limit_record_failure_lease(
            "p0",
            "o",
            lease,
            Some(Duration::from_secs(100_000))
        ));
        let map = cb.rate_limit.lock().unwrap();
        let e = map.get(&("p0".to_string(), "o".to_string())).unwrap();
        assert_eq!(e.cooldown, Duration::from_secs(30));
    }

    #[test]
    fn half_open_multi_probe_concurrency() {
        use std::sync::Arc;
        let cb = Arc::new(CircuitBreaker::new(CircuitBreakerConfig {
            proxy_failure_cooldown_secs: 0,
            half_open_max_probes: 3,
            retry_after_max_secs: 300,
            ..cfg()
        }));
        cb.transport_record_failure("p0");
        std::thread::sleep(Duration::from_millis(5));
        let cb2 = cb.clone();
        let h = std::thread::spawn(move || cb2.transport_reserve("p0"));
        let r1 = cb.transport_reserve("p0");
        let r2 = h.join().unwrap();
        // With max 3 both main-thread and child probes must be admitted.
        assert!(r1 && r2);
        assert_eq!(cb.half_open_inflight_transport("p0"), 2);
        // 3rd admitted, 4th rejected.
        assert!(cb.transport_reserve("p0"));
        assert!(!cb.transport_reserve("p0"));
    }

    #[test]
    fn retain_models_keeps_open_entry_for_cataloged_model() {
        use std::sync::Arc;
        let cb = Arc::new(CircuitBreaker::new(cfg()));
        cb.model_record_failure("kept-model");
        let mut active = HashSet::new();
        active.insert("kept-model".to_string());
        cb.retain_models(&active);
        assert_eq!(cb.model_state("kept-model"), BreakerState::Open);
    }

    #[test]
    fn retain_proxies_keeps_active_lease_of_removed_proxy() {
        use std::sync::Arc;
        let cb = Arc::new(CircuitBreaker::new(cfg()));
        let lease = cb.transport_reserve_lease("removed-proxy").expect("lease");
        let rg = cb
            .rate_limit_reserve_lease("removed-proxy", "o")
            .expect("rl lease");
        let active = HashSet::new();
        cb.retain_proxies(&active);
        // Entries with live leases survive; leases remain finalizable.
        assert!(cb.transport_record_success_lease("removed-proxy", lease));
        assert!(cb.rate_limit_record_success_lease("removed-proxy", "o", rg));
    }

    #[test]
    fn concurrency_one_probe() {
        use std::sync::Arc;
        let cb = Arc::new(CircuitBreaker::new(CircuitBreakerConfig {
            proxy_failure_cooldown_secs: 0,
            half_open_max_probes: 1,
            retry_after_max_secs: 300,
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
