//! Process-wide admission control with safe hot-reloadable capacity.

use std::sync::Mutex;

/// A controller that enforces a hard upper bound on the number of in-flight
/// requests for the whole process. Capacity can be changed at runtime without
/// replacing the controller instance, so requests admitted under an older
/// configuration keep using the same accounting.
pub struct AdmissionController {
    state: Mutex<State>,
}

#[derive(Debug)]
struct State {
    capacity: usize,
    active: usize,
}

/// RAII lease representing one admitted request. Dropping it releases the
/// permit exactly once, including on cancellation/panic paths.
pub struct AdmissionPermit {
    controller: std::sync::Arc<AdmissionController>,
    armed: bool,
}

impl AdmissionController {
    pub fn new(capacity: usize) -> Self {
        Self {
            state: Mutex::new(State {
                capacity: capacity.max(1),
                active: 0,
            }),
        }
    }

    /// Change the configured capacity. Increasing it allows more requests;
    /// decreasing it does not cancel in-flight requests, but new acquisitions
    /// are rejected until `active < capacity`.
    pub fn update_capacity(&self, capacity: usize) {
        let mut st = self.state.lock().expect("admission lock");
        st.capacity = capacity.max(1);
    }

    pub fn capacity(&self) -> usize {
        self.state.lock().expect("admission lock").capacity
    }

    pub fn active(&self) -> usize {
        self.state.lock().expect("admission lock").active
    }

    /// Try to admit a request. Returns a permit when under capacity.
    pub fn try_acquire(self: &std::sync::Arc<Self>) -> Option<AdmissionPermit> {
        let mut st = self.state.lock().expect("admission lock");
        if st.active < st.capacity {
            st.active += 1;
            drop(st);
            Some(AdmissionPermit {
                controller: self.clone(),
                armed: true,
            })
        } else {
            None
        }
    }

    fn release_one(&self) {
        let mut st = self.state.lock().expect("admission lock");
        st.active = st.active.saturating_sub(1);
    }
}

impl Drop for AdmissionPermit {
    fn drop(&mut self) {
        if self.armed {
            self.controller.release_one();
            self.armed = false;
        }
    }
}

impl AdmissionPermit {
    /// Explicitly release; idempotent.
    pub fn release(&mut self) {
        if self.armed {
            self.controller.release_one();
            self.armed = false;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn capacity_enforced() {
        let c = Arc::new(AdmissionController::new(2));
        let p1 = c.try_acquire().unwrap();
        let p2 = c.try_acquire().unwrap();
        assert!(c.try_acquire().is_none());
        drop(p1);
        assert!(c.try_acquire().is_some());
        drop(p2);
    }

    #[test]
    fn increase_capacity() {
        let c = Arc::new(AdmissionController::new(1));
        let p = c.try_acquire().unwrap();
        assert!(c.try_acquire().is_none());
        c.update_capacity(2);
        assert!(c.try_acquire().is_some());
        drop(p);
    }

    #[test]
    fn decrease_capacity_no_cancel() {
        let c = Arc::new(AdmissionController::new(2));
        let p1 = c.try_acquire().unwrap();
        let p2 = c.try_acquire().unwrap();
        c.update_capacity(1);
        assert!(c.try_acquire().is_none());
        assert_eq!(c.active(), 2);
        drop(p1);
        drop(p2);
        assert_eq!(c.active(), 0);
        // now can acquire one
        assert!(c.try_acquire().is_some());
    }

    #[test]
    fn permit_drop_once() {
        let c = Arc::new(AdmissionController::new(1));
        let mut p = c.try_acquire().unwrap();
        p.release();
        p.release();
        assert_eq!(c.active(), 0);
        assert!(c.try_acquire().is_some());
    }
}
