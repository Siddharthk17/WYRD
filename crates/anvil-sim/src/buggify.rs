use std::collections::BTreeMap;
use std::sync::Mutex;

/// Metadata and activation statistics for a single buggify injection point.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BuggifyPoint {
    pub name: String,
    pub location: String,
    pub probability_ppm: u32,
    pub evaluations: u64,
    pub activations: u64,
}

/// Global registry recording all buggify evaluation points and their activation frequencies.
pub struct BuggifyRegistry {
    enabled: bool,
    points: BTreeMap<String, BuggifyPoint>,
    state: u64,
}

impl BuggifyRegistry {
    /// Creates a new registry.
    pub const fn new() -> Self {
        Self {
            enabled: true,
            points: BTreeMap::new(),
            state: 0xDEAD_BEEF_CAFE_BABE,
        }
    }

    /// Enables or disables buggify point firing globally.
    pub fn set_enabled(&mut self, enabled: bool) {
        self.enabled = enabled;
    }

    /// Sets the master pseudo-random state for buggify evaluations.
    pub fn set_seed(&mut self, seed: u64) {
        self.state = seed;
    }

    /// Returns whether buggify is currently enabled.
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// Evaluates a buggify point, updating statistics and returning true if activated.
    pub fn evaluate(&mut self, name: &str, file: &str, line: u32, probability_ppm: u32) -> bool {
        let location = format!("{file}:{line}");
        let key = format!("{name}@{location}");

        let point = self.points.entry(key).or_insert_with(|| BuggifyPoint {
            name: name.to_string(),
            location,
            probability_ppm,
            evaluations: 0,
            activations: 0,
        });

        point.evaluations = point.evaluations.saturating_add(1);

        if !self.enabled || probability_ppm == 0 {
            return false;
        }

        // Fast deterministic pseudo-random draw using SplitMix64 round
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        let rand_val = (z ^ (z >> 31)) % 1_000_000;

        let active = rand_val < u64::from(probability_ppm);
        if active {
            point.activations = point.activations.saturating_add(1);
        }
        active
    }

    /// Returns a list of all registered buggify points and their activation statistics.
    pub fn list_points(&self) -> Vec<BuggifyPoint> {
        self.points.values().cloned().collect()
    }

    /// Resets all statistics.
    pub fn reset(&mut self) {
        self.points.clear();
    }
}

impl Default for BuggifyRegistry {
    fn default() -> Self {
        Self::new()
    }
}

static GLOBAL_BUGGIFY: Mutex<BuggifyRegistry> = Mutex::new(BuggifyRegistry::new());

/// Evaluates a buggify point against the global registry.
#[allow(clippy::unwrap_used)]
pub fn record_and_evaluate(name: &str, file: &str, line: u32, probability_ppm: u32) -> bool {
    let mut reg = GLOBAL_BUGGIFY
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    reg.evaluate(name, file, line, probability_ppm)
}

/// Lists all buggify points recorded so far.
#[allow(clippy::unwrap_used)]
pub fn list_registered_points() -> Vec<BuggifyPoint> {
    let reg = GLOBAL_BUGGIFY
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    reg.list_points()
}

/// Configures whether buggify points fire.
#[allow(clippy::unwrap_used)]
pub fn set_buggify_enabled(enabled: bool) {
    let mut reg = GLOBAL_BUGGIFY
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    reg.set_enabled(enabled);
}

/// Sets the master seed for buggify pseudo-random evaluation.
#[allow(clippy::unwrap_used)]
pub fn set_buggify_seed(seed: u64) {
    let mut reg = GLOBAL_BUGGIFY
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    reg.set_seed(seed);
}

/// Resets the global buggify registry.
#[allow(clippy::unwrap_used)]
pub fn reset_buggify_registry() {
    let mut reg = GLOBAL_BUGGIFY
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    reg.reset();
}

/// Buggify macro: injects deterministic pseudo-random faults.
/// Evaluates to constant false unless the `sim` feature is enabled.
#[macro_export]
#[cfg(feature = "sim")]
macro_rules! buggify {
    ($name:expr, $probability_ppm:expr) => {
        $crate::buggify::record_and_evaluate($name, file!(), line!(), $probability_ppm)
    };
    ($probability_ppm:expr) => {
        $crate::buggify::record_and_evaluate("default", file!(), line!(), $probability_ppm)
    };
    () => {
        $crate::buggify::record_and_evaluate("default", file!(), line!(), 250_000)
    };
}

#[macro_export]
#[cfg(not(feature = "sim"))]
macro_rules! buggify {
    ($($args:tt)*) => {
        false
    };
}
