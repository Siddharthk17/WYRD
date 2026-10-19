use core::ops::{Range, RangeInclusive};
use rand_chacha::ChaCha8Rng;
use rand_chacha::rand_core::{Rng, SeedableRng};

/// `SplitMix64` pseudo-random generator used to expand master seed into component streams.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SplitMix64 {
    state: u64,
}

impl SplitMix64 {
    /// Initializes a new `SplitMix64` state with the given 64-bit seed.
    pub const fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    /// Draws the next 64-bit pseudo-random value.
    pub fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Mixes an additional 64-bit word into the generator state.
    #[must_use]
    pub fn mix(mut self, input: u64) -> Self {
        self.state = self.state.wrapping_add(input);
        let _ = self.next_u64();
        self
    }
}

/// Derives a 32-byte `ChaCha8` seed from a 64-bit master seed and a hierarchical component path.
#[must_use]
pub fn derive_component_seed(master_seed: u64, component_path: &str) -> [u8; 32] {
    let mut sm = SplitMix64::new(master_seed);

    let bytes = component_path.as_bytes();
    let mut chunk = 0u64;
    let mut shift = 0;
    for &b in bytes {
        chunk |= u64::from(b) << shift;
        shift += 8;
        if shift == 64 {
            sm = sm.mix(chunk);
            chunk = 0;
            shift = 0;
        }
    }
    if shift > 0 {
        sm = sm.mix(chunk);
    }
    sm = sm.mix(bytes.len() as u64);

    let w0 = sm.next_u64();
    let w1 = sm.next_u64();
    let w2 = sm.next_u64();
    let w3 = sm.next_u64();

    let mut seed = [0u8; 32];
    seed[0..8].copy_from_slice(&w0.to_le_bytes());
    seed[8..16].copy_from_slice(&w1.to_le_bytes());
    seed[16..24].copy_from_slice(&w2.to_le_bytes());
    seed[24..32].copy_from_slice(&w3.to_le_bytes());
    seed
}

/// Deterministic random number generator backed by an exact-pinned `ChaCha8` engine.
#[derive(Debug, Clone)]
pub struct DetRng {
    rng: ChaCha8Rng,
}

impl DetRng {
    /// Constructs a `DetRng` for a specific component path derived from a master seed.
    pub fn new(master_seed: u64, component_path: &str) -> Self {
        let seed = derive_component_seed(master_seed, component_path);
        Self::from_seed(seed)
    }

    /// Constructs a `DetRng` directly from a 32-byte seed.
    pub fn from_seed(seed: [u8; 32]) -> Self {
        Self {
            rng: ChaCha8Rng::from_seed(seed),
        }
    }

    /// Returns the next raw pseudo-random 64-bit word.
    pub fn next_u64(&mut self) -> u64 {
        self.rng.next_u64()
    }

    /// Returns the next raw pseudo-random 32-bit word.
    pub fn next_u32(&mut self) -> u32 {
        self.rng.next_u32()
    }

    /// Returns an unbiased uniformly distributed random integer in `range.start..range.end`.
    ///
    /// Implements Lemire's nearly divisionless algorithm for high-speed unbiased sampling.
    #[allow(clippy::cast_possible_truncation)]
    pub fn gen_range(&mut self, range: Range<u64>) -> u64 {
        let low = range.start;
        let high = range.end;
        assert!(low < high, "range must be non-empty (low < high)");
        let span = high - low;
        if span == 1 {
            return low;
        }

        let mut x = self.next_u64();
        let mut m = u128::from(x) * u128::from(span);
        let mut l = m as u64;

        if l < span {
            let threshold = span.wrapping_neg() % span;
            while l < threshold {
                x = self.next_u64();
                m = u128::from(x) * u128::from(span);
                l = m as u64;
            }
        }

        low + (m >> 64) as u64
    }

    /// Returns an unbiased uniformly distributed random integer in `*range.start()..=*range.end()`.
    pub fn gen_range_inclusive(&mut self, range: RangeInclusive<u64>) -> u64 {
        let low = *range.start();
        let high = *range.end();
        assert!(low <= high, "range must be valid (low <= high)");
        if low == high {
            return low;
        }
        if high == u64::MAX && low == 0 {
            return self.next_u64();
        }
        let span = (high - low) + 1;
        self.gen_range(low..(low + span))
    }

    /// Samples a boolean with probability `numerator / denominator`.
    pub fn bernoulli(&mut self, numerator: u64, denominator: u64) -> bool {
        assert!(denominator > 0, "denominator must be positive");
        if numerator >= denominator {
            return true;
        }
        if numerator == 0 {
            return false;
        }
        self.gen_range(0..denominator) < numerator
    }

    /// Samples a boolean with probability given in parts per million (ppm).
    pub fn bernoulli_ppm(&mut self, ppm: u32) -> bool {
        self.bernoulli(u64::from(ppm), 1_000_000)
    }

    /// Samples from an exponential distribution with the specified mean using pure integer arithmetic.
    ///
    /// Uses 32-bit fixed-point inverse transform sampling: $X = -\text{mean} \times \ln(U)$.
    #[allow(clippy::cast_possible_truncation)]
    pub fn exponential(&mut self, mean_nanos: u64) -> u64 {
        if mean_nanos == 0 {
            return 0;
        }

        // Generate non-zero 32-bit random sample U in (0, 1]
        let mut u = self.next_u32();
        if u == 0 {
            u = 1;
        }

        // Fixed-point -ln(U) approximation with 16 fractional bits:
        // -ln(u / 2^32) = 32*ln(2) - ln(u)
        // 32 * ln(2) * 65536 = 1,453,598
        let ln_u_fixed = integer_ln_fixed(u);
        let neg_ln_fixed = 1_453_598u64.saturating_sub(ln_u_fixed);

        // Result = (mean_nanos * neg_ln_fixed) >> 16
        let product = u128::from(mean_nanos) * u128::from(neg_ln_fixed);
        (product >> 16) as u64
    }

    /// Samples from a bimodal distribution with two distinct modes (e.g. fast path vs disk I/O stall).
    ///
    /// With probability `p_first_ppm / 1_000_000`, draws around `(mean1, spread1)`.
    /// Otherwise, draws around `(mean2, spread2)`.
    pub fn bimodal(
        &mut self,
        mean1: u64,
        spread1: u64,
        mean2: u64,
        spread2: u64,
        p_first_ppm: u32,
    ) -> u64 {
        let (mean, spread) = if self.bernoulli_ppm(p_first_ppm) {
            (mean1, spread1)
        } else {
            (mean2, spread2)
        };

        if spread == 0 {
            return mean;
        }

        // Quasi-normal triangular/Irwin-Hall variation around mean
        let delta = self.gen_range_inclusive(0..=(spread.saturating_mul(2)));
        mean.saturating_sub(spread).saturating_add(delta)
    }

    /// In-place unbiased Fisher-Yates shuffle of a slice.
    #[allow(clippy::cast_possible_truncation)]
    pub fn shuffle<T>(&mut self, slice: &mut [T]) {
        let len = slice.len();
        if len <= 1 {
            return;
        }
        for i in (1..len).rev() {
            let j = self.gen_range_inclusive(0..=(i as u64)) as usize;
            slice.swap(i, j);
        }
    }
}

/// Computes an integer natural log approximation of `x` (`u32`) scaled by $2^{16} = 65536$.
#[allow(clippy::cast_possible_truncation)]
fn integer_ln_fixed(x: u32) -> u64 {
    if x <= 1 {
        return 0;
    }

    // Leading zeros gives the binary exponent
    let leading_zeros = x.leading_zeros();
    let k = 31 - leading_zeros; // x in [2^k, 2^(k+1))

    // ln(x) = k * ln(2) + ln(1 + f) where f = (x - 2^k) / 2^k
    // ln(2) in 16-bit fixed point = 45426
    let k_ln2 = u64::from(k) * 45426;

    // Linear + quadratic Taylor expansion for ln(1 + f): f - f^2/2
    // f in 16-bit fixed point:
    let rem = x - (1 << k);
    let f = ((u64::from(rem) << 16) >> k) as u32;

    let f_term = u64::from(f);
    let f_sq_half = (u64::from(f) * u64::from(f)) >> 17;
    let ln_1_plus_f = f_term.saturating_sub(f_sq_half);

    k_ln2.saturating_add(ln_1_plus_f)
}
