// SPDX-License-Identifier: BUSL-1.1

//! Seedable CSPRNG wrapper for provably fair random selection.
//!
//! Uses `rand::rngs::StdRng` (ChaCha-based) with optional seed.
//! When a seed is provided, the same seed + same data produces the same result.
//! When no seed is provided, uses OS entropy.

use rand::RngExt;
use rand::rngs::StdRng;

/// Seedable random number generator.
///
/// Wraps `StdRng` (ChaCha-based CSPRNG). Either seeded deterministically
/// from a string (for provably fair draws) or from OS entropy.
pub struct SeedableRng {
    inner: StdRng,
}

impl SeedableRng {
    /// Create from OS entropy (non-deterministic).
    pub fn from_entropy() -> Self {
        use rand::SeedableRng as _;
        Self {
            inner: StdRng::from_rng(&mut rand::rng()),
        }
    }

    /// Create from a string seed (deterministic).
    ///
    /// The seed string is hashed with SHA-256 to produce 32 bytes for StdRng.
    /// Same seed → same sequence of random numbers. Suitable for provably
    /// fair draws where the seed is revealed after the fact.
    pub fn from_seed_str(seed: &str) -> Self {
        use sha2::{Digest, Sha256};

        let hash = Sha256::digest(seed.as_bytes());
        let mut seed_bytes = [0u8; 32];
        seed_bytes.copy_from_slice(&hash);

        use rand::SeedableRng as _;
        Self {
            inner: StdRng::from_seed(seed_bytes),
        }
    }

    /// Generate a random u64 in [0, upper_exclusive).
    pub fn gen_range(&mut self, upper_exclusive: u64) -> u64 {
        self.inner.random_range(0..upper_exclusive)
    }

    /// Generate a random f64 in [0.0, 1.0).
    pub fn gen_f64(&mut self) -> f64 {
        self.inner.random::<f64>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deterministic_with_same_seed() {
        let mut rng1 = SeedableRng::from_seed_str("player-123:pull-456");
        let mut rng2 = SeedableRng::from_seed_str("player-123:pull-456");

        let seq1: Vec<u64> = (0..10).map(|_| rng1.gen_range(1000)).collect();
        let seq2: Vec<u64> = (0..10).map(|_| rng2.gen_range(1000)).collect();

        assert_eq!(seq1, seq2);
    }

    #[test]
    fn different_seeds_different_results() {
        let mut rng1 = SeedableRng::from_seed_str("seed-A");
        let mut rng2 = SeedableRng::from_seed_str("seed-B");

        let val1 = rng1.gen_range(1_000_000);
        let val2 = rng2.gen_range(1_000_000);

        assert_ne!(val1, val2);
    }

    #[test]
    fn entropy_rng_produces_values() {
        let mut rng = SeedableRng::from_entropy();
        let val = rng.gen_range(1000);
        assert!(val < 1000);
    }

    #[test]
    fn gen_f64_in_range() {
        let mut rng = SeedableRng::from_seed_str("test");
        for _ in 0..100 {
            let v = rng.gen_f64();
            assert!((0.0..1.0).contains(&v));
        }
    }
}
