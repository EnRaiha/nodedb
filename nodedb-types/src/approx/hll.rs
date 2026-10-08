// SPDX-License-Identifier: Apache-2.0

//! HyperLogLog — approximate count distinct (12 KB memory, ~0.8% error).

/// HyperLogLog cardinality estimator.
///
/// Uses 2^14 = 16384 registers (12 KB memory). Achieves ~0.8% relative
/// error at any cardinality. Mergeable: `hll_a.merge(&hll_b)` produces
/// the union cardinality.
#[derive(Debug, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub struct HyperLogLog {
    registers: Vec<u8>,
    precision: u8,
}

impl HyperLogLog {
    pub fn new() -> Self {
        Self::with_precision(14)
    }

    pub fn with_precision(p: u8) -> Self {
        let p = p.clamp(4, 18);
        let m = 1usize << p;
        Self {
            registers: vec![0u8; m],
            precision: p,
        }
    }

    pub fn add(&mut self, value: u64) {
        let hash = splitmix64(value);
        let m = self.registers.len();
        let idx = (hash as usize) & (m - 1);
        let remaining = hash >> self.precision;
        let leading_zeros = if remaining == 0 {
            (64 - self.precision) + 1
        } else {
            (remaining.leading_zeros() as u8).saturating_sub(self.precision) + 1
        };
        if leading_zeros > self.registers[idx] {
            self.registers[idx] = leading_zeros;
        }
    }

    pub fn add_batch(&mut self, values: &[u64]) {
        for &v in values {
            self.add(v);
        }
    }

    pub fn add_f64_batch(&mut self, values: &[f64]) {
        for &v in values {
            self.add(v.to_bits());
        }
    }

    pub fn estimate(&self) -> f64 {
        let m = self.registers.len() as f64;
        let alpha = match self.registers.len() {
            16 => 0.673,
            32 => 0.697,
            64 => 0.709,
            _ => 0.7213 / (1.0 + 1.079 / m),
        };

        let sum: f64 = self
            .registers
            .iter()
            .map(|&r| 2.0f64.powi(-(r as i32)))
            .sum();
        let raw_estimate = alpha * m * m / sum;

        if raw_estimate <= 2.5 * m {
            let zeros = self.registers.iter().filter(|&&r| r == 0).count() as f64;
            if zeros > 0.0 {
                return m * (m / zeros).ln();
            }
        }

        raw_estimate
    }

    pub fn merge(&mut self, other: &HyperLogLog) {
        for (i, &r) in other.registers.iter().enumerate() {
            if i < self.registers.len() && r > self.registers[i] {
                self.registers[i] = r;
            }
        }
    }

    pub fn memory_bytes(&self) -> usize {
        self.registers.len()
    }

    /// Access raw registers for serialization.
    pub fn registers(&self) -> &[u8] {
        &self.registers
    }

    /// Reconstruct from serialized registers (assumes precision 14).
    pub fn from_registers(data: &[u8]) -> Self {
        let precision = match data.len() {
            16 => 4,
            32 => 5,
            64 => 6,
            128 => 7,
            256 => 8,
            512 => 9,
            1024 => 10,
            2048 => 11,
            4096 => 12,
            8192 => 13,
            16384 => 14,
            32768 => 15,
            65536 => 16,
            131072 => 17,
            262144 => 18,
            _ => 14,
        };
        Self {
            registers: data.to_vec(),
            precision,
        }
    }
}

impl Default for HyperLogLog {
    fn default() -> Self {
        Self::new()
    }
}

/// Splitmix64 hash — excellent avalanche for sequential integers.
fn splitmix64(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9e3779b97f4a7c15);
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d049bb133111eb);
    x ^ (x >> 31)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hll_empty() {
        let hll = HyperLogLog::new();
        assert!(hll.estimate() < 1.0);
    }

    #[test]
    fn hll_small_cardinality() {
        let mut hll = HyperLogLog::new();
        for i in 0..100u64 {
            hll.add(i);
        }
        let est = hll.estimate();
        assert!((90.0..110.0).contains(&est), "expected ~100, got {est:.0}");
    }

    #[test]
    fn hll_large_cardinality() {
        let mut hll = HyperLogLog::new();
        for i in 0..100_000u64 {
            hll.add(i);
        }
        let est = hll.estimate();
        let error = (est - 100_000.0).abs() / 100_000.0;
        assert!(error < 0.05, "expected <5% error, got {error:.3}");
    }

    #[test]
    fn hll_duplicates_ignored() {
        let mut hll = HyperLogLog::new();
        for _ in 0..10_000 {
            hll.add(42);
        }
        let est = hll.estimate();
        assert!(est < 5.0, "expected ~1, got {est:.0}");
    }

    #[test]
    fn hll_merge() {
        let mut a = HyperLogLog::new();
        let mut b = HyperLogLog::new();
        for i in 0..5000u64 {
            a.add(i);
        }
        for i in 3000..8000u64 {
            b.add(i);
        }
        a.merge(&b);
        let est = a.estimate();
        let error = (est - 8000.0).abs() / 8000.0;
        assert!(error < 0.05, "expected ~8000, got {est:.0}");
    }

    #[test]
    fn hll_f64_batch() {
        let mut hll = HyperLogLog::new();
        let values: Vec<f64> = (0..1000).map(|i| i as f64 * 0.1).collect();
        hll.add_f64_batch(&values);
        let est = hll.estimate();
        assert!(
            (900.0..1100.0).contains(&est),
            "expected ~1000, got {est:.0}"
        );
    }

    #[test]
    fn hll_memory() {
        let hll = HyperLogLog::new();
        assert_eq!(hll.memory_bytes(), 16384);
    }

    #[test]
    fn hll_msgpack_roundtrip_merge_semantics() {
        let mut a = HyperLogLog::new();
        let mut b = HyperLogLog::new();
        for i in 0..1000u64 {
            a.add(i);
        }
        for i in 500..1500u64 {
            b.add(i);
        }

        let bytes = zerompk::to_msgpack_vec(&a).expect("serialize HLL");
        let mut a_prime: HyperLogLog = zerompk::from_msgpack(&bytes).expect("deserialize HLL");

        // Merge b into deserialized a'.
        a_prime.merge(&b);

        // Merge b into original a.
        a.merge(&b);

        // Both should estimate ~1500 (the union cardinality).
        let est_orig = a.estimate();
        let est_rt = a_prime.estimate();
        // Allow 1% relative difference between the two paths (they are identical state).
        let diff = (est_orig - est_rt).abs() / est_orig.max(1.0);
        assert!(
            diff < 0.01,
            "roundtrip diverged: orig={est_orig:.0}, roundtrip={est_rt:.0}"
        );
    }
}
