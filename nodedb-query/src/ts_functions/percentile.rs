// SPDX-License-Identifier: Apache-2.0

//! Exact percentile and streaming accumulator.

/// Exact percentile via sorting with linear interpolation between ranks.
///
/// `p` is clamped to `[0.0, 1.0]`. NaN values are excluded.
/// Returns `None` if all values are NaN or the slice is empty.
pub fn ts_percentile_exact(values: &[f64], p: f64) -> Option<f64> {
    let p = p.clamp(0.0, 1.0);
    let mut sorted: Vec<f64> = values.iter().copied().filter(|v| !v.is_nan()).collect();
    if sorted.is_empty() {
        return None;
    }
    sorted.sort_unstable_by(|a, b| crate::numeric_cmp::cmp_f64(*a, *b));

    let n = sorted.len();
    if n == 1 {
        return Some(sorted[0]);
    }

    // Linear interpolation between nearest ranks.
    let rank = p * (n - 1) as f64;
    let lo = rank.floor() as usize;
    let hi = lo + 1;
    if hi >= n {
        return Some(sorted[n - 1]);
    }
    let frac = rank - lo as f64;
    Some(sorted[lo].mul_add(1.0 - frac, sorted[hi] * frac))
}

/// Online percentile accumulator for DataFusion `Accumulator` integration.
///
/// Collects all values, then computes exact percentile on `evaluate()`.
/// For very large datasets the caller should consider switching to
/// `nodedb_types::approx::TDigest`.
#[derive(Debug)]
pub struct TsPercentileAccum {
    values: Vec<f64>,
    percentile: f64,
}

impl TsPercentileAccum {
    pub fn new(percentile: f64) -> Self {
        Self {
            values: Vec::new(),
            percentile,
        }
    }

    pub fn update(&mut self, value: f64) {
        if !value.is_nan() {
            self.values.push(value);
        }
    }

    pub fn update_batch(&mut self, batch: &[f64]) {
        self.values
            .extend(batch.iter().copied().filter(|v| !v.is_nan()));
    }

    pub fn evaluate(&self) -> Option<f64> {
        ts_percentile_exact(&self.values, self.percentile)
    }

    /// Merge another accumulator's values into this one.
    pub fn merge(&mut self, other: &Self) {
        self.values.extend_from_slice(&other.values);
    }

    /// Serialize state as a flat `Vec<f64>` for DataFusion partial aggregation.
    pub fn state(&self) -> Vec<f64> {
        self.values.clone()
    }

    /// Restore state from a flat `Vec<f64>`.
    pub fn merge_state(&mut self, state: &[f64]) {
        self.values.extend_from_slice(state);
    }

    pub fn size(&self) -> usize {
        std::mem::size_of::<Self>() + self.values.capacity() * std::mem::size_of::<f64>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn median() {
        assert!(
            (ts_percentile_exact(&[1.0, 2.0, 3.0, 4.0, 5.0], 0.5).unwrap() - 3.0).abs() < 1e-12
        );
    }

    #[test]
    fn p0_and_p100() {
        let vals = [10.0, 20.0, 30.0];
        assert!((ts_percentile_exact(&vals, 0.0).unwrap() - 10.0).abs() < 1e-12);
        assert!((ts_percentile_exact(&vals, 1.0).unwrap() - 30.0).abs() < 1e-12);
    }

    #[test]
    fn interpolation() {
        // p=0.25 on [0, 10, 20, 30] → rank 0.75 → 0*(1-0.75) + 10*0.75 = 7.5
        let r = ts_percentile_exact(&[0.0, 10.0, 20.0, 30.0], 0.25).unwrap();
        assert!((r - 7.5).abs() < 1e-12);
    }

    #[test]
    fn nan_excluded() {
        assert!((ts_percentile_exact(&[f64::NAN, 5.0, 10.0], 0.5).unwrap() - 7.5).abs() < 1e-12);
    }

    #[test]
    fn empty() {
        assert!(ts_percentile_exact(&[], 0.5).is_none());
    }

    #[test]
    fn single() {
        assert!((ts_percentile_exact(&[42.0], 0.99).unwrap() - 42.0).abs() < 1e-12);
    }

    #[test]
    fn accumulator_merge() {
        let mut a = TsPercentileAccum::new(0.5);
        a.update_batch(&[1.0, 3.0, 5.0]);
        let mut b = TsPercentileAccum::new(0.5);
        b.update_batch(&[2.0, 4.0]);
        a.merge(&b);
        assert!((a.evaluate().unwrap() - 3.0).abs() < 1e-12);
    }
}
