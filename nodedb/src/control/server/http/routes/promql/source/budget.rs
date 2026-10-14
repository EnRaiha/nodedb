// SPDX-License-Identifier: BUSL-1.1

//! The stored-row budget of one PromQL request.
//!
//! Each scan asks the Data Plane for one row more than the budget has left.
//! The ceiling bounds what the owning core gathers, and a reply that fills
//! it proves the answer is larger than the budget. The request then fails.
//! It never answers with a truncated set of samples.

use crate::control::promql::PromqlError;

pub(crate) struct SampleBudget {
    limit: usize,
    remaining: usize,
}

impl SampleBudget {
    pub(crate) fn new(limit: usize) -> Self {
        Self {
            limit,
            remaining: limit,
        }
    }

    /// Row ceiling for the next scan: one past the remaining budget.
    pub(crate) fn fetch_limit(&self) -> usize {
        self.remaining.saturating_add(1)
    }

    /// Spend `rows` of the budget. Fails when they do not fit.
    pub(crate) fn charge(&mut self, rows: usize) -> Result<(), PromqlError> {
        if rows > self.remaining {
            return Err(PromqlError::TooManySamples { limit: self.limit });
        }
        self.remaining -= rows;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exactly_the_limit_fits() {
        let mut b = SampleBudget::new(3);
        assert_eq!(b.fetch_limit(), 4);
        b.charge(3).unwrap();
        assert_eq!(b.fetch_limit(), 1);
        b.charge(0).unwrap();
    }

    #[test]
    fn one_past_the_limit_fails() {
        let mut b = SampleBudget::new(3);
        b.charge(2).unwrap();
        assert!(matches!(
            b.charge(2),
            Err(PromqlError::TooManySamples { limit: 3 })
        ));
    }
}
