// SPDX-License-Identifier: BUSL-1.1

//! `AggAccum::finalize` — consume the accumulator and produce the result `Value`.

use super::state::AggAccum;
use nodedb_physical::physical_plan::AggregateSpec;
use nodedb_types::Value;

impl AggAccum {
    /// Consume the accumulator and produce the final `Value`.
    ///
    /// Fails with `EvalError::NumericOverflow` when an exact integer SUM /
    /// AVG total lies outside the `Decimal` range.
    pub(crate) fn finalize(self, agg: &AggregateSpec) -> Result<Value, nodedb_query::EvalError> {
        Ok(match self {
            AggAccum::Count { n } => Value::Integer(n as i64),
            // No contributing value is NULL for both SUM and AVG: SUM has no
            // zero identity in SQL. Integer inputs total exactly.
            AggAccum::SumAvg { sum } => {
                if agg.function == "avg" {
                    sum.avg()?
                } else {
                    sum.sum()?
                }
            }
            AggAccum::SumAvgDistinct { seen } => {
                // A DISTINCT sum is order-independent, so the arbitrary map
                // iteration order gives a deterministic result.
                let mut sum = nodedb_query::ExactSum::new();
                for v in seen.values() {
                    sum.add_value(v);
                }
                if agg.function == "avg_distinct" {
                    sum.avg()?
                } else {
                    sum.sum()?
                }
            }
            AggAccum::Min { best } => best.unwrap_or(Value::Null),
            AggAccum::Max { best } => best.unwrap_or(Value::Null),
            AggAccum::CountDistinct { seen } => Value::Integer(seen.len() as i64),
            AggAccum::Welford { n, mean: _, m2 } => welford_finalize(&agg.function, n, m2),
            AggAccum::Hll { hll } => Value::Integer(hll.estimate().round() as i64),
            AggAccum::TDigest { digest, pct } => {
                let r = digest.quantile(pct);
                if r.is_nan() {
                    Value::Null
                } else {
                    Value::Float(r)
                }
            }
            AggAccum::TopK { ss, k } => {
                let arr: Vec<Value> = ss
                    .top_k()
                    .into_iter()
                    .take(k)
                    .map(|(item, count, error)| {
                        Value::Object(
                            [
                                ("item".to_string(), Value::Integer(item as i64)),
                                ("count".to_string(), Value::Integer(count as i64)),
                                ("error".to_string(), Value::Integer(error as i64)),
                            ]
                            .into_iter()
                            .collect(),
                        )
                    })
                    .collect();
                Value::Array(arr)
            }
            AggAccum::ArrayAgg { values } => Value::Array(values),
            AggAccum::ArrayAggDistinct { values, .. } => {
                Value::Array(values.into_iter().map(|(_, value)| value).collect())
            }
            AggAccum::PercentileCont { mut values, pct } => {
                if values.is_empty() {
                    return Ok(Value::Null);
                }
                values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
                let idx = (pct * (values.len() - 1) as f64).clamp(0.0, (values.len() - 1) as f64);
                let lo = idx.floor() as usize;
                let hi = idx.ceil() as usize;
                let frac = idx - lo as f64;
                Value::Float(values[lo] * (1.0 - frac) + values[hi] * frac)
            }
            AggAccum::StringAgg { parts } => Value::String(parts.join(",")),
        })
    }
}

/// The final value of a Welford accumulator over `n` values with sum of
/// squared deviations `m2`, with PostgreSQL semantics.
///
/// - `stddev`, `stddev_samp`, `variance`, `var_samp`: sample forms, divided
///   by `n - 1`. NULL for fewer than 2 values.
/// - `stddev_pop`, `var_pop`: population forms, divided by `n`. NULL for no
///   value, 0 for one value.
/// - `ts_stddev`: population form, NULL for fewer than 2 values, as the
///   native timeseries scan computes it.
fn welford_finalize(function: &str, n: u64, m2: f64) -> Value {
    let (variance, is_stddev) = match function {
        "stddev_pop" | "var_pop" if n >= 1 => (m2 / n as f64, function == "stddev_pop"),
        "ts_stddev" if n >= 2 => (m2 / n as f64, true),
        "stddev" | "stddev_samp" | "variance" | "var_samp" if n >= 2 => {
            (m2 / (n - 1) as f64, function.starts_with("stddev"))
        }
        _ => return Value::Null,
    };
    Value::Float(if is_stddev { variance.sqrt() } else { variance })
}

#[cfg(test)]
mod tests {
    use super::welford_finalize;
    use nodedb_types::Value;

    /// Values 2, 4, 4, 4, 5, 5, 7, 9: mean 5, sum of squared deviations 32.
    const N: u64 = 8;
    const M2: f64 = 32.0;

    fn float(v: Value) -> f64 {
        match v {
            Value::Float(f) => f,
            other => panic!("expected a float, got {other:?}"),
        }
    }

    #[test]
    fn sample_forms_divide_by_n_minus_one() {
        let sample_variance = 32.0 / 7.0;
        for function in ["variance", "var_samp"] {
            assert_eq!(float(welford_finalize(function, N, M2)), sample_variance);
        }
        for function in ["stddev", "stddev_samp"] {
            assert_eq!(
                float(welford_finalize(function, N, M2)),
                sample_variance.sqrt()
            );
        }
    }

    #[test]
    fn population_forms_divide_by_n() {
        assert_eq!(float(welford_finalize("var_pop", N, M2)), 4.0);
        assert_eq!(float(welford_finalize("stddev_pop", N, M2)), 2.0);
        assert_eq!(float(welford_finalize("ts_stddev", N, M2)), 2.0);
    }

    #[test]
    fn small_inputs_follow_postgres() {
        for function in ["stddev", "stddev_samp", "variance", "var_samp", "ts_stddev"] {
            assert!(matches!(welford_finalize(function, 1, 0.0), Value::Null));
        }
        assert_eq!(float(welford_finalize("stddev_pop", 1, 0.0)), 0.0);
        assert_eq!(float(welford_finalize("var_pop", 1, 0.0)), 0.0);
        for function in ["stddev_pop", "var_pop"] {
            assert!(matches!(welford_finalize(function, 0, 0.0), Value::Null));
        }
    }
}
