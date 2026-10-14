// SPDX-License-Identifier: BUSL-1.1

//! Core data types for PromQL evaluation.
//!
//! These mirror the Prometheus data model: samples, series, instant/range
//! vectors, and the JSON response envelope.

use std::collections::BTreeMap;

/// Default lookback delta in milliseconds (5 minutes).
///
/// When evaluating an instant vector selector, a sample is considered
/// "current" if its timestamp is within this window before the evaluation
/// timestamp. Matches Prometheus default.
pub const DEFAULT_LOOKBACK_MS: i64 = 300_000;

/// Ordered label set. BTreeMap ensures deterministic serialization.
pub type Labels = BTreeMap<String, String>;

/// A single timestamped value.
#[derive(Debug, Clone, Copy)]
pub struct Sample {
    pub timestamp_ms: i64,
    pub value: f64,
}

/// A time series with its full label set and collected samples.
#[derive(Debug, Clone)]
pub struct Series {
    pub labels: Labels,
    /// Ascending by timestamp, at most one sample per timestamp. The
    /// selectors binary-search on this order.
    pub samples: Vec<Sample>,
}

impl Series {
    pub fn metric_name(&self) -> &str {
        self.labels.get("__name__").map_or("", |s| s.as_str())
    }
}

/// One element of an instant vector: a label set + single sample.
#[derive(Debug, Clone)]
pub struct InstantSample {
    pub labels: Labels,
    pub value: f64,
    pub timestamp_ms: i64,
}

/// One element of a range vector: a label set + sample window.
#[derive(Debug, Clone)]
pub struct RangeSeries {
    pub labels: Labels,
    pub samples: Vec<Sample>,
}

/// Result of evaluating a PromQL expression.
#[derive(Debug, Clone)]
pub enum Value {
    Scalar(f64, i64),
    Vector(Vec<InstantSample>),
    Matrix(Vec<RangeSeries>),
}

impl Value {
    pub fn result_type(&self) -> &'static str {
        match self {
            Self::Scalar(..) => "scalar",
            Self::Vector(_) => "vector",
            Self::Matrix(_) => "matrix",
        }
    }
}

/// Prometheus-compatible JSON response wrapper.
#[derive(Debug)]
pub struct PromResult {
    pub status: &'static str,
    pub data: Value,
    pub error: Option<String>,
    pub error_type: Option<String>,
}

impl PromResult {
    pub fn success(data: Value) -> Self {
        Self {
            status: "success",
            data,
            error: None,
            error_type: None,
        }
    }

    pub fn error(err_type: &str, message: String) -> Self {
        Self {
            status: "error",
            data: Value::Vector(vec![]),
            error: Some(message),
            error_type: Some(err_type.to_string()),
        }
    }

    /// Serialize to Prometheus-compatible JSON.
    pub fn to_json(&self) -> String {
        let mut out = String::with_capacity(1024);
        out.push_str(r#"{"status":""#);
        out.push_str(self.status);
        out.push('"');

        if let Some(ref e) = self.error {
            out.push_str(r#","errorType":""#);
            out.push_str(self.error_type.as_deref().unwrap_or("internal"));
            out.push_str(r#"","error":""#);
            json_escape(&mut out, e);
            out.push('"');
        }

        out.push_str(r#","data":{"resultType":""#);
        out.push_str(self.data.result_type());
        out.push_str(r#"","result":"#);
        write_value_json(&mut out, &self.data);
        out.push_str("}}");
        out
    }
}

fn write_value_json(out: &mut String, val: &Value) {
    match val {
        Value::Scalar(v, ts) => {
            out.push_str(&format!("[{},\"{}\"]", *ts as f64 / 1000.0, format_f64(*v)));
        }
        Value::Vector(samples) => {
            out.push('[');
            for (i, s) in samples.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                out.push_str(r#"{"metric":"#);
                write_labels_json(out, &s.labels);
                out.push_str(r#","value":["#);
                out.push_str(&format!("{}", s.timestamp_ms as f64 / 1000.0));
                out.push_str(r#",""#);
                out.push_str(&format_f64(s.value));
                out.push_str("\"]}");
            }
            out.push(']');
        }
        Value::Matrix(series) => {
            out.push('[');
            for (i, s) in series.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                out.push_str(r#"{"metric":"#);
                write_labels_json(out, &s.labels);
                out.push_str(r#","values":["#);
                for (j, sample) in s.samples.iter().enumerate() {
                    if j > 0 {
                        out.push(',');
                    }
                    out.push('[');
                    out.push_str(&format!("{}", sample.timestamp_ms as f64 / 1000.0));
                    out.push_str(",\"");
                    out.push_str(&format_f64(sample.value));
                    out.push_str("\"]");
                }
                out.push_str("]}");
            }
            out.push(']');
        }
    }
}

/// Write a JSON object from a label set — public for use by HTTP routes.
pub fn write_labels_json(out: &mut String, labels: &Labels) {
    out.push('{');
    for (i, (k, v)) in labels.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        out.push('"');
        json_escape(out, k);
        out.push_str("\":\"");
        json_escape(out, v);
        out.push('"');
    }
    out.push('}');
}

/// Escape a string for JSON output — public for use by HTTP routes.
pub fn json_escape(out: &mut String, s: &str) {
    for ch in s.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            // JSON forbids every other control character unescaped.
            c if u32::from(c) < 0x20 => out.push_str(&format!("\\u{:04x}", u32::from(c))),
            c => out.push(c),
        }
    }
}

fn format_f64(v: f64) -> String {
    if v.is_nan() {
        "NaN".to_string()
    } else if v.is_infinite() {
        if v > 0.0 { "+Inf" } else { "-Inf" }.to_string()
    } else if v == v.floor() && v.abs() < 1e15 {
        format!("{v:.0}")
    } else {
        format!("{v}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scalar_json() {
        let r = PromResult::success(Value::Scalar(42.0, 1000));
        let json = r.to_json();
        assert!(json.contains(r#""resultType":"scalar""#));
        assert!(json.contains(r#""status":"success""#));
    }

    #[test]
    fn vector_json() {
        let mut labels = Labels::new();
        labels.insert("__name__".into(), "up".into());
        let r = PromResult::success(Value::Vector(vec![InstantSample {
            labels,
            value: 1.0,
            timestamp_ms: 1000,
        }]));
        let json = r.to_json();
        assert!(json.contains(r#""__name__":"up""#));
        assert!(json.contains(r#""1""#)); // value
    }

    #[test]
    fn error_json() {
        let r = PromResult::error("bad_data", "parse error".into());
        let json = r.to_json();
        assert!(json.contains(r#""status":"error""#));
        assert!(json.contains(r#""errorType":"bad_data""#));
    }
}
