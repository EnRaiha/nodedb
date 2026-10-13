// SPDX-License-Identifier: BUSL-1.1

//! Prometheus remote write/read protobuf message types.
//!
//! Hand-coded prost types matching the Prometheus remote storage protocol:
//! <https://github.com/prometheus/prometheus/blob/main/prompb/remote.proto>
//! <https://github.com/prometheus/prometheus/blob/main/prompb/types.proto>

/// Remote write request: a batch of time series with samples.
#[derive(Clone, PartialEq, prost::Message)]
pub struct WriteRequest {
    #[prost(message, repeated, tag = "1")]
    pub timeseries: Vec<TimeSeries>,
}

/// Remote read request: one or more queries.
#[derive(Clone, PartialEq, prost::Message)]
pub struct ReadRequest {
    #[prost(message, repeated, tag = "1")]
    pub queries: Vec<Query>,
}

/// Remote read response: one result per query.
#[derive(Clone, PartialEq, prost::Message)]
pub struct ReadResponse {
    #[prost(message, repeated, tag = "1")]
    pub results: Vec<QueryResult>,
}

/// A single remote read query.
#[derive(Clone, PartialEq, prost::Message)]
pub struct Query {
    #[prost(int64, tag = "1")]
    pub start_timestamp_ms: i64,
    #[prost(int64, tag = "2")]
    pub end_timestamp_ms: i64,
    #[prost(message, repeated, tag = "3")]
    pub matchers: Vec<LabelMatcher>,
}

/// Result of a single query in a read response.
#[derive(Clone, PartialEq, prost::Message)]
pub struct QueryResult {
    #[prost(message, repeated, tag = "1")]
    pub timeseries: Vec<TimeSeries>,
}

/// A time series: labels + samples + optional exemplars.
#[derive(Clone, PartialEq, prost::Message)]
pub struct TimeSeries {
    #[prost(message, repeated, tag = "1")]
    pub labels: Vec<Label>,
    #[prost(message, repeated, tag = "2")]
    pub samples: Vec<Sample>,
    #[prost(message, repeated, tag = "3")]
    pub exemplars: Vec<Exemplar>,
}

/// A label name-value pair.
#[derive(Clone, PartialEq, prost::Message)]
pub struct Label {
    #[prost(string, tag = "1")]
    pub name: String,
    #[prost(string, tag = "2")]
    pub value: String,
}

/// A single sample: timestamp + value.
#[derive(Clone, PartialEq, prost::Message)]
pub struct Sample {
    #[prost(double, tag = "1")]
    pub value: f64,
    #[prost(int64, tag = "2")]
    pub timestamp: i64, // milliseconds since epoch
}

/// An exemplar: labels + value + timestamp (links metrics to traces).
#[derive(Clone, PartialEq, prost::Message)]
pub struct Exemplar {
    #[prost(message, repeated, tag = "1")]
    pub labels: Vec<Label>,
    #[prost(double, tag = "2")]
    pub value: f64,
    #[prost(int64, tag = "3")]
    pub timestamp: i64,
}

/// Label matcher for remote read queries.
#[derive(Clone, PartialEq, prost::Message)]
pub struct LabelMatcher {
    #[prost(enumeration = "MatchType", tag = "1")]
    pub match_type: i32,
    #[prost(string, tag = "2")]
    pub name: String,
    #[prost(string, tag = "3")]
    pub value: String,
}

/// Label match type.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, prost::Enumeration)]
#[repr(i32)]
pub enum MatchType {
    Eq = 0,
    Neq = 1,
    Re = 2,
    Nre = 3,
}

impl TimeSeries {
    /// Get the `__name__` label value.
    pub fn metric_name(&self) -> &str {
        self.labels
            .iter()
            .find(|l| l.name == "__name__")
            .map_or("", |l| l.value.as_str())
    }

    /// Convert to ILP lines for ingestion into the metric's timeseries
    /// collection.
    ///
    /// Format: `metric,label1=v1,label2=v2 value=X timestampNs`. Names and
    /// values are escaped for line protocol. An empty-valued label is left
    /// out, because Prometheus treats it as absent.
    ///
    /// The whole series is refused when it has no metric name, when a label
    /// would collide with a stored column (`value`, `timestamp`, the
    /// bitemporal system columns), or when a label holds a newline, which
    /// line protocol cannot carry. A single sample is skipped, and counted,
    /// when its value is not finite (line protocol has no NaN or infinity,
    /// and Prometheus sends NaN as a staleness marker) or its timestamp does
    /// not fit in nanoseconds.
    pub fn to_ilp_lines(&self) -> Result<IlpSeries, super::PromqlError> {
        let metric = self.metric_name();
        if metric.is_empty() {
            return Err(unwritable(
                metric,
                "the series has no __name__ label".into(),
            ));
        }
        if metric.contains(['\n', '\r']) {
            return Err(unwritable(metric, "the metric name holds a newline".into()));
        }

        let mut tag_str = String::new();
        for label in &self.labels {
            if label.name == "__name__" || label.name.is_empty() || label.value.is_empty() {
                continue;
            }
            if RESERVED_COLUMNS.contains(&label.name.as_str()) {
                return Err(unwritable(
                    metric,
                    format!("label '{}' collides with a stored column", label.name),
                ));
            }
            if label.name.contains(['\n', '\r']) || label.value.contains(['\n', '\r']) {
                return Err(unwritable(
                    metric,
                    format!("label '{}' holds a newline", label.name),
                ));
            }
            tag_str.push(',');
            escape_into(&mut tag_str, &label.name, true);
            tag_str.push('=');
            escape_into(&mut tag_str, &label.value, true);
        }

        let mut measurement = String::with_capacity(metric.len());
        escape_into(&mut measurement, metric, false);

        let mut out = IlpSeries::default();
        for sample in &self.samples {
            let Some(ts_ns) = sample.timestamp.checked_mul(1_000_000) else {
                out.rejected += 1;
                continue;
            };
            if !sample.value.is_finite() {
                out.rejected += 1;
                continue;
            }
            out.lines.push(format!(
                "{measurement}{tag_str} value={} {ts_ns}",
                sample.value
            ));
        }
        Ok(out)
    }
}

/// The ILP lines of one series and the samples it could not encode.
#[derive(Debug, Default)]
pub struct IlpSeries {
    pub lines: Vec<String>,
    pub rejected: u64,
}

/// Columns every metric collection stores beside its labels.
const RESERVED_COLUMNS: &[&str] = &[
    "value",
    "timestamp",
    nodedb_types::columnar::schema::TS_SYSTEM,
    nodedb_types::columnar::schema::TS_VALID_FROM,
    nodedb_types::columnar::schema::TS_VALID_UNTIL,
];

fn unwritable(metric: &str, detail: String) -> super::PromqlError {
    super::PromqlError::UnwritableSeries {
        metric: metric.to_owned(),
        detail,
    }
}

/// Escape line-protocol delimiters. A measurement escapes `,`, space and
/// `\`; a tag key or value also escapes `=`.
fn escape_into(out: &mut String, raw: &str, escape_equals: bool) {
    for ch in raw.chars() {
        if ch == ',' || ch == ' ' || ch == '\\' || (escape_equals && ch == '=') {
            out.push('\\');
        }
        out.push(ch);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use prost::Message;

    #[test]
    fn write_request_roundtrip() {
        let req = WriteRequest {
            timeseries: vec![TimeSeries {
                labels: vec![
                    Label {
                        name: "__name__".into(),
                        value: "up".into(),
                    },
                    Label {
                        name: "job".into(),
                        value: "api".into(),
                    },
                ],
                samples: vec![Sample {
                    value: 1.0,
                    timestamp: 1000,
                }],
                exemplars: vec![],
            }],
        };

        let mut buf = Vec::new();
        req.encode(&mut buf).unwrap();
        let decoded = WriteRequest::decode(&buf[..]).unwrap();
        assert_eq!(decoded.timeseries.len(), 1);
        assert_eq!(decoded.timeseries[0].metric_name(), "up");
    }

    #[test]
    fn to_ilp_lines() {
        let ts = TimeSeries {
            labels: vec![
                Label {
                    name: "__name__".into(),
                    value: "http_requests".into(),
                },
                Label {
                    name: "method".into(),
                    value: "GET".into(),
                },
            ],
            samples: vec![
                Sample {
                    value: 42.0,
                    timestamp: 1000,
                },
                Sample {
                    value: 43.0,
                    timestamp: 2000,
                },
            ],
            exemplars: vec![],
        };
        let series = ts.to_ilp_lines().unwrap();
        let lines = series.lines;
        assert_eq!(lines.len(), 2);
        assert_eq!(series.rejected, 0);
        assert!(lines[0].starts_with("http_requests,method=GET value=42"));
        assert!(lines[0].ends_with("1000000000"));
    }

    fn series(labels: &[(&str, &str)], samples: &[(f64, i64)]) -> TimeSeries {
        TimeSeries {
            labels: labels
                .iter()
                .map(|(n, v)| Label {
                    name: (*n).into(),
                    value: (*v).into(),
                })
                .collect(),
            samples: samples
                .iter()
                .map(|&(value, timestamp)| Sample { value, timestamp })
                .collect(),
            exemplars: vec![],
        }
    }

    #[test]
    fn escaped_labels_parse_back_unchanged() {
        let ts = series(
            &[("__name__", "req total"), ("path", "/a b,c=d\\e")],
            &[(1.0, 1000)],
        );
        let lines = ts.to_ilp_lines().unwrap().lines;
        let parsed = crate::engine::timeseries::ilp::parse_batch(&lines[0])
            .unwrap()
            .into_lines();
        assert_eq!(parsed[0].measurement, "req total");
        assert_eq!(parsed[0].tags[0].0, "path");
        assert_eq!(parsed[0].tags[0].1, "/a b,c=d\\e");
    }

    #[test]
    fn non_finite_samples_are_skipped_and_counted() {
        let ts = series(
            &[("__name__", "up")],
            &[
                (1.0, 1000),
                (f64::NAN, 2000),
                (f64::INFINITY, 3000),
                (3.0, 4000),
            ],
        );
        let out = ts.to_ilp_lines().unwrap();
        assert_eq!(out.lines.len(), 2);
        assert_eq!(out.rejected, 2);
    }

    #[test]
    fn empty_label_value_is_left_out() {
        let ts = series(&[("__name__", "up"), ("env", "")], &[(1.0, 1000)]);
        let lines = ts.to_ilp_lines().unwrap().lines;
        assert!(lines[0].starts_with("up value=1 "));
    }

    #[test]
    fn unwritable_series_are_refused() {
        for labels in [
            vec![("job", "api")],
            vec![("__name__", "up"), ("value", "x")],
            vec![("__name__", "up"), ("timestamp", "x")],
            vec![("__name__", "up"), ("job", "a\nb")],
        ] {
            let ts = series(&labels, &[(1.0, 1000)]);
            assert!(ts.to_ilp_lines().is_err(), "{labels:?}");
        }
    }

    #[test]
    fn read_request_roundtrip() {
        let req = ReadRequest {
            queries: vec![Query {
                start_timestamp_ms: 0,
                end_timestamp_ms: 10000,
                matchers: vec![LabelMatcher {
                    match_type: MatchType::Eq as i32,
                    name: "__name__".into(),
                    value: "up".into(),
                }],
            }],
        };

        let mut buf = Vec::new();
        req.encode(&mut buf).unwrap();
        let decoded = ReadRequest::decode(&buf[..]).unwrap();
        assert_eq!(decoded.queries.len(), 1);
        assert_eq!(decoded.queries[0].matchers[0].value, "up");
    }
}
