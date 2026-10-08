// SPDX-License-Identifier: Apache-2.0

//! PostgreSQL text and JSON forms of a float, including the non-finite
//! values JSON numbers cannot hold.
//!
//! PostgreSQL renders a non-finite float as `NaN`, `Infinity` or
//! `-Infinity`, and `to_json` renders it as that text in a JSON string.
//! A JSON number cannot hold these values, and `serde_json` turns them into
//! `null`. Every path that renders a float to text or JSON calls these
//! functions instead.

/// The PostgreSQL text of a non-finite float: `NaN`, `Infinity` or
/// `-Infinity`. `None` for a finite float.
pub fn non_finite_float_text(f: f64) -> Option<&'static str> {
    (!f.is_finite()).then(|| non_finite_text(f))
}

/// A float as a JSON value. A finite float is a JSON number. A non-finite
/// float is its PostgreSQL text in a JSON string, as `to_json` renders it.
pub fn float_to_json(f: f64) -> serde_json::Value {
    match serde_json::Number::from_f64(f) {
        Some(n) => serde_json::Value::Number(n),
        // `from_f64` refuses exactly the non-finite floats.
        None => serde_json::Value::String(non_finite_text(f).to_owned()),
    }
}

/// The PostgreSQL text of a float the caller knows is non-finite.
fn non_finite_text(f: f64) -> &'static str {
    if f.is_nan() {
        "NaN"
    } else if f > 0.0 {
        "Infinity"
    } else {
        "-Infinity"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn non_finite_floats_render_postgres_text() {
        assert_eq!(non_finite_float_text(f64::NAN), Some("NaN"));
        assert_eq!(non_finite_float_text(f64::INFINITY), Some("Infinity"));
        assert_eq!(non_finite_float_text(f64::NEG_INFINITY), Some("-Infinity"));
        assert_eq!(non_finite_float_text(1.5), None);
        assert_eq!(non_finite_float_text(f64::MAX), None);
    }

    #[test]
    fn non_finite_floats_are_json_strings() {
        assert_eq!(float_to_json(f64::NAN), serde_json::json!("NaN"));
        assert_eq!(float_to_json(f64::INFINITY), serde_json::json!("Infinity"));
        assert_eq!(
            float_to_json(f64::NEG_INFINITY),
            serde_json::json!("-Infinity")
        );
        assert_eq!(float_to_json(2.5), serde_json::json!(2.5));
    }
}
