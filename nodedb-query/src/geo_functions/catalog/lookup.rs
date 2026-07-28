// SPDX-License-Identifier: Apache-2.0

//! Name resolution against the geo function catalog.

use super::spec::{GeoFunctionSpec, GeoReturn};
use super::table::GEO_FUNCTIONS;

/// Resolve any accepted spelling to its catalog entry. Case-insensitive.
///
/// Returns `None` for a name this crate does not own, so callers can fall
/// through to their own function tables.
pub fn lookup(name: &str) -> Option<&'static GeoFunctionSpec> {
    let lower = name.to_ascii_lowercase();
    GEO_FUNCTIONS.iter().find(|spec| spec.matches(&lower))
}

/// Whether a call to `name` produces a geometry.
///
/// The planner uses this to decide whether an expression may appear in
/// geometry position (a spatial predicate's query-geometry argument, or a
/// GEOMETRY column's inserted value).
pub fn returns_geometry(name: &str) -> bool {
    lookup(name).is_some_and(|spec| spec.returns == GeoReturn::Geometry)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn resolves_standard_and_internal_spellings_to_one_entry() {
        let standard = lookup("ST_X").expect("ST_X must resolve");
        let internal = lookup("geo_x").expect("geo_x must resolve");
        assert_eq!(standard.canonical, internal.canonical);
    }

    #[test]
    fn lookup_is_case_insensitive() {
        assert!(lookup("ST_AsText").is_some());
        assert!(lookup("st_astext").is_some());
        assert!(lookup("ST_ASTEXT").is_some());
    }

    #[test]
    fn unknown_name_does_not_resolve() {
        assert!(lookup("st_not_a_function").is_none());
    }

    /// Two rows claiming the same spelling would make resolution
    /// order-dependent and silently shadow one of them.
    #[test]
    fn every_spelling_is_unique_across_the_catalog() {
        let mut seen: HashSet<&str> = HashSet::new();
        for spec in GEO_FUNCTIONS {
            for name in spec.names() {
                assert!(
                    seen.insert(name),
                    "'{name}' is declared by more than one catalog entry"
                );
            }
        }
    }

    /// A row is only reachable through its own names; a canonical repeated in
    /// its own alias list would be a copy-paste slip.
    #[test]
    fn canonical_is_not_repeated_in_aliases() {
        for spec in GEO_FUNCTIONS {
            assert!(
                !spec.aliases.contains(&spec.canonical),
                "'{}' lists itself as an alias",
                spec.canonical
            );
        }
    }

    #[test]
    fn geometry_returning_calls_are_recognized() {
        assert!(returns_geometry("st_geomfromtext"));
        assert!(returns_geometry("ST_Buffer"));
        assert!(returns_geometry("geo_point"));
        assert!(!returns_geometry("st_area"));
        assert!(!returns_geometry("st_dwithin"));
    }
}
