// SPDX-License-Identifier: BUSL-1.1

//! Resolve the `WITH (primary='vector', vector_field=...)` access-path
//! config for `build_and_persist`. Relocated verbatim from the pgwire
//! `pgwire::ddl::collection::create::build` module (now deleted).

use super::super::super::super::result::DdlError;
use super::build_flags::err;

/// Resolve `PrimaryEngine` + optional `VectorPrimaryConfig` from the
/// WITH-clause `primary=` / `vector_field=` knobs. Validates the
/// vector field exists in the column list and the declared `dim`
/// matches the column's `VECTOR(n)` type when both are present.
pub(super) fn resolve_primary_engine(
    options: &[(String, String)],
    columns: &[(String, String)],
    fields: &[(String, String)],
    collection_type: &nodedb_types::CollectionType,
) -> Result<
    (
        nodedb_types::PrimaryEngine,
        Option<nodedb_types::VectorPrimaryConfig>,
    ),
    DdlError,
> {
    match nodedb_sql::ddl_ast::parse::vector_primary::parse_vector_primary_options_from_kvs(options)
    {
        Ok(Some(mut vp_cfg)) => {
            let col_list: Vec<(String, String)> = if fields.is_empty() {
                columns.to_vec()
            } else {
                fields.to_vec()
            };
            nodedb_sql::ddl_ast::parse::vector_primary::validate_vector_field(&vp_cfg, &col_list)
                .map_err(|e| err("42601", e.to_string()))?;
            nodedb_sql::ddl_ast::parse::vector_primary::validate_payload_indexes(
                &mut vp_cfg,
                &col_list,
            )
            .map_err(|e| err("42601", e.to_string()))?;
            // Infer dim from VECTOR(n) column type when not in WITH clause.
            if let Some((_, type_str)) = col_list
                .iter()
                .find(|(n, _)| n.eq_ignore_ascii_case(&vp_cfg.vector_field))
            {
                let upper_t = type_str.to_uppercase();
                if let Some(inner) = upper_t
                    .strip_prefix("VECTOR(")
                    .and_then(|s| s.strip_suffix(')'))
                    && let Ok(d) = inner.trim().parse::<u32>()
                {
                    if vp_cfg.dim == 0 {
                        vp_cfg.dim = d;
                    } else if vp_cfg.dim != d {
                        return Err(err(
                            "42601",
                            format!(
                                "vector dim mismatch: WITH clause specifies {}, column type VECTOR({}) specifies {}",
                                vp_cfg.dim, d, d
                            ),
                        ));
                    }
                }
            }
            Ok((nodedb_types::PrimaryEngine::Vector, Some(vp_cfg)))
        }
        Ok(None) => Ok((
            nodedb_types::PrimaryEngine::infer_from_collection_type(collection_type),
            None,
        )),
        Err(e) => Err(err("42601", e.to_string())),
    }
}
