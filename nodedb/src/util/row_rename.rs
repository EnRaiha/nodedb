// SPDX-License-Identifier: BUSL-1.1

//! Renaming the columns of a MessagePack result-row array.
//!
//! An aggregate keys each GROUP BY value by its internal row name: the bare
//! column, or `group_{index}` for a computed key. HAVING and ORDER BY read
//! those names. Once the rows are final, an aliased key takes its SELECT-list
//! name, so a derived table or an outer query reads it by that name.

use nodedb_physical::physical_plan::GroupKeySpec;

/// The `(row name, alias)` pairs that give each aliased GROUP BY key its
/// SELECT-list name.
///
/// `aliases` is parallel to `group_by`. A missing entry, a `None` entry, or
/// an alias equal to the row name renames nothing.
pub fn group_key_renames(
    group_by: &[GroupKeySpec],
    aliases: &[Option<String>],
) -> Vec<(String, String)> {
    group_by
        .iter()
        .zip(aliases)
        .filter_map(|(spec, alias)| {
            alias
                .as_ref()
                .filter(|alias| **alias != spec.output_name)
                .map(|alias| (spec.output_name.clone(), alias.clone()))
        })
        .collect()
}

/// `payload`, a MessagePack array of row maps, with every `from` key of each
/// row renamed to its `to`.
///
/// An empty `renames` returns the payload unchanged. A payload that is not
/// an array of maps is an error: an aggregate result always is one.
pub fn rename_row_keys(payload: &[u8], renames: &[(String, String)]) -> crate::Result<Vec<u8>> {
    if renames.is_empty() {
        return Ok(payload.to_vec());
    }
    let decoded = super::bounded_msgpack::read_value(payload).map_err(|e| crate::Error::Codec {
        detail: format!("aggregate result rename decode: {e}"),
    })?;
    let rmpv::Value::Array(rows) = decoded else {
        return Err(crate::Error::Codec {
            detail: "aggregate result rename: the result is not an array of rows".into(),
        });
    };
    let mut renamed = Vec::with_capacity(rows.len());
    for row in rows {
        let rmpv::Value::Map(fields) = row else {
            return Err(crate::Error::Codec {
                detail: "aggregate result rename: a result row is not a map".into(),
            });
        };
        let fields = fields
            .into_iter()
            .map(|(key, value)| {
                let target = key.as_str().and_then(|name| {
                    renames
                        .iter()
                        .find(|(from, _)| from == name)
                        .map(|(_, to)| to.as_str())
                });
                match target {
                    Some(to) => (rmpv::Value::String(to.into()), value),
                    None => (key, value),
                }
            })
            .collect();
        renamed.push(rmpv::Value::Map(fields));
    }
    let mut buf = Vec::with_capacity(payload.len());
    rmpv::encode::write_value(&mut buf, &rmpv::Value::Array(renamed)).map_err(|e| {
        crate::Error::Codec {
            detail: format!("aggregate result rename encode: {e}"),
        }
    })?;
    Ok(buf)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn encode(value: &rmpv::Value) -> Vec<u8> {
        let mut buf = Vec::new();
        rmpv::encode::write_value(&mut buf, value).expect("encode");
        buf
    }

    #[test]
    fn aliased_keys_take_their_select_list_name() {
        let group_by = vec![
            GroupKeySpec::column("station"),
            GroupKeySpec::column("host"),
            GroupKeySpec {
                output_name: "group_2".into(),
                field: None,
                expr: None,
            },
        ];
        let aliases = vec![Some("s".into()), None, Some("b".into())];
        let renames = group_key_renames(&group_by, &aliases);
        assert_eq!(
            renames,
            vec![
                ("station".to_string(), "s".to_string()),
                ("group_2".to_string(), "b".to_string()),
            ]
        );
        let payload = encode(&rmpv::Value::Array(vec![rmpv::Value::Map(vec![
            (rmpv::Value::from("station"), rmpv::Value::from("TMR")),
            (rmpv::Value::from("host"), rmpv::Value::from("h1")),
            (rmpv::Value::from("group_2"), rmpv::Value::from(7)),
            (rmpv::Value::from("n"), rmpv::Value::from(2)),
        ])]));
        let renamed = rename_row_keys(&payload, &renames).expect("rename");
        assert_eq!(
            crate::util::bounded_msgpack::read_value(&renamed).expect("decode"),
            rmpv::Value::Array(vec![rmpv::Value::Map(vec![
                (rmpv::Value::from("s"), rmpv::Value::from("TMR")),
                (rmpv::Value::from("host"), rmpv::Value::from("h1")),
                (rmpv::Value::from("b"), rmpv::Value::from(7)),
                (rmpv::Value::from("n"), rmpv::Value::from(2)),
            ])])
        );
    }

    #[test]
    fn a_self_alias_renames_nothing() {
        let renames = group_key_renames(
            &[GroupKeySpec::column("station")],
            &[Some("station".into())],
        );
        assert!(renames.is_empty());
    }
}
