// SPDX-License-Identifier: BUSL-1.1

//! Encode a [`ResultSet`] into a pgwire `Response::Query`.

use std::sync::Arc;

use futures::stream;
use pgwire::api::results::{DataRowEncoder, QueryResponse, Response};
use pgwire::error::PgWireResult;

use super::exec::ResultSet;
use super::value::{VType, VValue};
use crate::control::server::pgwire::types::{bool_field, int4_field, int8_field, text_field};

pub fn encode(rs: ResultSet) -> PgWireResult<Vec<Response>> {
    let schema = Arc::new(
        rs.columns
            .iter()
            .map(|c| match c.ty {
                VType::Bool => bool_field(&c.name),
                VType::Int4 => int4_field(&c.name),
                VType::Int8 => int8_field(&c.name),
                VType::Text => text_field(&c.name),
            })
            .collect::<Vec<_>>(),
    );

    let mut out_rows = Vec::with_capacity(rs.rows.len());
    let mut encoder = DataRowEncoder::new(schema.clone());

    for row in &rs.rows {
        for (i, val) in row.iter().enumerate() {
            let ty = rs.columns[i].ty;
            encode_value(&mut encoder, val, ty)?;
        }
        out_rows.push(Ok(encoder.take_row()));
    }

    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(out_rows),
    ))])
}

fn encode_value(encoder: &mut DataRowEncoder, val: &VValue, ty: VType) -> PgWireResult<()> {
    match (val, ty) {
        (VValue::Null, VType::Bool) => encoder.encode_field(&Option::<bool>::None),
        (VValue::Null, VType::Int4) => encoder.encode_field(&Option::<i32>::None),
        (VValue::Null, VType::Int8) => encoder.encode_field(&Option::<i64>::None),
        (VValue::Null, VType::Text) => encoder.encode_field(&Option::<&str>::None),
        (VValue::Bool(b), _) => encoder.encode_field(b),
        (VValue::Int4(i), VType::Int8) => encoder.encode_field(&(*i as i64)),
        (VValue::Int4(i), _) => encoder.encode_field(i),
        (VValue::Int8(i), VType::Int4) => encoder.encode_field(&(*i as i32)),
        (VValue::Int8(i), _) => encoder.encode_field(i),
        (VValue::Text(s), _) => encoder.encode_field(&s.as_str()),
        // Arrays are projected as their PostgreSQL text form (`{a,b}`).
        (arr @ VValue::Array(_), _) => {
            encoder.encode_field(&arr.to_pg_text().unwrap_or_default().as_str())
        }
    }
}
