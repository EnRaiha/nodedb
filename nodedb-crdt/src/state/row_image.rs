// SPDX-License-Identifier: Apache-2.0

//! The state of one row that a scalar write can change, captured before the
//! write and put back when the write is abandoned.

use loro::{LoroMap, LoroValue, ValueOrContainer};

use crate::error::{CrdtError, Result};

use super::core::CrdtState;

/// One row as `upsert` and `set_fields` see it.
///
/// Both writes change only scalar fields. They refuse a field held by a
/// nested container. So the scalar fields are the whole pre-image that such a
/// write has to put back.
#[derive(Debug, Clone, PartialEq)]
pub enum RowImage {
    /// The collection held no entry under the row id.
    Absent,
    /// The row id held a plain value.
    Value(LoroValue),
    /// The row id held a map with these scalar fields. Container-valued
    /// fields are left out: a scalar write does not change them.
    Fields(Vec<(String, LoroValue)>),
}

fn loro_error(error: loro::LoroError) -> CrdtError {
    CrdtError::Loro(error.to_string())
}

/// The scalar fields of `row`, in key order.
fn scalar_fields(row: &LoroMap) -> Vec<(String, LoroValue)> {
    row.keys()
        .filter_map(|key| match row.get(&key) {
            Some(ValueOrContainer::Value(value)) => Some((key.to_string(), value)),
            _ => None,
        })
        .collect()
}

impl CrdtState {
    /// Capture the state of `row_id` that a scalar write can change.
    ///
    /// A row held by a non-map container is refused with `NonMapRowValue`.
    /// A scalar write replaces that container with a map, and no row image
    /// can put the container back.
    pub fn row_image(&self, collection: &str, row_id: &str) -> Result<RowImage> {
        let coll = self.doc.get_map(collection);
        match coll.get(row_id) {
            None => Ok(RowImage::Absent),
            Some(ValueOrContainer::Value(value)) => Ok(RowImage::Value(value)),
            Some(ValueOrContainer::Container(loro::Container::Map(row))) => {
                Ok(RowImage::Fields(scalar_fields(&row)))
            }
            Some(ValueOrContainer::Container(other)) => Err(CrdtError::NonMapRowValue {
                collection: collection.to_string(),
                row_id: row_id.to_string(),
                value: format!("a {:?} container", other.get_type()),
            }),
        }
    }

    /// Put `row_id` back to `image` with new operations.
    ///
    /// Container-valued fields keep their state. A `Fields` image needs the
    /// row to still be a map: a scalar write keeps the row's map, so any
    /// other row shape is refused with `NonMapRowValue`.
    pub fn restore_row_image(
        &self,
        collection: &str,
        row_id: &str,
        image: &RowImage,
    ) -> Result<()> {
        let coll = self.doc.get_map(collection);
        match image {
            RowImage::Absent => {
                if coll.get(row_id).is_some() {
                    coll.delete(row_id).map_err(loro_error)?;
                }
                Ok(())
            }
            RowImage::Value(value) => coll.insert(row_id, value.clone()).map_err(loro_error),
            RowImage::Fields(fields) => {
                let row = match coll.get(row_id) {
                    Some(ValueOrContainer::Container(loro::Container::Map(row))) => row,
                    other => {
                        return Err(CrdtError::NonMapRowValue {
                            collection: collection.to_string(),
                            row_id: row_id.to_string(),
                            value: match other {
                                None => "nothing".to_string(),
                                Some(ValueOrContainer::Value(value)) => {
                                    format!("the scalar {value:?}")
                                }
                                Some(ValueOrContainer::Container(container)) => {
                                    format!("a {:?} container", container.get_type())
                                }
                            },
                        });
                    }
                };
                for (key, _) in scalar_fields(&row) {
                    if !fields.iter().any(|(field, _)| *field == key) {
                        row.delete(&key).map_err(loro_error)?;
                    }
                }
                for (field, value) in fields {
                    match row.get(field) {
                        Some(ValueOrContainer::Value(current)) if current == *value => {}
                        _ => {
                            row.insert(field, value.clone()).map_err(loro_error)?;
                        }
                    }
                }
                Ok(())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn string(value: &str) -> LoroValue {
        LoroValue::String(value.to_string().into())
    }

    #[test]
    fn an_absent_row_is_removed_again() {
        let state = CrdtState::new(1).expect("state");
        let image = state.row_image("c", "r").expect("image");
        assert_eq!(image, RowImage::Absent);
        state
            .upsert("c", "r", &[("a", string("x"))])
            .expect("upsert");
        state.restore_row_image("c", "r", &image).expect("restore");
        assert!(state.read_row("c", "r").is_none());
    }

    #[test]
    fn a_replaced_row_gets_its_scalar_fields_back() {
        let state = CrdtState::new(1).expect("state");
        state
            .upsert("c", "r", &[("a", string("x")), ("b", string("y"))])
            .expect("seed");
        let before = state.read_row("c", "r");
        let image = state.row_image("c", "r").expect("image");
        state
            .upsert("c", "r", &[("a", string("z")), ("n", string("new"))])
            .expect("upsert");
        state.restore_row_image("c", "r", &image).expect("restore");
        assert_eq!(state.read_row("c", "r"), before);
    }

    #[test]
    fn a_partial_write_is_put_back() {
        let state = CrdtState::new(1).expect("state");
        state.upsert("c", "r", &[("a", string("x"))]).expect("seed");
        let before = state.read_row("c", "r");
        let image = state.row_image("c", "r").expect("image");
        state
            .set_fields("c", "r", &[("a", string("y")), ("b", string("z"))])
            .expect("set");
        state.restore_row_image("c", "r", &image).expect("restore");
        assert_eq!(state.read_row("c", "r"), before);
    }

    #[test]
    fn a_container_field_keeps_its_state() {
        let state = CrdtState::new(1).expect("state");
        state.upsert("c", "r", &[("a", string("x"))]).expect("seed");
        state
            .list_insert_fields("c", "r", "blocks", 0, &[("t".to_string(), string("b0"))])
            .expect("block");
        let image = state.row_image("c", "r").expect("image");
        assert_eq!(
            image,
            RowImage::Fields(vec![("a".to_string(), string("x"))])
        );
        state
            .upsert("c", "r", &[("a", string("y"))])
            .expect("upsert");
        state.restore_row_image("c", "r", &image).expect("restore");
        assert_eq!(state.read_field("c", "r", "a"), Some(string("x")));
        assert_eq!(state.list_length("c", "r", "blocks").expect("length"), 1);
    }
}
