// Copyright (C) 2024 The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use arrow::array::*;

use crate::deserialize::{ArRowDeserialize, DeserializationError, DeserializationTarget};

/// Decodes non-`Option`s from a
/// [dictionary-encoded](https://arrow.apache.org/docs/format/Columnar.html#dictionary-encoded-layout)
/// array
pub fn read_from_dictionary_array<'a, 'b, T, Item>(
    src: &dyn AnyDictionaryArray,
    mut dst: &'b mut T,
) -> Result<usize, DeserializationError>
where
    Item: 'a + Clone + ArRowDeserialize,
    &'b mut T: DeserializationTarget<'a, Item = Item> + 'b,
{
    if let Some(_) = src.nulls() {
        return Err(DeserializationError::UnexpectedNull(format!(
            "{} column contains nulls",
            std::any::type_name::<Item>(),
        )));
    };
    let deserialized_values = <Item>::from_array(src.values().clone())?;
    for (key, d) in src.normalized_keys().into_iter().zip(dst.iter_mut()) {
        // FIXME: does Rustc eliminate the copy in normalized_keys()?
        // If not, we should match on the key type and iter on that.
        *d = deserialized_values
            .get(key)
            .ok_or_else(|| DeserializationError::DictionaryOverflow {
                key,
                len: deserialized_values.len(),
                data_type: src.data_type().clone(),
            })?
            .clone();
    }
    Ok(src.len())
}

/// Decodes `Option`s from a
/// [dictionary-encoded](https://arrow.apache.org/docs/format/Columnar.html#dictionary-encoded-layout)
/// array
pub fn read_options_from_dictionary_array<'a, 'b, T, Item>(
    src: &dyn AnyDictionaryArray,
    mut dst: &'b mut T,
) -> Result<usize, DeserializationError>
where
    Item: 'a + Clone + ArRowDeserialize,
    Option<Item>: 'a + Clone + ArRowDeserialize,
    &'b mut T: DeserializationTarget<'a, Item = Option<Item>> + 'b,
{
    let deserialized_values = <Item>::from_array(src.values().clone())?;
    match src.nulls() {
        None => read_from_dictionary_array(src, dst),
        Some(nulls) => {
            for ((not_null, key), d) in nulls
                .iter()
                .zip(src.normalized_keys().into_iter())
                .zip(dst.iter_mut())
            {
                // FIXME: does Rustc eliminate the copy in normalized_keys()?
                // If not, we should match on the key type and iter on that.
                if not_null {
                    *d = Some(
                        deserialized_values
                            .get(key)
                            .ok_or_else(|| DeserializationError::DictionaryOverflow {
                                key,
                                len: deserialized_values.len(),
                                data_type: src.data_type().clone(),
                            })?
                            .clone(),
                    );
                } else {
                    *d = None;
                }
            }
            Ok(src.len())
        }
    }
}
