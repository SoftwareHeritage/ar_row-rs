// Copyright (C) 2023 The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::convert::TryInto;
use std::iter;

use json::JsonValue;

use structured_reader::ColumnTree;

fn map_nullable_json_values<V, C: Iterator<Item = Option<V>>, F>(column: C, f: F) -> Vec<JsonValue>
where
    F: Fn(V) -> JsonValue,
{
    column
        .map(|v| match v {
            None => JsonValue::Null,
            Some(v) => f(v),
        })
        .collect()
}

pub fn columntree_to_json_rows<'a>(tree: ColumnTree<'a>) -> Vec<JsonValue> {
    match tree {
        ColumnTree::Boolean(column) => {
            map_nullable_json_values(column.iter(), |b| JsonValue::Boolean(b != 0))
        }
        ColumnTree::Byte(column)
        | ColumnTree::Short(column)
        | ColumnTree::Int(column)
        | ColumnTree::Long(column) => {
            map_nullable_json_values(column.iter(), |b| JsonValue::Number(b.into()))
        }
        ColumnTree::Float(column) | ColumnTree::Double(column) => {
            map_nullable_json_values(column.iter(), |b| JsonValue::Number(b.into()))
        }
        ColumnTree::String(column) => map_nullable_json_values(column.iter(), |s| {
            JsonValue::String(String::from_utf8_lossy(s).into_owned())
        }),
        ColumnTree::Binary(column) => map_nullable_json_values(column.iter(), |s| {
            JsonValue::Array(
                s.into_iter()
                    .map(|&byte| JsonValue::Number(byte.into()))
                    .collect(),
            )
        }),
        ColumnTree::Struct {
            not_null,
            num_elements,
            elements,
        } => {
            if let Some(not_null) = not_null {
                assert_eq!(num_elements, not_null.len() as u64);
            }
            let num_fields = elements.len();
            let num_not_null_elements = match not_null {
                None => num_elements,
                Some(not_null) => not_null
                    .iter()
                    .filter(|&&b| b != 0)
                    .count()
                    .try_into()
                    .expect("Could not convert usize to u64"),
            };

            let mut objects: Vec<_> = (0..num_not_null_elements)
                .map(|_| json::object::Object::with_capacity(num_fields))
                .collect();

            for (field_name, subtree) in elements.into_iter() {
                for (subvalue, object) in iter::zip(
                    columntree_to_json_rows(subtree).into_iter(),
                    objects.iter_mut(),
                ) {
                    object.insert(&field_name, subvalue);
                }
            }

            match not_null {
                None => objects.into_iter().map(JsonValue::Object).collect(),
                Some(not_null) => {
                    let mut values = Vec::with_capacity(not_null.len());
                    let mut objects_iter = objects.into_iter();
                    for &b in not_null {
                        if b == 0 {
                            values.push(JsonValue::Null);
                        } else {
                            values.push(JsonValue::Object(
                                objects_iter
                                    .next()
                                    .expect("Struct field vector unexpectedly too short"),
                            ));
                        }
                    }

                    assert_eq!(
                        objects_iter.next(),
                        None,
                        "Struct field vector unexpectedly too long"
                    );
                    values
                }
            }
        }
        ColumnTree::List {
            mut offsets,
            elements,
        } => {
            let values = columntree_to_json_rows(*elements);
            let mut arrays: Vec<Option<Vec<_>>> = Vec::new(); // TODO: try to guess the capacity

            let mut next_split = None;
            loop {
                let offset = offsets.next();
                match offset {
                    // Vector only contains nulls (or is empty)
                    None => break,
                    // First values in the vector are nulls
                    Some(None) => arrays.push(None),
                    // First non-null value in the vector
                    Some(Some(first_split)) => {
                        next_split = Some(first_split as usize);
                        break;
                    }
                }
            }
            for (i, value) in values.into_iter().enumerate() {
                while Some(i) == next_split {
                    let offset = offsets.next();
                    match offset {
                        // Last list of vector
                        None => {
                            arrays.push(Some(Vec::new()));
                            next_split = None
                        }
                        // New null value
                        Some(None) => arrays.push(None),
                        // New list value
                        Some(Some(j)) => {
                            arrays.push(Some(Vec::new()));
                            next_split = Some(j as usize);
                        }
                    }
                }
                arrays.last_mut().unwrap().as_mut().unwrap().push(value);
            }

            // Fill nulls at the end
            while let Some(_) = next_split {
                arrays.push(None);
                next_split = offsets.next().unwrap_or(None).map(|offset| offset as usize);
            }

            arrays
                .into_iter()
                .map(|v| match v {
                    Some(vec) => JsonValue::Array(vec),
                    None => JsonValue::Null,
                })
                .collect()
        }
        ColumnTree::Map {
            mut offsets,
            keys,
            elements,
        } => {
            let keys = columntree_to_json_rows(*keys);
            let values = columntree_to_json_rows(*elements);
            let mut maps: Vec<_> = Vec::new(); // TODO: try to guess the capacity

            let mut next_split = None;
            loop {
                let offset = offsets.next();
                match offset {
                    // Vector only contains nulls (or is empty)
                    None => break,
                    // First values in the vector are nulls
                    Some(None) => maps.push(None),
                    // First non-null value in the vector
                    Some(Some(first_split)) => {
                        next_split = Some(first_split as usize);
                        break;
                    }
                }
            }
            for (i, (key, value)) in iter::zip(keys.into_iter(), values.into_iter()).enumerate() {
                while Some(i) == next_split {
                    let offset = offsets.next();
                    match offset {
                        // Last map of vector
                        None => {
                            maps.push(Some(Vec::new()));
                            next_split = None
                        }
                        // New null value
                        Some(None) => maps.push(None),
                        // New map value
                        Some(Some(j)) => {
                            maps.push(Some(Vec::new()));
                            next_split = Some(j as usize);
                        }
                    }
                }
                let mut object = json::object::Object::with_capacity(2);
                object.insert("key", key);
                object.insert("value", value);
                maps.last_mut()
                    .unwrap()
                    .as_mut()
                    .unwrap()
                    .push(JsonValue::Object(object));
            }

            // Fill nulls at the end
            while let Some(_) = next_split {
                maps.push(None);
                next_split = offsets.next().unwrap_or(None).map(|offset| offset as usize);
            }

            maps.into_iter()
                .map(|o| match o {
                    None => JsonValue::Null,
                    Some(o) => JsonValue::Array(o),
                })
                .collect()
        }
        _ => todo!("{:?}", tree),
    }
}
