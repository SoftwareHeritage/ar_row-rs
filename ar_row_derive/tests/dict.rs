// Copyright (C) 2024 The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

extern crate ar_row;
extern crate ar_row_derive;

use std::sync::Arc;

use ar_row::arrow::array::{ArrayRef, DictionaryArray, Int8Array, StructArray};
use ar_row::arrow::datatypes::{DataType, Field, Int8Type};
use ar_row::arrow::record_batch::RecordBatch;

use ar_row::deserialize::ArRowDeserialize;
use ar_row_derive::ArRowDeserialize;

#[test]
fn test_utf8_dict() {
    #[derive(ArRowDeserialize, Clone, Default, Debug, PartialEq, Eq)]
    struct Row {
        value: String,
    }

    let array: DictionaryArray<Int8Type> = vec!["a", "a", "b", "c", "a"].into_iter().collect();
    assert_eq!(
        array.keys(),
        &Int8Array::from(vec![Some(0), Some(0), Some(1), Some(2), Some(0)])
    );
    let batch: RecordBatch = StructArray::from(vec![(
        Arc::new(Field::new(
            "value",
            DataType::Dictionary(DataType::Int8.into(), DataType::Utf8.into()),
            false,
        )),
        Arc::new(array) as ArrayRef,
    )])
    .into();

    let rows: Vec<_> = <Row>::from_record_batch(batch).unwrap();

    assert_eq!(
        rows,
        vec![
            Row {
                value: "a".to_string()
            },
            Row {
                value: "a".to_string()
            },
            Row {
                value: "b".to_string()
            },
            Row {
                value: "c".to_string()
            },
            Row {
                value: "a".to_string()
            },
        ]
    );
}
