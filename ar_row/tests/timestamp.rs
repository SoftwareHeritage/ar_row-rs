// Copyright (C) 2024 The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::sync::Arc;

use ar_row::arrow::array::{ArrayRef, Int64Array, StructArray, UInt32Array};
use ar_row::arrow::datatypes::{DataType, Field};
use ar_row::arrow::record_batch::RecordBatch;

use ar_row::deserialize::ArRowDeserialize;
use ar_row::Timestamp;

#[test]
fn test_struct_timestamp() {
    let seconds: Int64Array = vec![-1000, -1, 0, 1, 1000].into_iter().collect();
    let nanoseconds: UInt32Array = vec![1, 2, 3, 4, 5].into_iter().collect();
    let batch: RecordBatch = StructArray::from(vec![
        (
            Arc::new(Field::new("seconds", DataType::Int64.into(), false)),
            Arc::new(seconds) as ArrayRef,
        ),
        (
            Arc::new(Field::new("nanoseconds", DataType::UInt32.into(), false)),
            Arc::new(nanoseconds) as ArrayRef,
        ),
    ])
    .into();

    let rows: Vec<_> = <Timestamp>::from_record_batch(batch).unwrap();

    assert_eq!(
        rows,
        vec![
            Timestamp {
                seconds: -1000,
                nanoseconds: 1
            },
            Timestamp {
                seconds: -1,
                nanoseconds: 2
            },
            Timestamp {
                seconds: 0,
                nanoseconds: 3
            },
            Timestamp {
                seconds: 1,
                nanoseconds: 4
            },
            Timestamp {
                seconds: 1000,
                nanoseconds: 5
            }
        ]
    );
}
