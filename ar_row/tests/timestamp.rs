// Copyright (C) 2024 The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::sync::Arc;

use ar_row::arrow::array::{
    ArrayRef, Int64Array, Int64Builder, StructArray, UInt32Array, UInt32Builder,
};
use ar_row::arrow::buffer::{MutableBuffer, NullBuffer};
use ar_row::arrow::datatypes::{DataType, Field};

use ar_row::deserialize::ArRowDeserialize;
use ar_row::Timestamp;

#[test]
fn test_struct_timestamp() {
    let seconds: Int64Array = vec![-1000, -1, 0, 1, 1000].into_iter().collect();
    let nanoseconds: UInt32Array = vec![1, 2, 3, 4, 5].into_iter().collect();
    let array = StructArray::from(vec![
        (
            Arc::new(Field::new("seconds", DataType::Int64.into(), false)),
            Arc::new(seconds) as ArrayRef,
        ),
        (
            Arc::new(Field::new("nanoseconds", DataType::UInt32.into(), false)),
            Arc::new(nanoseconds) as ArrayRef,
        ),
    ]);

    let rows: Vec<_> = <Timestamp>::from_array(Arc::new(array) as Arc<_>).unwrap();

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

#[test]
fn test_opt_struct_timestamp() {
    let mut seconds_builder = Int64Builder::new();
    let mut nanoseconds_builder = UInt32Builder::new();

    for v in [-1000, -1, 0, 1, 1000] {
        seconds_builder.append_null();
        seconds_builder.append_value(v);
    }
    for v in [1, 2, 3, 4, 5] {
        nanoseconds_builder.append_null();
        nanoseconds_builder.append_value(v);
    }

    let fields = vec![
        (Arc::new(Field::new("seconds", DataType::Int64.into(), false))),
        (Arc::new(Field::new("nanoseconds", DataType::UInt32.into(), false))),
    ];

    let array = StructArray::new(
        fields.into(),
        vec![
            Arc::new(seconds_builder.finish()) as _,
            Arc::new(nanoseconds_builder.finish()) as _,
        ],
        Some(NullBuffer::from(vec![
            false, true, false, true, false, true, false, true, false, true,
        ])),
    );

    let rows: Vec<_> = <Option<Timestamp>>::from_array(Arc::new(array) as Arc<_>).unwrap();

    assert_eq!(
        rows,
        vec![
            None,
            Some(Timestamp {
                seconds: -1000,
                nanoseconds: 1
            }),
            None,
            Some(Timestamp {
                seconds: -1,
                nanoseconds: 2
            }),
            None,
            Some(Timestamp {
                seconds: 0,
                nanoseconds: 3
            }),
            None,
            Some(Timestamp {
                seconds: 1,
                nanoseconds: 4
            }),
            None,
            Some(Timestamp {
                seconds: 1000,
                nanoseconds: 5
            })
        ]
    );
}

#[test]
fn test_opt_struct_timestamp_no_nullbuffer() {
    // no nulls within the struct
    let mut seconds_builder = Int64Builder::new_from_buffer(MutableBuffer::default(), None);
    let mut nanoseconds_builder = UInt32Builder::new_from_buffer(MutableBuffer::default(), None);

    for v in [-1000, -1, 0, 1, 1000] {
        seconds_builder.append_value(v);
    }
    for v in [1, 2, 3, 4, 5] {
        nanoseconds_builder.append_value(v);
    }

    let fields = vec![
        (Arc::new(Field::new("seconds", DataType::Int64.into(), false))),
        (Arc::new(Field::new("nanoseconds", DataType::UInt32.into(), false))),
    ];

    let array = StructArray::new(
        fields.into(),
        vec![
            Arc::new(seconds_builder.finish()) as _,
            Arc::new(nanoseconds_builder.finish()) as _,
        ],
        None,
    );

    let rows: Vec<_> = <Option<Timestamp>>::from_array(Arc::new(array) as Arc<_>).unwrap();

    assert_eq!(
        rows,
        vec![
            Some(Timestamp {
                seconds: -1000,
                nanoseconds: 1
            }),
            Some(Timestamp {
                seconds: -1,
                nanoseconds: 2
            }),
            Some(Timestamp {
                seconds: 0,
                nanoseconds: 3
            }),
            Some(Timestamp {
                seconds: 1,
                nanoseconds: 4
            }),
            Some(Timestamp {
                seconds: 1000,
                nanoseconds: 5
            })
        ]
    );
}
