// Copyright (C) 2024 The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

extern crate ar_row;
extern crate ar_row_derive;

use ar_row::arrow::datatypes::{DataType, Field};
use ar_row::deserialize::{ArRowStruct, CheckableDataType};
use ar_row::FixedSizeBinary;
use ar_row_derive::ArRowDeserialize;

#[test]
fn test_fixedsizebinary() {
    #[derive(ArRowDeserialize, Clone, Default, Debug, PartialEq)]
    struct Test {
        abc: String,
        def: FixedSizeBinary<20>,
    }

    Test::check_datatype(&DataType::Struct(
        vec![
            Field::new("abc", DataType::Utf8, false),
            Field::new("def", DataType::FixedSizeBinary(20), false),
        ]
        .into(),
    ))
    .unwrap();

    assert!(Test::check_datatype(&DataType::Struct(
        vec![
            Field::new("abc", DataType::Utf8, false),
            Field::new("def", DataType::FixedSizeBinary(21), false),
        ]
        .into(),
    ))
    .is_err());

    assert_eq!(Test::columns(), vec!["abc", "def"]);
}

#[test]
fn test_fixedsizebinary_overflow_i32() {
    #[derive(ArRowDeserialize, Clone, Default, Debug, PartialEq)]
    struct Test {
        abc: String,
        def: FixedSizeBinary<{ usize::MAX }>, // does not fit in i32
    }

    assert!(Test::check_datatype(&DataType::Struct(
        vec![
            Field::new("abc", DataType::Utf8, false),
            Field::new("def", DataType::FixedSizeBinary(21), false),
        ]
        .into(),
    ))
    .is_err());
}
