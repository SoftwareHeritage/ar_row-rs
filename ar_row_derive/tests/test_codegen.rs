// Copyright (C) 2023-2024 The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use ar_row::arrow::datatypes::{DataType, Field};
use ar_row::deserialize::{ArRowStruct, CheckableDataType};
use ar_row_derive::ArRowDeserialize;

#[test]
fn test_basic() {
    #[derive(ArRowDeserialize, Clone, Default, Debug, PartialEq)]
    struct Test {
        abc: String,
        def: i64,
    }

    Test::check_datatype(&DataType::Struct(
        vec![
            Field::new("abc", DataType::Utf8, false),
            Field::new("def", DataType::Int64, false),
        ]
        .into(),
    ))
    .unwrap();

    assert_eq!(Test::columns(), vec!["abc", "def"]);
}

#[test]
fn test_raw_literal() {
    #[derive(ArRowDeserialize, Clone, Default, Debug, PartialEq)]
    struct Test {
        r#type: String,
    }

    Test::check_datatype(&DataType::Struct(
        vec![Field::new("type", DataType::Utf8, false)].into(),
    ))
    .unwrap();

    assert_eq!(Test::columns(), vec!["type"]);
}

#[test]
fn test_nested() {
    #[derive(ArRowDeserialize, Clone, Default, Debug, PartialEq)]
    struct Test {
        abc: String,
        def: Inner,
        def2: Vec<Inner>,
    }

    #[derive(ArRowDeserialize, Clone, Default, Debug, PartialEq)]
    struct Inner {
        ghi: i64,
        jkl: Vec<i32>,
    }

    let inner_datatype = DataType::Struct(
        vec![
            Field::new("ghi", DataType::Int64, false),
            Field::new("jkl", DataType::new_list(DataType::Int32, false), false),
        ]
        .into(),
    );
    Test::check_datatype(&DataType::Struct(
        vec![
            Field::new("abc", DataType::Utf8, false),
            Field::new("def", inner_datatype.clone(), false),
            Field::new("def2", DataType::new_list(inner_datatype, false), false),
        ]
        .into(),
    ))
    .unwrap();

    assert_eq!(
        Test::columns(),
        vec!["abc", "def.ghi", "def.jkl", "def2.ghi", "def2.jkl"]
    );
}
