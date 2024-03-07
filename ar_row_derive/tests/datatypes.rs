// Copyright (C) 2023-2024 The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

extern crate ar_row;
extern crate ar_row_derive;
extern crate datafusion_orc;

use std::fs::File;

use ar_row::arrow::array::RecordBatchReader;
use datafusion_orc::projection::ProjectionMask;
use datafusion_orc::ArrowReaderBuilder;

use ar_row::deserialize::CheckableKind;
use ar_row_derive::OrcDeserialize;

fn get_reader_builder() -> ArrowReaderBuilder<File> {
    let orc_path = "../test_data/TestOrcFile.test1.orc";
    let file = File::open(orc_path).expect("could not open .orc");
    ArrowReaderBuilder::try_new(file).expect("Could not make builder")
}

#[derive(OrcDeserialize, Default, Debug, PartialEq, Eq)]
struct Test1IncorrectOrder {
    long1: Option<i64>,
    string1: Option<String>,
    bytes1: Option<Box<[u8]>>,
}

/// Tests when the order of fields in the file is not consistent with the struct's
/// (string1 and bytes1 are swapped)
#[test]
fn incorrect_order() {
    let builder = get_reader_builder();
    let projection = ProjectionMask::named_roots(
        builder.file_metadata().root_data_type(),
        &["long1", "string1", "bytes1"],
    );
    let reader = builder.with_projection(projection).build();
    assert_eq!(
        Test1IncorrectOrder::check_schema(&*reader.schema()),
        Err("Test1IncorrectOrder cannot be decoded:\n\tField #1 must be called string1, not bytes1\n\tField #2 must be called bytes1, not string1".to_string()));
}

#[derive(OrcDeserialize, Default, Debug, PartialEq, Eq)]
struct Test1IncorrectType {
    long1: Option<i64>,
    bytes1: Option<String>,
}

/// Tests when the order of fields in the file is not consistent with the struct's
/// (string1 and bytes1 are swapped)
#[test]
fn incorrect_type() {
    let builder = get_reader_builder();
    let projection = ProjectionMask::named_roots(
        builder.file_metadata().root_data_type(),
        &["long1", "bytes1"],
    );
    let reader = builder.with_projection(projection).build();
    assert_eq!(
        Test1IncorrectType::check_schema(&*reader.schema()),
        Err("Test1IncorrectType cannot be decoded:\n\tField bytes1 cannot be decoded: String must be decoded from Arrow Utf8/LargeUtf8, not Arrow Binary".to_string()));
}
