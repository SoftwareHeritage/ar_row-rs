// Copyright (C) 2023-2024 The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

extern crate ar_row;
extern crate ar_row_derive;
extern crate datafusion_orc;

use std::fs::File;

use datafusion_orc::projection::ProjectionMask;
use datafusion_orc::{ArrowReader, ArrowReaderBuilder};

use ar_row::arrow::array::RecordBatchReader;
use ar_row::deserialize::{CheckableDataType, DeserializationError, ArRowDeserialize};
use ar_row_derive::ArRowDeserialize;

fn get_reader_builder() -> ArrowReaderBuilder<File> {
    let orc_path = "../test_data//TestOrcFile.testStringAndBinaryStatistics.orc";
    let file = File::open(orc_path).expect("could not open .orc");
    ArrowReaderBuilder::try_new(file).expect("Could not make builder")
}

fn get_reader() -> ArrowReader<File> {
    let builder = get_reader_builder();
    let projection = ProjectionMask::named_roots(
        builder.file_metadata().root_data_type(),
        &["bytes1", "string1"],
    );
    builder.with_projection(projection).build()
}

#[test]
fn test_all_options() {
    #[derive(ArRowDeserialize, Default, Debug, PartialEq)]
    struct Root {
        bytes1: Option<Box<[u8]>>,
        string1: Option<String>,
    }

    let reader = get_reader();
    Root::check_schema(&reader.schema()).unwrap();

    let mut rows: Vec<Root> = Vec::new();

    for batch in reader {
        let new_rows = Root::from_record_batch(batch.unwrap()).unwrap();
        rows.extend(new_rows);
    }

    assert_eq!(
        rows,
        vec![
            Root {
                bytes1: Some(Box::new([0, 1, 2, 3, 4])),
                string1: Some("foo".to_owned())
            },
            Root {
                bytes1: Some(Box::new([0, 1, 2, 3])),
                string1: Some("bar".to_owned())
            },
            Root {
                bytes1: Some(Box::new([0, 1, 2, 3, 4, 5])),
                string1: None
            },
            Root {
                bytes1: None,
                string1: Some("hi".to_owned())
            }
        ]
    );
}

#[test]
fn test_string_no_option() {
    #[derive(ArRowDeserialize, Default, Debug, PartialEq)]
    struct Root {
        bytes1: Option<Box<[u8]>>,
        string1: String,
    }

    let mut reader = get_reader();
    Root::check_schema(&*reader.schema()).unwrap();

    let batch = reader.next().unwrap().unwrap();
    assert_eq!(
        Root::from_record_batch(batch),
        Err(DeserializationError::UnexpectedNull(
            "String column contains nulls".to_owned()
        ))
    );
}
