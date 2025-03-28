// Copyright (C) 2023-2024 The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::fs::File;

use ar_row::arrow::array::RecordBatchReader;
use ar_row::arrow::datatypes::{DataType, Field, Schema};
use orc_rust::ArrowReaderBuilder;

use ar_row::deserialize::{ArRowDeserialize, CheckableDataType};
use ar_row::{Date, Timestamp};
use ar_row_derive::ArRowDeserialize;

fn reader_builder(orc_path: &str) -> ArrowReaderBuilder<File> {
    let file = File::open(orc_path).expect("could not open .orc");
    ArrowReaderBuilder::try_new(file).expect("Could not make builder")
}

#[should_panic] // datafusion-orc does not support non-struct root type yet
#[test]
fn test_timestamp() {
    let reader = reader_builder("../test_data/TestOrcFile.testTimestamp.orc").build();
    Timestamp::check_schema(&reader.schema()).unwrap();

    let mut rows: Vec<Timestamp> = Vec::new();

    for batch in reader {
        let new_rows = Timestamp::from_record_batch(batch.unwrap()).unwrap();
        rows.extend(new_rows);
    }

    assert_eq!(
        rows,
        vec![
            Timestamp {
                seconds: 2114380800,
                nanoseconds: 999000
            },
            Timestamp {
                seconds: 1041379200,
                nanoseconds: 222
            },
            Timestamp {
                seconds: 915148800,
                nanoseconds: 999999999
            },
            Timestamp {
                seconds: 788918400,
                nanoseconds: 688888888
            },
            Timestamp {
                seconds: 1009843200,
                nanoseconds: 100000000
            },
            Timestamp {
                seconds: 1267488000,
                nanoseconds: 9001
            },
            Timestamp {
                seconds: 1104537600,
                nanoseconds: 2229
            },
            Timestamp {
                seconds: 1136073600,
                nanoseconds: 900203003
            },
            Timestamp {
                seconds: 1041379200,
                nanoseconds: 800000007
            },
            Timestamp {
                seconds: 838944000,
                nanoseconds: 723100809
            },
            Timestamp {
                seconds: 909964800,
                nanoseconds: 857340643
            },
            Timestamp {
                seconds: 1222905600,
                nanoseconds: 0
            }
        ]
    );
}

#[derive(ArRowDeserialize, Default, Debug, PartialEq, Eq, Clone)]
struct TimeAndDate {
    time: Timestamp,
    date: Date,
}

#[test]
fn test_timestamp_1900() {
    let reader = reader_builder("../test_data/TestOrcFile.testDate1900.orc").build();
    TimeAndDate::check_schema(&reader.schema()).unwrap();

    let mut rows: Vec<TimeAndDate> = Vec::new();

    for batch in reader {
        let new_rows = TimeAndDate::from_record_batch(batch.unwrap()).unwrap();
        rows.extend(new_rows);
    }

    assert_eq!(
        rows[0..3].to_vec(),
        vec![
            TimeAndDate {
                time: Timestamp {
                    seconds: -2198229903,
                    nanoseconds: -900000000
                },
                date: Date(-25209),
            },
            TimeAndDate {
                time: Timestamp {
                    seconds: -2198229903,
                    nanoseconds: -899900000
                },
                date: Date(-25209),
            },
            TimeAndDate {
                time: Timestamp {
                    seconds: -2198229903,
                    nanoseconds: -899800000
                },
                date: Date(-25209),
            },
        ]
    )
}

#[test]
fn test_timestamp_1900_decimal() {
    let schema = Schema::new(vec![
        Field::new("time", DataType::Decimal128(38, 9), false),
        Field::new("date", DataType::Date32, false),
    ]);
    let reader = reader_builder("../test_data/TestOrcFile.testDate1900.orc")
        .with_schema(schema.clone().into())
        .build();
    TimeAndDate::check_schema(&schema).unwrap();

    let mut rows: Vec<TimeAndDate> = Vec::new();

    for batch in reader {
        let new_rows = TimeAndDate::from_record_batch(batch.unwrap()).unwrap();
        rows.extend(new_rows);
    }

    assert_eq!(
        rows[0..3].to_vec(),
        vec![
            TimeAndDate {
                time: Timestamp {
                    seconds: -2198229903,
                    nanoseconds: -900000000
                },
                date: Date(-25209),
            },
            TimeAndDate {
                time: Timestamp {
                    seconds: -2198229903,
                    nanoseconds: -899900000
                },
                date: Date(-25209),
            },
            TimeAndDate {
                time: Timestamp {
                    seconds: -2198229903,
                    nanoseconds: -899800000
                },
                date: Date(-25209),
            },
        ]
    )
}
