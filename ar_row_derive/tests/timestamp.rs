// Copyright (C) 2023-2024 The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

/* TODO

extern crate ar_row;
extern crate ar_row_derive;
extern crate rust_decimal;
extern crate rust_decimal_macros;

use std::fs::File;

use ar_row::deserialize::{CheckableKind, OrcDeserialize};
use ar_row::reader;
use ar_row::Timestamp;

fn reader_builder() -> ArrowReaderBuilder {
    let orc_path = "../test_data//TestOrcFile.testTimestamp.orc";
    let file = File::open(orc_path).expect("could not open .orc");
    ArrowReaderBuilder::try_new(file).expect("Could not make builder")
}

#[test]
fn test_timestamp() {
    let mut reader = reader_builder().build();
    Timestamp::check_datatype(reader.schema()).unwrap();

    let mut rows: Vec<Timestamp> = Vec::new();

    for batch in reader {
        let new_rows = Timestamp::from_vector_batch(&batch.borrow()).unwrap();
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
*/
