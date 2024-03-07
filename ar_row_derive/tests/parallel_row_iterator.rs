/// Tests ar_row::parallel_row_iterator::ParallelRowIterator
extern crate ar_row;
extern crate ar_row_derive;
extern crate rayon;

use std::convert::TryInto;
use std::sync::Arc;

use rayon::iter::{IndexedParallelIterator, ParallelIterator};

use ar_row::parallel_row_iterator::ParallelRowIterator;
use ar_row::reader;
use ar_row::row_iterator::RowIterator;
use ar_row_derive::OrcDeserialize;

#[derive(OrcDeserialize, Clone, Default, Debug, PartialEq)]
struct Row {
    boolean1: bool,
    byte1: i8,
    short1: i16,
    int1: i32,
    long1: i64,
    float1: f32,
    double1: f64,
    bytes1: Vec<u8>,
    string1: String,
    list: Vec<Item>,
}

#[derive(OrcDeserialize, Clone, Default, Debug, PartialEq)]
struct Item {
    int1: i32,
    string1: String,
}
#[test]
fn test_seek() {
    let orc_path = "../ar_row/orc/examples/TestOrcFile.testSeek.orc";
    let input_stream = reader::InputStream::from_local_file(orc_path).expect("Could not open .orc");
    let reader = reader::Reader::new(input_stream).expect("Could not read .orc");

    let seq_rows = RowIterator::<Row>::new(&reader, 10.try_into().unwrap())
        .unwrap()
        .collect::<Vec<_>>();

    let reader = Arc::new(reader);

    assert_eq!(
        seq_rows,
        ParallelRowIterator::<Row>::new(reader.clone(), 10.try_into().unwrap())
            .unwrap()
            .collect::<Vec<_>>(),
    );

    let mut par_rows = Vec::new();
    ParallelRowIterator::<Row>::new(reader, 10.try_into().unwrap())
        .unwrap()
        .collect_into_vec(&mut par_rows);
    assert_eq!(seq_rows, par_rows);
}
