# ar_row-rs

Row-oriented access to Apache Arrow

Currently, it only allows reading arrays, not building them.

Arrow is a column-oriented data storage format designed to be stored in memory.
While a columnar is very efficient, it can be cumbersome to work with, so this
crate provides a work to work on rows by "zipping" columns together into classic
Rust structures.

This crate was forked from [orcxx](https://crates.io/crates/orcxx), an ORC parsing
library, by removing the bindings to the underlying ORC C++ library and rewriting
the high-level API to operate on Arrow instead of ORC-specific structures.

The `ar_row_derive` crate provides a custom `derive` macro.

```rust
extern crate ar_row;
extern crate ar_row_derive;
extern crate datafusion_orc;

use std::fs::File;
use std::num::NonZeroU64;

use datafusion_orc::projection::ProjectionMask;
use datafusion_orc::{ArrowReader, ArrowReaderBuilder};

use ar_row::deserialize::{ArRowDeserialize, ArRowStruct};
use ar_row::row_iterator::RowIterator;
use ar_row_derive::ArRowDeserialize;

// Define structure
#[derive(ArRowDeserialize, Clone, Default, Debug, PartialEq, Eq)]
struct Test1 {
    long1: Option<i64>,
}

// Open file
let orc_path = "../test_data/TestOrcFile.test1.orc";
let file = File::open(orc_path).expect("could not open .orc");
let builder = ArrowReaderBuilder::try_new(file).expect("could not make builder");
let projection = ProjectionMask::named_roots(
    builder.file_metadata().root_data_type(),
    &["long1"],
);
let reader = builder.with_projection(projection).build();
let rows: Vec<Option<Test1>> = reader
    .flat_map(|batch| -> Vec<Option<Test1>> {
        <Option<Test1>>::from_record_batch(batch.unwrap()).unwrap()
    })
    .collect();

assert_eq!(
    rows,
    vec![
        Some(Test1 {
            long1: Some(9223372036854775807)
        }),
        Some(Test1 {
            long1: Some(9223372036854775807)
        })
    ]
);
```

## `RowIterator` API

This API allows reusing the buffer between record batches, but needs `RecordBatch`
instead of `Result<RecordBatch, _>` as input.

<!-- Keep this in sync with ar_row_derive/src/lib.rs -->

```rust
extern crate ar_row;
extern crate ar_row_derive;
extern crate datafusion_orc;

use std::fs::File;
use std::num::NonZeroU64;

use datafusion_orc::projection::ProjectionMask;
use datafusion_orc::{ArrowReader, ArrowReaderBuilder};

use ar_row::deserialize::{ArRowDeserialize, ArRowStruct};
use ar_row::row_iterator::RowIterator;
use ar_row_derive::ArRowDeserialize;

// Define structure
#[derive(ArRowDeserialize, Clone, Default, Debug, PartialEq, Eq)]
struct Test1 {
    long1: Option<i64>,
}

// Open file
let orc_path = "../test_data/TestOrcFile.test1.orc";
let file = File::open(orc_path).expect("could not open .orc");
let builder = ArrowReaderBuilder::try_new(file).expect("could not make builder");
let projection = ProjectionMask::named_roots(
    builder.file_metadata().root_data_type(),
    &["long1"],
);
let reader = builder.with_projection(projection).build();
let mut rows: Vec<Option<Test1>> = RowIterator::new(reader.map(|batch| batch.unwrap()))
    .expect("Could not create iterator")
    .collect();

assert_eq!(
    rows,
    vec![
        Some(Test1 {
            long1: Some(9223372036854775807)
        }),
        Some(Test1 {
            long1: Some(9223372036854775807)
        })
    ]
);
```


## Nested structures

The above two examples also work with nested structures:

```rust
extern crate ar_row;
extern crate ar_row_derive;

use ar_row_derive::ArRowDeserialize;

#[derive(ArRowDeserialize, Default, Debug, PartialEq)]
struct Test1Option {
    boolean1: Option<bool>,
    byte1: Option<i8>,
    short1: Option<i16>,
    int1: Option<i32>,
    long1: Option<i64>,
    float1: Option<f32>,
    double1: Option<f64>,
    bytes1: Option<Box<[u8]>>,
    string1: Option<String>,
    list: Option<Vec<Option<Test1ItemOption>>>,
}

#[derive(ArRowDeserialize, Default, Debug, PartialEq)]
struct Test1ItemOption {
    int1: Option<i32>,
    string1: Option<String>,
}
```
