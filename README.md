# ar_row-rs

Rust wrapper for the official C++ library for Apache ORC.

It uses a submodule pointing to an Apache ORC release, builds its C++ part
(including vendored protobuf, lz4, zstd, ...), and links against that,
unless the `ORC_USE_SYSTEM_LIBRARIES` environment variable is set.
If it is, you need to make sure the dependencies are installed
(`apt-get install libprotoc-dev liblz4-dev libsnappy-dev libzstd-dev zlib1g-dev`
on Debian-based distributions).

If you have issues when building the crate with linker errors agains libhdfs,
you may try to define the `ORC_DISABLE_HDFS` environment variable.

The `ar_row_derive` crate provides a custom `derive` macro.

## `RowIterator` API

<!-- Keep this in sync with ar_row_derive/src/lib.rs -->

```rust
extern crate ar_row;
extern crate ar_row_derive;
extern crate datafusion_orc;

use std::fs::File;
use std::num::NonZeroU64;

use datafusion_orc::projection::ProjectionMask;
use datafusion_orc::{ArrowReader, ArrowReaderBuilder};

use ar_row::deserialize::{OrcDeserialize, OrcStruct};
use ar_row::row_iterator::RowIterator;
use ar_row_derive::OrcDeserialize;

// Define structure
#[derive(OrcDeserialize, Clone, Default, Debug, PartialEq, Eq)]
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

use ar_row_derive::OrcDeserialize;

#[derive(OrcDeserialize, Default, Debug, PartialEq)]
struct Test1Option {
    boolean1: Option<bool>,
    byte1: Option<i8>,
    short1: Option<i16>,
    int1: Option<i32>,
    long1: Option<i64>,
    float1: Option<f32>,
    double1: Option<f64>,
    bytes1: Option<Vec<u8>>,
    string1: Option<String>,
    list: Option<Vec<Option<Test1ItemOption>>>,
}

#[derive(OrcDeserialize, Default, Debug, PartialEq)]
struct Test1ItemOption {
    int1: Option<i32>,
    string1: Option<String>,
}
```
