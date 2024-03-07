// Copyright (C) 2023 The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

//! Rust wrapper for the Apache ORC C++ library.
//!
//! Currently, it only allows reading files, not writing.
//!
//! ORC, short for Optimized Row Columnar, is a column-oriented data storage format.
//! As such, most of the APIs in this library operate on columns, rather than rows.
//! In order to work on rows, readers need to "zip" columns together.
//!
//! # Compiling
//!
//! This crate uses a submodule pointing to an Apache ORC release, builds its C++ part
//! (including vendored protobuf, lz4, zstd, ...), and links against that,
//! unless the `ORC_USE_SYSTEM_LIBRARIES` environment variable is set.
//! If it is, you need to make sure the dependencies are installed
//! (`apt-get install libprotoc-dev liblz4-dev libsnappy-dev libzstd-dev zlib1g-dev`
//! on Debian-based distributions).
//!
//! # Usage principles
//!
//! [`reader`] contains the entry points to parse a file, and reads into a
//! [`OwnedColumnVectorBatch`](vector::OwnedColumnVectorBatch) structure, which can be
//! `.borrow()`ed to get a [`dyn Array`](vector::BorrowedColumnVectorBatch),
//! which implements most of the operations.
//!
//! This structure is untyped, and needs to be cast into the correct type, by calling
//! [`try_into_longs()`](dyn Array::try_into_longs),
//! [`try_into_strings()`](dyn Array::try_into_strings),
//! [`try_into_structs()`](dyn Array::try_into_structs), etc.
//!
//! While this works when parsing files whose structure is known, this is not very
//! practical. The [`StructuredRowReader`](structured_reader::StructuredRowReader) offers
//! an abstraction over [`RowReader`](reader::RowReader), which reads the schema of the
//! file (through [`selected_kind()`](reader::RowReader::selected_kind)) and dynamically
//! casts the vectors into the right type, recursively, in a
//! [`ColumnTree`](structured_reader::ColumnTree).
//!
//! For row-oriented access, see the [`ar_row_derive`](https://docs.rs/ar_row_derive) crate, which allows
//! `#[derive(OrcDeserialize)]` on structures in order to deserialize ORC files into
//! a structure instance for each row.
//! These structures can be deserialized either directly into vector batches with
//! [`deserialize::OrcDeserialize::read_from_vector_batch`], or iterated through
//! [`row_iterator::RowIterator`].
//!
//! # Panics
//!
//! May panic when requesting vector batches larger than `isize`;
//! this includes vector batches for variable-sized columns (maps and lists).
//! This is unlikely to happen on 64-bits machines (they would OOM first).
//!
//! [`row_iterator::RowIterator`] panics when underlying calls to
//! [`deserialize::OrcDeserialize::read_from_vector_batch`] error (so you may want to
//! avoid the former when working with non-trusted data).
//!
//! Panics may happen when the C++ library doesn't behave as expected, too.
//! C++ exceptions should be converted to Rust [`Result`]s, though.
//!
//! # Examples
//!
//! See the [`ar_row_derive` documentation](https://docs.rs/ar_row_derive/)

pub extern crate arrow;
extern crate thiserror;

mod array_iterators;
pub mod deserialize;
pub mod row_iterator;

extern crate rust_decimal;

/// ORC timestamp (timezone-less)
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub struct Timestamp {
    pub seconds: i64,
    pub nanoseconds: i64,
}
