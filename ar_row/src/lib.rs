// Copyright (C) 2023-2024 The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

//! Row-oriented access to Apache Arrow
//!
//! Currently, it only allows reading arrays, not building them.
//!
//! Arrow is a column-oriented data storage format designed to be stored in memory.
//! While a columnar is very efficient, it can be cumbersome to work with, so this
//! crate provides a work to work on rows by "zipping" columns together into classic
//! Rust structures.
//!
//! # Usage principles
//!
//! See the [`ar_row_derive`](https://docs.rs/ar_row_derive) crate, which allows
//! `#[derive(ArRowDeserialize)]` on structures in order to deserialize Arrow arrays into
//! a structure instance for each row.
//! These structures can be deserialized either directly into vectors with
//! [`deserialize::ArRowDeserialize::read_from_array`], or iterated through
//! [`row_iterator::RowIterator`].
//!
//! # Examples
//!
//! See the [`ar_row_derive` documentation](https://docs.rs/ar_row_derive/)

pub extern crate arrow;
extern crate thiserror;

mod array_iterators;
pub mod deserialize;
pub mod dictionaries;
pub mod row_iterator;

//#[cfg(feature = "rust_decimal")]
//extern crate rust_decimal;

/// Timezone-less timestamp
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub struct Timestamp {
    pub seconds: i64,
    pub nanoseconds: i64,
}

/// Scale-less decimal number
///
/// To get a meaningful value, it should be divided by 10^(the schema's scale)
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub struct NaiveDecimal128(pub i128);

/// Days since epoch
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub struct Date(pub i64);
