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

pub use arrow;

mod array_iterators;
pub mod deserialize;
pub mod dictionaries;
pub mod row_iterator;

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

/// Array wrapper that implements [`Default`]
#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct FixedSizeBinary<const N: usize>(pub [u8; N]);

impl<const N: usize> Default for FixedSizeBinary<N> {
    fn default() -> Self {
        FixedSizeBinary([0; N])
    }
}

impl<const N: usize> From<[u8; N]> for FixedSizeBinary<N> {
    fn from(value: [u8; N]) -> Self {
        FixedSizeBinary(value)
    }
}

impl<const N: usize> From<FixedSizeBinary<N>> for [u8; N] {
    fn from(value: FixedSizeBinary<N>) -> Self {
        value.0
    }
}

impl<const N: usize> std::ops::Deref for FixedSizeBinary<N> {
    type Target = [u8; N];
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<const N: usize> std::ops::DerefMut for FixedSizeBinary<N> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
