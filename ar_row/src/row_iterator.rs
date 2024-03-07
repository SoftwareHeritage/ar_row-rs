// Copyright (C) 2023-2024 The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

//! Iterator on ORC rows.
//!
//! Iterator items need to implement [`ArRowDeserialize`] trait; `ar_row_derive` can
//! generate implementations for structures.
//!
//! TODO: write a test for this after we add the write API to arrays
//! (currently it's only indirectly tested in `ar_row_derive`), because all the test
//! files have a structure at the root and we can't use `#[derive(ArRowDeserialize)]`
//! in this crate to implement it.

use arrow::record_batch::RecordBatch;

use deserialize::{ArRowDeserialize, DeserializationError};

/// Iterator on rows of yielded by an iterator of [`RecordBatch`].
///
/// Reading from this may be less efficient than calling
/// [`ArRowDeserialize::read_from_array`] and working on the column array,
/// but provides a more familiar API to work with individual rows.
///
/// # Panics
///
/// next() repeatedly calls [`ArRowDeserialize::read_from_array`] and panics
/// when it returns a [`::deserialize::DeserializationError`].
pub struct RowIterator<R: Iterator<Item = RecordBatch>, T: ArRowDeserialize + Clone> {
    reader: R,
    batch: Vec<T>,

    /// Index in the batch
    index: usize,

    /// Maximum value of the index + 1
    decoded_items: usize,
}

impl<R: Iterator<Item = RecordBatch>, T: ArRowDeserialize + Clone> RowIterator<R, T> {
    /// Returns an iterator on rows from an iterator on [`RecordBatch`]
    ///
    /// Errors are detailed descriptions of format mismatch (as returned by
    /// [`CheckableDataType::check_datatype`](crate::deserialize::CheckableDataType::check_datatype))
    pub fn new(reader: R) -> Result<RowIterator<R, T>, DeserializationError> {
        let mut row_iterator = RowIterator {
            reader,
            batch: Vec::new(),
            index: 0,
            decoded_items: 0, // Will be filled on the first run of next()
        };
        row_iterator.read_batch()?; // Get an early error if the type is incorrect
        Ok(row_iterator)
    }

    fn read_batch(&mut self) -> Result<bool, DeserializationError> {
        self.index = 0;
        match self.reader.next() {
            Some(record_batch) => {
                self.batch.resize(record_batch.num_rows(), T::default());
                self.decoded_items = T::read_from_record_batch(record_batch, &mut self.batch)?;
                Ok(false)
            }
            None => Ok(true),
        }
    }
}

/// # Panics
///
/// next() repeatedly calls [`ArRowDeserialize::read_from_array`] and panics
/// when it returns a [`::deserialize::DeserializationError`].
impl<R: Iterator<Item = RecordBatch>, T: ArRowDeserialize + Clone> Iterator for RowIterator<R, T> {
    type Item = T;

    fn next(&mut self) -> Option<T> {
        // Exhausted the current batch, read the next one.
        if self.index == self.decoded_items {
            let ended = self.read_batch().expect("ArRowDeserialize::read_from_array() call from RowIterator::next() returns a deserialization error");
            if ended {
                return None;
            }
        }

        let item = self.batch.get(self.index);
        self.index += 1;

        item.cloned()
    }
}
