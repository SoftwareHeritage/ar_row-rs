// Copyright (C) 2023-2024 The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

//! Iterator on ORC rows.
//!
//! Iterator items need to implement [`OrcDeserialize`] trait; `ar_row_derive` can
//! generate implementations for structures.
//!
//! TODO: write a test for this after we add the write API to vector batches
//! (currently it's only indirectly tested in `ar_row_derive`), because all the test
//! files have a structure at the root and we can't use `#[derive(OrcDeserialize)]`
//! in this crate to implement it.

use arrow::record_batch::RecordBatch;

use deserialize::{DeserializationError, OrcDeserialize};

/// Iterator on rows of the given [`RowReader`].
///
/// Reading from this may be less efficient than calling
/// [`OrcDeserialize::read_from_vector_batch`] and working on the column vector,
/// but provides a more familiar API to work with individual rows.
///
/// # Panics
///
/// next() repeatedly calls [`OrcDeserialize::read_from_vector_batch`] and panics
/// when it returns a [`::deserialize::DeserializationError`].
pub struct RowIterator<R: Iterator<Item = RecordBatch>, T: OrcDeserialize + Clone> {
    reader: R,
    batch: Vec<T>,

    /// Index in the batch
    index: usize,

    /// Maximum value of the index + 1
    decoded_items: usize,
}

impl<R: Iterator<Item = RecordBatch>, T: OrcDeserialize + Clone> RowIterator<R, T> {
    /// Returns an iterator on rows of the given [`Reader`].
    ///
    /// This calls [`RowIterator::new_with_options`] with default options and
    /// includes only the needed columns (see [`RowReaderOptions::include_names`]).
    ///
    /// Errors are either detailed descriptions of format mismatch (as returned by
    /// [`CheckableKind::check_datatype`], or C++ exceptions.
    ///
    /// # Panics
    ///
    /// When `batch_size` is larger than `usize`.
    pub fn new(
        reader: R,
    ) -> Result<RowIterator<R, T>, DeserializationError> {
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
/// next() repeatedly calls [`OrcDeserialize::read_from_vector_batch`] and panics
/// when it returns a [`::deserialize::DeserializationError`].
impl<R: Iterator<Item = RecordBatch>, T: OrcDeserialize + Clone> Iterator for RowIterator<R, T> {
    type Item = T;

    fn next(&mut self) -> Option<T> {
        // Exhausted the current batch, read the next one.
        if self.index == self.decoded_items {
            let ended = self.read_batch().expect("OrcDeserialize::read_from_vector_batch() call from RowIterator::next() returns a deserialization error");
            if ended {
                return None;
            }
        }

        let item = self.batch.get(self.index);
        self.index += 1;

        item.cloned()
    }
}
