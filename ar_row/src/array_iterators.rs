// Copyright (C) 2023-2024 The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information


use arrow::array::*;

/// Like [`arrow::array::iterator::ArrayIter`] for arrays with no nulls.
#[derive(Debug, Clone)]
pub struct NotNullArrayIter<T: ArrayAccessor> {
    array: T,
    index: usize,
}

impl<T: ArrayAccessor> NotNullArrayIter<T> {
    /// Returns `None` if the given array has nulls.
    pub fn new(array: T) -> Option<NotNullArrayIter<T>> {
        if array.nulls().is_some() {
            None
        } else {
            Some(NotNullArrayIter { array, index: 0 })
        }
    }
}

impl<T: ArrayAccessor> Iterator for NotNullArrayIter<T> {
    type Item = T::Item;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.array.len() {
            return None;
        }

        let datum = self.array.value(self.index);

        self.index += 1;

        Some(datum)
    }
}

/// A view over an iterator of values, with an extra iterator telling where to insert
/// None inbetween values of the former.
pub struct NullableValuesIterator<Values: Iterator, Nulls: Iterator<Item=bool>> {
    values: Values,
    nulls: Option<Nulls>,
}

impl<Values: Iterator, Nulls: Iterator<Item=bool>> NullableValuesIterator<Values, Nulls> {
    pub fn new(values: Values, nulls: Option<Nulls>) -> Self {
        NullableValuesIterator { values, nulls }
    }
}
impl<Values: Iterator, Nulls: Iterator<Item=bool>> Iterator for NullableValuesIterator<Values, Nulls> {
    type Item = Option<Values::Item>;

    fn next(&mut self) -> Option<Self::Item> {
        match &mut self.nulls {
            None => match self.values.next() {
                Some(item) => Some(Some(item)),
                None => None,
            },
            Some(ref mut nulls) => match nulls.next() {
                Some(false) => Some(Some(self.values.next().expect("more 'false' bits in nulls() than there are values"))),
                Some(true) => Some(None),
                None => None, // end of iteration
            }
        }
    }
}

impl<Values: ExactSizeIterator, Nulls: ExactSizeIterator<Item=bool>> ExactSizeIterator for NullableValuesIterator<Values, Nulls> {
    fn len(&self) -> usize {
        match &self.nulls {
            None => self.values.len(),
            Some(nulls) => nulls.len(),
        }
    }
}

/*
#[derive(Debug, Clone)]
pub struct ListArrayIter<OffsetSize: OffsetSizeTrait> {
    array: GenericListArray<OffsetSize>,
    index: usize,
}

impl<OffsetSize: OffsetSizeTrait> ListArrayIter<OffsetSize> {
    /// Returns `None` if the given array has nulls.
    pub fn new(array: GenericListArray<OffsetSize>) -> ListArrayIter<OffsetSize> {
        ListArrayIter { array, index: 0 }
    }
}

impl<OffsetSize: OffsetSizeTrait> Iterator for ListArrayIter<OffsetSize> {
    type Item = Arc<dyn Array>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.len() == 0 {
            return None;
        }

        let datum = self.array.value(self.index);

        self.index += 1;

        Some(datum)
    }
}

impl<OffsetSize: OffsetSizeTrait> ExactSizeIterator for ListArrayIter<OffsetSize> {
    fn len(&self) -> usize {
        let num_lists = self.array.value_offsets().len() - 1;
        num_lists.saturating_sub(self.index)
    }
}
*/
