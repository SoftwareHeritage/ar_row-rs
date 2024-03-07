// Copyright (C) 2023 The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

/// Reads ORC file meta-data and constructs [`RowReader`]
pub struct Reader(UniquePtr<ffi::Reader>);

impl Reader {
    pub fn new(input_stream: InputStream) -> OrcResult<Reader> {
        Reader::new_with_options(input_stream, ReaderOptions::default())
    }

    pub fn new_with_options(
        input_stream: InputStream,
        options: ReaderOptions,
    ) -> OrcResult<Reader> {
        ffi::createReader(input_stream.0, &options.0)
            .map_err(OrcError)
            .map(Reader)
    }

    pub fn row_reader(&self, options: &RowReaderOptions) -> OrcResult<RowReader> {
        self.0
            .createRowReader(&options.0)
            .map(RowReader)
            .map_err(OrcError)
    }

    /// Returns the data type of the file being read. This is usually a struct.
    pub fn kind(&self) -> kind::Kind {
        kind::Kind::new_from_orc_type(self.0.getType())
    }

    /// Returns an iterator of [`StripeInformation`]
    pub fn stripes(&self) -> impl Iterator<Item = StripeInformation> + '_ {
        (0..self.0.getNumberOfStripes()).map(move |i| StripeInformation(self.0.getStripe(i)))
    }

    /// Returns the total number of rows in the file
    pub fn row_count(&self) -> u64 {
        self.stripes()
            .map(|stripe| stripe.rows_count())
            .sum::<u64>()
    }
}

/// Options passed to [`Reader::row_reader`]
pub struct RowReaderOptions(UniquePtr<ffi::RowReaderOptions>);

impl Default for RowReaderOptions {
    fn default() -> RowReaderOptions {
        RowReaderOptions(ffi::RowReaderOptions_new())
    }
}

impl RowReaderOptions {
    /// For files that have structs as the top-level object, select the fields
    /// to read by name. By default, all columns are read. This option clears
    /// any previous setting of the selected columns.
    pub fn include_names<I, S>(mut self, names: I) -> RowReaderOptions
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let mut cxx_names = ffi::StringList_new();
        for name in names.into_iter() {
            let_cxx_string!(cxx_name = name.as_ref());
            cxx_names.pin_mut().push_back(&cxx_name);
        }
        self.0.pin_mut().include_names(&cxx_names);
        self
    }
}

impl Clone for RowReaderOptions {
    fn clone(&self) -> RowReaderOptions {
        RowReaderOptions(ffi::RowReaderOptions_copy(&self.0))
    }
}

unsafe impl Send for RowReaderOptions {}
unsafe impl Sync for RowReaderOptions {}

/// Reads rows from ORC files to a raw [`vector::OwnedColumnVectorBatch`]
pub struct RowReader(UniquePtr<ffi::RowReader>);

impl RowReader {
    /// Creates a vector batch, to be passed to [`RowReader::read_into`]
    ///
    /// ``size`` is the number of rows to read at once.
    pub fn row_batch(&mut self, size: u64) -> vector::OwnedColumnVectorBatch {
        vector::OwnedColumnVectorBatch(self.0.createRowBatch(size))
    }

    /// Read the next stripe into the batch, or returns false if there are no
    /// more stripes.
    pub fn read_into(&mut self, batch: &mut vector::OwnedColumnVectorBatch) -> bool {
        self.0.pin_mut().next(batch.0.pin_mut())
    }

    /// Returns the data type being read.
    ///
    /// With the default [`RowReaderOptions`], this is the same as [`Reader::kind`].
    /// Otherwise this is usually a subset [`Reader::kind`].
    pub fn selected_kind(&self) -> kind::Kind {
        kind::Kind::new_from_orc_type(self.0.getSelectedType())
    }

    /// Get the row number of the first row in the previously read batch.
    pub fn get_row_number(&self) -> u64 {
        self.0.getRowNumber()
    }

    /// Seek to a given row.
    pub fn seek_to_row(&mut self, row_number: u64) {
        self.0.pin_mut().seekToRow(row_number)
    }
}

