// Copyright (C) 2023-2024 The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

//! Helpers for the `ar_row_derive` crate.

#![allow(clippy::redundant_closure_call)]

use std::convert::{TryFrom, TryInto};
use std::sync::Arc;

use arrow::array::*;
use arrow::datatypes::*;
//use rust_decimal::Decimal;
use thiserror::Error;

use std::iter::Map;
use std::num::TryFromIntError;
use std::slice::IterMut;

use crate::array_iterators::{NotNullArrayIter, NullableValuesIterator};
use crate::dictionaries::{read_from_dictionary_array, read_options_from_dictionary_array};
use crate::{Date, FixedSizeBinary, NaiveDecimal128, Timestamp};

const DECIMAL_PRECISION: u8 = 38;
const DECIMAL_SCALE: i8 = 9;
const TIMESTAMP_DECIMAL128_TYPE: DataType = DataType::Decimal128(DECIMAL_PRECISION, DECIMAL_SCALE);

/// Error returned when failing to read a particular batch of data
#[derive(Debug, Error, PartialEq)]
pub enum DeserializationError {
    /// Expected to parse a structure from the Arrow array / record batch, but the
    /// given column is of an incompatible type.
    /// Contains a human-readable description of the type incompatibility
    #[error("Mismatched Arrow column type: {0}")]
    MismatchedColumnDataType(String),
    /// The structure has a field which is missing from the Arrow array / record batch
    /// Contains the name of the field.
    #[error("Field {0} is missing from Arrow array")]
    MissingField(String),
    /// u64 could not be converted to usize. Contains the original error
    #[error("Number of items exceeds maximum buffer capacity on this platform: {0}")]
    UsizeOverflow(TryFromIntError),
    /// [`read_from_array`](ArRowDeserialize::read_from_array) or
    /// [`from_array`](ArRowDeserialize::from_array) orwas called
    /// as a method on a non-`Option` type, with a column containing nulls as parameter.
    ///
    /// Contains a human-readable error.
    #[error("Unexpected null value in Arrow array: {0}")]
    UnexpectedNull(String),
    /// [`read_from_array`](ArRowDeserialize::read_from_array) was given
    /// a `src` column batch longer than its a `dst` vector.
    #[error("Tried to deserialize {src}-long buffer into {dst}-long buffer")]
    MismatchedLength { src: usize, dst: usize },
    /// Tried to deserialized a `FixedSizeBinary` into arrays of the wrong size
    #[error("Tried to deserialize FixedSizeBinary({src}) buffer into arrays of length {dst}")]
    MismatchedBinarySize { src: usize, dst: usize },
    /// Tried to decode from a
    /// [dictionary-encoded](https://arrow.apache.org/docs/format/Columnar.html#dictionary-encoded-layout)
    /// array, but one of the keys has a value larger than the length of the dictionary
    #[error("Could not read entry {key} of a {data_type} dictionary of length {len}")]
    DictionaryOverflow {
        key: usize,
        len: usize,
        data_type: DataType,
    },
    /// Could not convert [`Decimal128Type`] to [`Timestamp`]
    #[error("Could not represent number of seconds ({seconds}) as a 64-bits signed integer")]
    TimestampOverflow { seconds: i128 },
}

fn check_datatype_equals(
    got_datatype: &DataType,
    expected_datatypes: &[DataType],
    type_name: &str,
) -> Result<(), String> {
    if expected_datatypes.contains(got_datatype) {
        Ok(())
    } else {
        Err(format!(
            "{} must be decoded from Arrow {}, not Arrow {:?}",
            type_name,
            expected_datatypes
                .iter()
                .map(|k| format!("{k:?}"))
                .collect::<Vec<_>>()
                .join("/"),
            got_datatype
        ))
    }
}

/// Types which provide a static `check_datatype` method to ensure Arrow arrays can be
/// deserialized into them.
pub trait CheckableDataType {
    /// Returns whether records of the type can be deserialized from
    /// an [`Array`] with the given data type
    ///
    /// This should be called before any method provided by [`ArRowDeserialize`],
    /// to get errors early and with a human-readable error message instead of cast errors
    /// or deserialization into incorrect types (eg. if a file has two fields swapped).
    fn check_datatype(datatype: &DataType) -> Result<(), String>;

    /// Returns whether records of the type can be deserialized from
    /// a [`RecordBatch`] with the given schema
    ///
    /// This should be called before any method provided by [`ArRowDeserialize`],
    /// to get errors early and with a human-readable error message instead of cast errors
    /// or deserialization into incorrect types (eg. if a file has two fields swapped).
    fn check_schema(schema: &Schema) -> Result<(), String> {
        Self::check_datatype(&DataType::Struct(schema.fields().clone()))
    }
}

// Needed because most structs are going to have Option as fields, and code generated by
// ar_row_derive needs to call check_datatype on them recursively.
// This avoid needing to dig into the AST to extract the inner type of the Option.
impl<T: CheckableDataType> CheckableDataType for Option<T> {
    fn check_datatype(datatype: &DataType) -> Result<(), String> {
        T::check_datatype(datatype)
    }
}

/// Types which provide a static `columns` method, which returns the names of all
/// Arrow columns the struct expects to read from.
///
/// Nested field names are separated by dots.
///
/// For scalars, this method simply returns the prefix.
pub trait ArRowStruct {
    fn columns() -> Vec<String> {
        Self::columns_with_prefix("")
    }

    fn columns_with_prefix(prefix: &str) -> Vec<String>;
}

impl<T: ArRowStruct> ArRowStruct for Option<T> {
    fn columns_with_prefix(prefix: &str) -> Vec<String> {
        T::columns_with_prefix(prefix)
    }
}

/// Types which can be read in batch from Arrow's [`Array`].
pub trait ArRowDeserialize: Sized + Default + CheckableDataType {
    /// Reads from a [`Array`] to a structure that behaves like
    /// a rewindable iterator of `&mut Self`, and returns the number of rows written.
    ///
    /// If the number of rows written is strictly smaller than `dst`'s size, then
    /// **elements at the end of the `dst` are left unchanged**.
    ///
    /// Users should call
    /// [`check_schema(record_batch.schema()).unwrap()`](CheckableDataType::check_schema)
    /// before calling this function on a `RecordBatch` (or
    /// [`check_datatype(array.schema()).unwrap()`](CheckableDataType::check_schema)
    /// before calling this function on a `Array` not produced from a `RecordBatch`)
    fn read_from_array<'a, 'b, T>(
        src: impl Array + AsArray,
        dst: &'b mut T,
    ) -> Result<usize, DeserializationError>
    where
        Self: 'a,
        &'b mut T: DeserializationTarget<'a, Item = Self> + 'b;

    /// Wrapper for [`read_from_array`](Self::read_from_array)
    fn read_from_record_batch<'a, 'b, T>(
        src: RecordBatch,
        dst: &'b mut T,
    ) -> Result<usize, DeserializationError>
    where
        Self: 'a,
        &'b mut T: DeserializationTarget<'a, Item = Self> + 'b,
    {
        /// Type helper
        fn f(a: Arc<dyn Array>) -> Arc<dyn Array> {
            a
        }
        let array: StructArray = src.into();
        Self::read_from_array(f(Arc::new(array)), dst)
    }

    /// Reads from a [`Array`] and returns a `Vec<Option<Self>>`
    ///
    /// Users should call
    /// [`check_schema(record_batch.schema()).unwrap()`](CheckableDataType::check_schema)
    /// before calling this function on a `RecordBatch` (or
    /// [`check_datatype(array.schema()).unwrap()`](CheckableDataType::check_schema)
    /// before calling this function on a `Array` not produced from a `RecordBatch`)
    ///
    /// This is a wrapper for
    /// [`read_from_array`](ArRowDeserialize::read_from_array)
    /// which takes care of allocating a buffer, and returns it.
    fn from_array(array: impl Array + AsArray) -> Result<Vec<Self>, DeserializationError> {
        let num_elements = array.len();
        let mut values = Vec::with_capacity(num_elements);
        values.resize_with(num_elements, Default::default);
        Self::read_from_array(array, &mut values)?;
        Ok(values)
    }

    /// Wrapper for [`from_array`](Self::from_array)
    fn from_record_batch(record_batch: RecordBatch) -> Result<Vec<Self>, DeserializationError> {
        /// Type helper
        fn f(a: Arc<dyn Array>) -> Arc<dyn Array> {
            a
        }
        let array: StructArray = record_batch.into();
        Self::from_array(f(Arc::new(array)))
    }
}

macro_rules! impl_scalar {
    ($ty:ty, $datatype:expr, $method:ident, $array_ty:ty) => {
        impl_scalar!($ty, $datatype, $method, $array_ty, |s| Ok(s));
    };
    ($ty:ty, $datatype:expr, $method:ident, $array_ty:ty, $cast:expr) => {
        impl ArRowStruct for $ty {
            fn columns_with_prefix(prefix: &str) -> Vec<String> {
                vec![prefix.to_string()]
            }
        }

        impl CheckableDataType for $ty {
            fn check_datatype(datatype: &DataType) -> Result<(), String> {
                check_datatype_equals(datatype, &$datatype, stringify!($ty))
            }
        }

        impl_scalar_deser!($ty, $datatype, $method, $array_ty, $cast);
    };
}

macro_rules! impl_scalar_deser {
    ($ty:ty, $datatype:expr, $method:ident, $array_ty:ty, $cast:expr) => {
        impl ArRowDeserialize for $ty {
            fn read_from_array<'a, 'b, T>(
                src: impl Array + AsArray,
                mut dst: &'b mut T,
            ) -> Result<usize, DeserializationError>
            where
                &'b mut T: DeserializationTarget<'a, Item = Self> + 'b,
            {
                if let Some(src) = src.$method() {
                    let src: &$array_ty = src;
                    match NotNullArrayIter::new(src) {
                        None => Err(DeserializationError::UnexpectedNull(format!(
                            "{} column contains nulls",
                            stringify!($ty)
                        ))),
                        Some(it) => {
                            let it: NotNullArrayIter<&$array_ty> = it;
                            for (s, d) in it.zip(dst.iter_mut()) {
                                *d = ($cast)(s)?
                            }

                            Ok(src.len())
                        }
                    }
                } else if let Some(src) = src.as_any_dictionary_opt() {
                    read_from_dictionary_array(src, dst)
                } else {
                    Err(DeserializationError::MismatchedColumnDataType(format!(
                        "Could not cast {:?} array with {}",
                        src.data_type(),
                        stringify!($method)
                    )))
                }
            }
        }

        impl ArRowDeserialize for Option<$ty> {
            fn read_from_array<'a, 'b, T>(
                src: impl Array + AsArray,
                mut dst: &'b mut T,
            ) -> Result<usize, DeserializationError>
            where
                &'b mut T: DeserializationTarget<'a, Item = Self> + 'b,
            {
                if let Some(src) = src.$method() {
                    let src: &$array_ty = src;
                    for (s, d) in src.iter().zip(dst.iter_mut()) {
                        match s {
                            None => *d = None,
                            Some(s) => *d = Some(($cast)(s)?),
                        }
                    }

                    Ok(src.len())
                } else if let Some(src) = src.as_any_dictionary_opt() {
                    read_options_from_dictionary_array(src, dst)
                } else {
                    Err(DeserializationError::MismatchedColumnDataType(format!(
                        "Could not cast {:?} array with {}",
                        src.data_type(),
                        stringify!($method)
                    )))
                }
            }
        }
    };
}

impl_scalar!(bool, [DataType::Boolean], as_boolean_opt, BooleanArray);
impl_scalar!(
    i8,
    [DataType::Int8],
    as_primitive_opt,
    PrimitiveArray<Int8Type>
);
impl_scalar!(
    i16,
    [DataType::Int16],
    as_primitive_opt,
    PrimitiveArray<Int16Type>
);
impl_scalar!(
    i32,
    [DataType::Int32],
    as_primitive_opt,
    PrimitiveArray<Int32Type>
);
impl_scalar!(
    i64,
    [DataType::Int64],
    as_primitive_opt,
    PrimitiveArray<Int64Type>
);
impl_scalar!(
    u8,
    [DataType::UInt8],
    as_primitive_opt,
    PrimitiveArray<UInt8Type>
);
impl_scalar!(
    u16,
    [DataType::UInt16],
    as_primitive_opt,
    PrimitiveArray<UInt16Type>
);
impl_scalar!(
    u32,
    [DataType::UInt32],
    as_primitive_opt,
    PrimitiveArray<UInt32Type>
);
impl_scalar!(
    u64,
    [DataType::UInt64],
    as_primitive_opt,
    PrimitiveArray<UInt64Type>
);
impl_scalar!(
    Date,
    [DataType::Date32],
    as_primitive_opt,
    PrimitiveArray<Date32Type>,
    |d: i32| Ok(Date(d.into()))
);
impl_scalar!(
    f32,
    [DataType::Float32],
    as_primitive_opt,
    PrimitiveArray<Float32Type>
);
impl_scalar!(
    f64,
    [DataType::Float64],
    as_primitive_opt,
    PrimitiveArray<Float64Type>
);
impl_scalar!(
    String,
    [DataType::Utf8, DataType::LargeUtf8],
    as_string_opt,
    StringArray,
    |s: &str| Ok(s.to_owned())
);
impl_scalar!(
    Box<[u8]>,
    [DataType::Binary, DataType::LargeBinary],
    as_binary_opt,
    BinaryArray,
    |s: &[u8]| Ok(s.into())
);

impl<const N: usize> ArRowStruct for FixedSizeBinary<N> {
    fn columns_with_prefix(prefix: &str) -> Vec<String> {
        vec![prefix.to_string()]
    }
}

impl<const N: usize> CheckableDataType for FixedSizeBinary<N> {
    fn check_datatype(datatype: &DataType) -> Result<(), String> {
        match datatype {
            DataType::FixedSizeBinary(size) => {
                match i32::try_from(N) {
                    Ok(expected_size) if expected_size == *size => Ok(()),
                    _ => Err(format!(
                    "[u8; {N}] must be decoded from Arrow FixedSizeBinary({N}), not Arrow FixedSizeBinary({size})",
                )),
                }
            },
            _ => Err(format!(
                "[u8; _] must be decoded from Arrow FixedSizeBinary, not Arrow {datatype:?}"
            )),
        }
    }
}

impl<const N: usize> ArRowDeserialize for FixedSizeBinary<N> {
    fn read_from_array<'a, 'b, T>(
        src: impl Array + AsArray,
        mut dst: &'b mut T,
    ) -> Result<usize, DeserializationError>
    where
        &'b mut T: DeserializationTarget<'a, Item = Self> + 'b,
    {
        if let Some(src) = src.as_fixed_size_binary_opt() {
            let src: &FixedSizeBinaryArray = src;
            match NotNullArrayIter::new(src) {
                None => Err(DeserializationError::UnexpectedNull(
                    "[u8; _] column contains nulls".to_string(),
                )),
                Some(it) => {
                    let it: NotNullArrayIter<&FixedSizeBinaryArray> = it;
                    for (s, d) in it.zip(dst.iter_mut()) {
                        *d = FixedSizeBinary(s.try_into().map_err(|_| {
                            DeserializationError::MismatchedBinarySize {
                                src: s.len(),
                                dst: N,
                            }
                        })?)
                    }

                    Ok(src.len())
                }
            }
        } else if let Some(src) = src.as_any_dictionary_opt() {
            read_from_dictionary_array(src, dst)
        } else {
            Err(DeserializationError::MismatchedColumnDataType(format!(
                "Could not cast {:?} array with as_fixed_size_binary_opt",
                src.data_type(),
            )))
        }
    }
}

impl<const N: usize> ArRowDeserialize for Option<FixedSizeBinary<N>> {
    fn read_from_array<'a, 'b, T>(
        src: impl Array + AsArray,
        mut dst: &'b mut T,
    ) -> Result<usize, DeserializationError>
    where
        &'b mut T: DeserializationTarget<'a, Item = Self> + 'b,
    {
        if let Some(src) = src.as_fixed_size_binary_opt() {
            let src: &FixedSizeBinaryArray = src;
            for (s, d) in src.iter().zip(dst.iter_mut()) {
                match s {
                    None => *d = None,
                    Some(s) => {
                        *d = Some(FixedSizeBinary(s.try_into().map_err(|_| {
                            DeserializationError::MismatchedBinarySize {
                                src: s.len(),
                                dst: N,
                            }
                        })?))
                    }
                }
            }

            Ok(src.len())
        } else if let Some(src) = src.as_any_dictionary_opt() {
            read_options_from_dictionary_array(src, dst)
        } else {
            Err(DeserializationError::MismatchedColumnDataType(format!(
                "Could not cast {:?} array with as_fixed_size_binary_opt",
                src.data_type(),
            )))
        }
    }
}

impl ArRowStruct for NaiveDecimal128 {
    fn columns_with_prefix(prefix: &str) -> Vec<String> {
        vec![prefix.to_string()]
    }
}
impl CheckableDataType for NaiveDecimal128 {
    fn check_datatype(datatype: &DataType) -> Result<(), String> {
        match datatype {
            DataType::Decimal128(_, _) => Ok(()),
            _ => Err(format!(
                "NaiveDecimal128 must be decoded from Arrow Decimal128(_, _), not Arrow {datatype:?}"
            )),
        }
    }
}
impl_scalar_deser!(
    NaiveDecimal128,
    [DataType::Decimal128],
    as_primitive_opt,
    PrimitiveArray<Decimal128Type>,
    |v| Ok(NaiveDecimal128(v))
);

impl ArRowStruct for Timestamp {
    fn columns_with_prefix(prefix: &str) -> Vec<String> {
        vec![prefix.to_string()]
    }
}

impl CheckableDataType for Timestamp {
    fn check_datatype(datatype: &DataType) -> Result<(), String> {
        use arrow::datatypes::TimeUnit::*;
        check_datatype_equals(
            datatype,
            &[
                DataType::Timestamp(Second, None),
                DataType::Timestamp(Millisecond, None),
                DataType::Timestamp(Microsecond, None),
                DataType::Timestamp(Nanosecond, None),
                DataType::Decimal128(DECIMAL_PRECISION, DECIMAL_SCALE),
            ],
            "Timestamp",
        )
    }
}

macro_rules! impl_timestamp {
    ($src:expr, $ty:ty, $ratio:expr, $dst:expr) => {{
        if let Some(src) = $src.as_primitive_opt::<$ty>() {
            return match NotNullArrayIter::new(src) {
                None => Err(DeserializationError::UnexpectedNull(format!(
                    "Timestamp column contains nulls",
                ))),
                Some(it) => {
                    for (s, d) in it.zip($dst.iter_mut()) {
                        *d = Timestamp {
                            seconds: s / $ratio,
                            #[allow(clippy::modulo_one)]
                            nanoseconds: (s % $ratio) * (1_000_000_000 / $ratio),
                        }
                    }

                    Ok(src.len())
                }
            };
        }
    }};
}

impl ArRowDeserialize for Timestamp {
    fn read_from_array<'a, 'b, T>(
        src: impl Array + AsArray,
        mut dst: &'b mut T,
    ) -> Result<usize, DeserializationError>
    where
        &'b mut T: DeserializationTarget<'a, Item = Self> + 'b,
    {
        impl_timestamp!(src, TimestampSecondType, 1, dst);
        impl_timestamp!(src, TimestampMillisecondType, 1_000, dst);
        impl_timestamp!(src, TimestampMicrosecondType, 1_000_000, dst);
        impl_timestamp!(src, TimestampNanosecondType, 1_000_000_000, dst);

        if let Some(src) = src.as_primitive_opt::<Decimal128Type>() {
            if *src.data_type() != TIMESTAMP_DECIMAL128_TYPE {
                return Err(DeserializationError::MismatchedColumnDataType(format!(
                    "Timestamp can only be decoded from {:?}, not {:?}",
                    TIMESTAMP_DECIMAL128_TYPE,
                    *src.data_type()
                )));
            }
            return match NotNullArrayIter::new(src) {
                None => Err(DeserializationError::UnexpectedNull(
                    "Timestamp column contains nulls".to_string(),
                )),
                Some(it) => {
                    for (s, d) in it.zip(dst.iter_mut()) {
                        *d = timestamp_from_decimal128(s)?;
                    }

                    Ok(src.len())
                }
            };
        }

        if let Some(src) = src.as_any_dictionary_opt() {
            return read_from_dictionary_array(src, dst);
        }

        Err(DeserializationError::MismatchedColumnDataType(format!(
            "Could not cast {:?} array with as_primitive_opt::<Timestamp*Type>",
            src.data_type(),
        )))
    }
}

macro_rules! impl_timestamp_option {
    ($src:expr, $ty:ty, $ratio:expr, $dst:expr) => {{
        if let Some(src) = $src.as_primitive_opt::<$ty>() {
            for (s, d) in src.iter().zip($dst.iter_mut()) {
                match s {
                    None => *d = None,
                    Some(s) => {
                        *d = Some(Timestamp {
                            seconds: s / $ratio,
                            #[allow(clippy::modulo_one)]
                            nanoseconds: (s % $ratio) * (1_000_000_000 / $ratio),
                        })
                    }
                }
            }
            return Ok(src.len());
        }
    }};
}

impl ArRowDeserialize for Option<Timestamp> {
    fn read_from_array<'a, 'b, T>(
        src: impl Array + AsArray,
        mut dst: &'b mut T,
    ) -> Result<usize, DeserializationError>
    where
        &'b mut T: DeserializationTarget<'a, Item = Self> + 'b,
    {
        impl_timestamp_option!(src, TimestampSecondType, 1, dst);
        impl_timestamp_option!(src, TimestampMillisecondType, 1_000, dst);
        impl_timestamp_option!(src, TimestampMicrosecondType, 1_000_000, dst);
        impl_timestamp_option!(src, TimestampNanosecondType, 1_000_000_000, dst);

        if let Some(src) = src.as_primitive_opt::<Decimal128Type>() {
            if *src.data_type() != TIMESTAMP_DECIMAL128_TYPE {
                return Err(DeserializationError::MismatchedColumnDataType(format!(
                    "Timestamp can only be decoded from {:?}, not {:?}",
                    TIMESTAMP_DECIMAL128_TYPE,
                    *src.data_type()
                )));
            }
            for (s, d) in src.iter().zip(dst.iter_mut()) {
                match s {
                    None => *d = None,
                    Some(s) => *d = Some(timestamp_from_decimal128(s)?),
                }
            }
            return Ok(src.len());
        }

        if let Some(src) = src.as_any_dictionary_opt() {
            return read_options_from_dictionary_array(src, dst);
        }

        Err(DeserializationError::MismatchedColumnDataType(format!(
            "Could not cast {:?} array with {}",
            src.data_type(),
            stringify!($method)
        )))
    }
}

fn timestamp_from_decimal128(s: i128) -> Result<Timestamp, DeserializationError> {
    let dividend = 10u64.pow(DECIMAL_SCALE.try_into().unwrap());
    let seconds = s / i128::from(dividend);
    let nanoseconds = s % i128::from(dividend);
    Ok(Timestamp {
        seconds: i64::try_from(seconds)
            .map_err(|_| DeserializationError::TimestampOverflow { seconds })?,
        nanoseconds: nanoseconds.try_into().unwrap(), // can't overflow, dividend fits in u64
    })
}

/* TODO rust_decimal
impl_scalar!(
    crate::Timestamp,
    [Kind::Timestamp],
    try_into_timestamps,
    |s: (i64, i64)| Ok(crate::Timestamp {
        seconds: s.0,
        nanoseconds: s.1
    })
);

impl ArRowStruct for Decimal {
    fn columns_with_prefix(prefix: &str) -> Vec<String> {
        vec![prefix.to_string()]
    }
}

impl CheckableDataType for Decimal {
    fn check_datatype(datatype: &DataType) -> Result<(), String> {
        match datatype {
            DataType::Decimal { .. } => Ok(()),
            _ => Err(format!(
                "Decimal must be decoded from Arrow Decimal, not Arrow {:?}",
                datatype
            )),
        }
    }
}

impl ArRowDeserialize for Decimal {
    fn read_from_array<'a, 'b, T>(
        src: &(impl Array + AsArray),
        mut dst: &'b mut T,
    ) -> Result<usize, DeserializationError>
    where
        &'b mut T: DeserializationTarget<'a, Item = Self> + 'b,
    {
        // TODO: add support for dictionary encoding?
        match src.try_into_decimals64() {
            Ok(src) => match NotNullArrayIter::new(src) {
                None => {
                    return Err(DeserializationError::UnexpectedNull(
                        "Decimal column contains nulls".to_string(),
                    ))
                }
                Some(it) => {
                    for (s, d) in it.zip(dst.iter_mut()) {
                        *d = s;
                    }
                }
            },
            Err(_) => {
                let src = src
                    .try_into_decimals128()
                    .map_err(DeserializationError::MismatchedColumnDataType)?;
                match NotNullArrayIter::new(src) {
                    None => {
                        return Err(DeserializationError::UnexpectedNull(
                            "Decimal column contains nulls".to_string(),
                        ))
                    }
                    Some(it) => {
                        for (s, d) in it.zip(dst.iter_mut()) {
                            *d = s;
                        }
                    }
                }
            }
        }

        Ok(src.num_elements().try_into().unwrap())
    }
}

impl ArRowDeserialize for Option<Decimal> {
    fn read_from_array<'a, 'b, T>(
        src: &(impl Array + AsArray),
        mut dst: &'b mut T,
    ) -> Result<usize, DeserializationError>
    where
        &'b mut T: DeserializationTarget<'a, Item = Self> + 'b,
    {
        // TODO: add support for dictionary encoding?
        match src.try_into_decimals64() {
            Ok(src) => {
                for (s, d) in src.iter().zip(dst.iter_mut()) {
                    match s {
                        None => *d = None,
                        Some(s) => *d = Some(s),
                    }
                }
            }
            Err(_) => {
                let src = src
                    .try_into_decimals128()
                    .map_err(DeserializationError::MismatchedColumnDataType)?;
                for (s, d) in src.iter().zip(dst.iter_mut()) {
                    match s {
                        None => *d = None,
                        Some(s) => *d = Some(s),
                    }
                }
            }
        }

        Ok(src.num_elements().try_into().unwrap())
    }
}
*/

impl<T: ArRowStruct> ArRowStruct for Vec<T> {
    fn columns_with_prefix(prefix: &str) -> Vec<String> {
        T::columns_with_prefix(prefix)
    }
}

impl<T: CheckableDataType> CheckableDataType for Vec<T> {
    fn check_datatype(datatype: &DataType) -> Result<(), String> {
        match datatype {
            DataType::List(inner) => T::check_datatype(inner.data_type()),
            _ => Err(format!("Must be a List, not {datatype:?}")),
        }
    }
}

/// Shared initialization code of `impl<I> ArRowDeserializeOption for Vec<I>`
/// and impl<I> ArRowDeserialize for Vec<I>
macro_rules! init_list_read {
    ($src:expr, $dst: expr) => {{
        let src = $src;

        let values: &Arc<_> = src.values();
        let num_elements = values.len();

        // Deserialize the inner elements recursively into this temporary buffer.
        // TODO: write them directly to the final location to avoid a copy
        let mut elements = Vec::with_capacity(num_elements);
        elements.resize_with(num_elements, Default::default);
        ArRowDeserialize::read_from_array::<Vec<I>>(values.clone(), &mut elements)?;

        let elements = elements.into_iter();

        (src, elements)
    }};
}

/// Shared loop code of `impl<I> ArRowDeserializeOption for Vec<I>`
/// and impl<I> ArRowDeserialize for Vec<I>
macro_rules! build_list_item {
    ($offset:expr, $previous_offset:expr, $elements:expr) => {{
        // Safe because offset is bounded by num_elements;
        let range = ($previous_offset as usize)..($offset as usize);
        let mut array: Vec<I> = Vec::with_capacity(range.len());
        for _ in range {
            match $elements.next() {
                Some(item) => {
                    array.push(item);
                }
                None => panic!(
                    "List too short (expected {} elements, got {})",
                    $offset - $previous_offset,
                    array.len()
                ),
            }
        }
        $previous_offset = $offset;
        array
    }};
}

/// Implementation of [`read_options_from_array`] generalized over offset type
macro_rules! read_list_of_options_from_array {
    ($src:expr, $offset_ty:ty, $dst: expr) => {{
        if let Some(src) = $src.as_list_opt::<$offset_ty>() {
            let (src, mut elements) = init_list_read!(src, $dst);
            let mut offsets = src.offsets().iter().copied();

            let mut previous_offset = offsets.next().unwrap_or(0);

            let offsets =
                NullableValuesIterator::new(offsets, src.nulls().map(|nulls| nulls.iter()));
            let num_lists = offsets.len();

            if num_lists > $dst.len() {
                return Err(DeserializationError::MismatchedLength {
                    src: num_lists,
                    dst: $dst.len(),
                });
            }

            let mut dst = $dst.iter_mut();

            for offset in offsets {
                // Safe because we checked dst.len() == num_elements, and num_elements
                // is also the size of offsets
                let dst_item: &mut Option<Vec<I>> = unsafe { dst.next().unwrap_unchecked() };
                match offset {
                    None => *dst_item = None,
                    Some(offset) => {
                        *dst_item = Some(build_list_item!(offset, previous_offset, elements));
                    }
                }
            }
            if elements.next().is_some() {
                panic!("List too long");
            }

            return Ok(num_lists);
        }
    }};
}

/// Deserialization of Arrow lists with nullable values
///
/// cannot do `impl<I> ArRowDeserialize for Option<Vec<Option<I>>>` because it causes
/// infinite recursion in the type-checker due to this other implementation being
/// available: `impl<I: ArRowDeserializeOption> ArRowDeserialize for Option<I>`.
impl<I> ArRowDeserializeOption for Vec<I>
where
    I: Default + ArRowDeserialize,
{
    fn read_options_from_array<'a, 'b, T>(
        src: impl Array + AsArray,
        mut dst: &'b mut T,
    ) -> Result<usize, DeserializationError>
    where
        &'b mut T: DeserializationTarget<'a, Item = Option<Self>> + 'b,
    {
        read_list_of_options_from_array!(src, i32, dst);
        read_list_of_options_from_array!(src, i64, dst);
        Err(DeserializationError::MismatchedColumnDataType(format!(
            "Could not cast {:?} array with as_list_opt",
            src.data_type()
        )))
    }
}

/// Implementation of [`read_from_array`] generalized over offset type
macro_rules! read_list_from_array {
    ($src:expr, $offset_ty:ty, $dst: expr) => {{
        if let Some(src) = $src.as_list_opt::<$offset_ty>() {
            let (src, mut elements) = init_list_read!(src, $dst);
            return match src.nulls() {
                Some(_) => Err(DeserializationError::UnexpectedNull(format!(
                    "{} column contains nulls",
                    stringify!($ty)
                ))),
                None => {
                    let mut offsets = src.offsets().iter().copied();

                    let mut previous_offset = offsets.next().unwrap_or(0);
                    let num_lists = offsets.len();

                    if num_lists > $dst.len() {
                        return Err(DeserializationError::MismatchedLength {
                            src: num_lists,
                            dst: $dst.len(),
                        });
                    }

                    let mut dst = $dst.iter_mut();

                    for offset in offsets {
                        // Safe because we checked dst.len() == num_elements, and num_elements
                        // is also the size of offsets
                        let dst_item: &mut Vec<I> = unsafe { dst.next().unwrap_unchecked() };

                        *dst_item = build_list_item!(offset, previous_offset, elements);
                    }
                    if elements.next().is_some() {
                        panic!("List too long");
                    }

                    Ok(num_lists)
                }
            };
        }
    }};
}

/// Deserialization of Arrow lists without nullable values
impl<I> ArRowDeserialize for Vec<I>
where
    I: ArRowDeserialize,
{
    fn read_from_array<'a, 'b, T>(
        src: impl Array + AsArray,
        mut dst: &'b mut T,
    ) -> Result<usize, DeserializationError>
    where
        &'b mut T: DeserializationTarget<'a, Item = Self> + 'b,
    {
        read_list_from_array!(src, i32, dst);
        read_list_from_array!(src, i64, dst);
        Err(DeserializationError::MismatchedColumnDataType(format!(
            "Could not cast {:?} array with as_list_opt",
            src.data_type()
        )))
    }
}

/// The trait of things that can have Arrow data written to them.
///
/// It must be (mutably) iterable, exact-size, and iterable multiple times (one for
/// each column it contains).
///
/// # Safety
///
/// Implementations returning `len()` values larger than the
/// actual length of the iterator returned by `iter_mut()` would lead to
/// undefined behavior (values yielded by the iterator are unwrapped unsafely,
/// for performance).
pub unsafe trait DeserializationTarget<'a> {
    type Item: 'a;
    type IterMut<'b>: Iterator<Item = &'b mut Self::Item>
    where
        Self: 'b,
        'a: 'b;

    fn len(&self) -> usize;
    fn iter_mut(&mut self) -> Self::IterMut<'_>;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn map<B, F>(&mut self, f: F) -> MultiMap<'_, Self, F>
    where
        Self: Sized,
        F: FnMut(&mut Self::Item) -> &mut B,
    {
        MultiMap { iter: self, f }
    }
}

unsafe impl<'a, V: Sized + 'a> DeserializationTarget<'a> for &mut Vec<V> {
    type Item = V;
    type IterMut<'b> = IterMut<'b, V> where V: 'b, 'a: 'b, Self: 'b;

    fn len(&self) -> usize {
        (self as &Vec<_>).len()
    }

    fn iter_mut(&mut self) -> IterMut<'_, V> {
        <[_]>::iter_mut(self)
    }
}

/// A map that can be iterated multiple times
pub struct MultiMap<'c, T: Sized, F> {
    iter: &'c mut T,
    f: F,
}

unsafe impl<'a, V: Sized + 'a, V2: Sized + 'a, T, F> DeserializationTarget<'a>
    for &mut MultiMap<'_, T, F>
where
    F: Copy + for<'b> FnMut(&'b mut V) -> &'b mut V2,
    T: DeserializationTarget<'a, Item = V>,
{
    type Item = V2;
    type IterMut<'b> = Map<T::IterMut<'b>, F> where T: 'b, 'a: 'b, F: 'b, Self: 'b;

    fn len(&self) -> usize {
        self.iter.len()
    }

    fn iter_mut(&mut self) -> Map<T::IterMut<'_>, F> {
        self.iter.iter_mut().map(self.f)
    }
}

/// Internal trait to allow implementing ArRowDeserialize on `Option<T>` where `T` is
/// a structure defined in other crates
pub trait ArRowDeserializeOption: Sized + CheckableDataType {
    /// Reads from a [`Array`] to a structure that behaves like
    /// a rewindable iterator of `&mut Option<Self>`.
    fn read_options_from_array<'a, 'b, T>(
        src: impl Array + AsArray,
        dst: &'b mut T,
    ) -> Result<usize, DeserializationError>
    where
        Self: 'a,
        &'b mut T: DeserializationTarget<'a, Item = Option<Self>> + 'b;
}

impl<I: ArRowDeserializeOption> ArRowDeserialize for Option<I> {
    fn read_from_array<'a, 'b, T>(
        src: impl Array + AsArray,
        dst: &'b mut T,
    ) -> Result<usize, DeserializationError>
    where
        &'b mut T: DeserializationTarget<'a, Item = Self> + 'b,
        I: 'a,
    {
        I::read_options_from_array(src, dst)
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;

    use super::*;

    #[test]
    fn test_map_struct() {
        // TODO: for now this test only makes sure the code compiles, but it should
        // actually run it eventually.
        #[derive(Default)]
        struct Test {
            field1: Option<i64>,
        }

        impl CheckableDataType for Test {
            fn check_datatype(datatype: &DataType) -> Result<(), String> {
                check_datatype_equals(
                    datatype,
                    &[DataType::Struct(
                        vec![Field::new("field1", DataType::Int64, true)].into(),
                    )],
                    "Vec<u8>",
                )
            }
        }

        impl ArRowDeserialize for Option<Test> {
            fn read_from_array<'a, 'b, T>(
                src: impl Array + AsArray,
                mut dst: &'b mut T,
            ) -> Result<usize, DeserializationError>
            where
                &'b mut T: DeserializationTarget<'a, Item = Self>,
            {
                let src = src.as_struct_opt().ok_or_else(|| {
                    DeserializationError::MismatchedColumnDataType(format!(
                        "Could not cast {:?} array with as_struct_opt",
                        src.data_type()
                    ))
                })?;
                let columns = src.columns();
                let column = columns.iter().next().unwrap();
                ArRowDeserialize::read_from_array::<MultiMap<&mut T, _>>(
                    column.clone(),
                    &mut dst.map(|struct_| &mut struct_.as_mut().unwrap().field1),
                )?;

                Ok(src.len())
            }
        }
    }

    #[test]
    fn test_check_datatype() {
        assert_eq!(i64::check_datatype(&DataType::Int64), Ok(()));
        assert_eq!(
            crate::Timestamp::check_datatype(&DataType::Timestamp(TimeUnit::Nanosecond, None)),
            Ok(())
        );
        assert_eq!(String::check_datatype(&DataType::Utf8), Ok(()));
        assert_eq!(String::check_datatype(&DataType::LargeUtf8), Ok(()));
        assert_eq!(Box::<[u8]>::check_datatype(&DataType::Binary), Ok(()));
        assert_eq!(Box::<[u8]>::check_datatype(&DataType::LargeBinary), Ok(()));
    }

    #[test]
    fn test_check_datatype_fail() {
        assert_eq!(
            i64::check_datatype(&DataType::Utf8),
            Err("i64 must be decoded from Arrow Int64, not Arrow Utf8".to_string())
        );
        assert_eq!(
            i64::check_datatype(&DataType::Int32),
            Err("i64 must be decoded from Arrow Int64, not Arrow Int32".to_string())
        );
        assert_eq!(
            String::check_datatype(&DataType::Int32),
            Err("String must be decoded from Arrow Utf8/LargeUtf8, not Arrow Int32".to_string())
        );
        assert_eq!(
            String::check_datatype(&DataType::Binary),
            Err("String must be decoded from Arrow Utf8/LargeUtf8, not Arrow Binary".to_string())
        );
        assert_eq!(
            Box::<[u8]>::check_datatype(&DataType::Int32),
            Err(
                "Box<[u8]> must be decoded from Arrow Binary/LargeBinary, not Arrow Int32"
                    .to_string()
            )
        );
        assert_eq!(
            Box::<[u8]>::check_datatype(&DataType::Utf8),
            Err(
                "Box<[u8]> must be decoded from Arrow Binary/LargeBinary, not Arrow Utf8"
                    .to_string()
            )
        );
    }
}
