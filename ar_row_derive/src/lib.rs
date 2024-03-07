// Copyright (C) 2023-2024 The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

//! Custom `derive` for the [`ar_row`](../ar_row/) crate, to deserialize `structs`
//! in a row-oriented way from Apache Arrow
//!
//! # Supported types
//!
//! Structures can have fields of the following types:
//!
//! * [`bool`], [`i8`], [`i16`], [`i32`], [`i64`], [`u8`], [`u16`], [`u32`], [`u64`], [`f32`], [`f64`], [`String`], `Box<[u8]>` (binary strings),
//!   mapping to their respective Arrow type
//! * `Vec<T>` when `T` is a supported type, mapping to an Arrow list
//! * `HashMap<K, V>` and `Vec<(K, V)>` are not supported yet to deserialize ORC maps
//!   (see <https://gitlab.softwareheritage.org/swh/devel/ar_row-rs/-/issues/1>)
//!
//! # About null values
//!
//! In order to support all Arrow arrays, every single type should be wrapped in `Option`
//! (eg. `struct<a:int, b:list<string>>` in ORC should be
//! `a: Option<i32>, b: Option<Vec<Option<String>>>`), but this is cumbersome, and
//! may have high overhead if you need to check it.
//!
//! If you omit `Option`, then `ar_row_derive` will return an error early for files
//! containing null values, and avoid this overhead for files which don't.
//!
//! # Examples
//!
//! <!-- Keep this in sync with README.md -->
//!
//! ```
//! extern crate ar_row;
//! extern crate ar_row_derive;
//! extern crate datafusion_orc;
//!
//! use std::fs::File;
//! use std::num::NonZeroU64;
//!
//! use datafusion_orc::projection::ProjectionMask;
//! use datafusion_orc::{ArrowReader, ArrowReaderBuilder};
//!
//! use ar_row::deserialize::{ArRowDeserialize, ArRowStruct};
//! use ar_row::row_iterator::RowIterator;
//! use ar_row_derive::ArRowDeserialize;
//!
//! // Define structure
//! #[derive(ArRowDeserialize, Clone, Default, Debug, PartialEq, Eq)]
//! struct Test1 {
//!     long1: Option<i64>,
//! }
//!
//! // Open file
//! let orc_path = "../test_data/TestOrcFile.test1.orc";
//! let file = File::open(orc_path).expect("could not open .orc");
//! let builder = ArrowReaderBuilder::try_new(file).expect("could not make builder");
//! let projection = ProjectionMask::named_roots(
//!     builder.file_metadata().root_data_type(),
//!     &["long1"],
//! );
//! let reader = builder.with_projection(projection).build();
//! let rows: Vec<Option<Test1>> = reader
//!     .flat_map(|batch| -> Vec<Option<Test1>> {
//!         <Option<Test1>>::from_record_batch(batch.unwrap()).unwrap()
//!     })
//!     .collect();
//!
//! assert_eq!(
//!     rows,
//!     vec![
//!         Some(Test1 {
//!             long1: Some(9223372036854775807)
//!         }),
//!         Some(Test1 {
//!             long1: Some(9223372036854775807)
//!         })
//!     ]
//! );
//! ```
//!
//! Or equivalently, using `RowIterator` to reuse the buffer between record batches,
//! but needs `RecordBatch` instead of `Result<RecordBatch, _>` as input:
//!
//! <!-- Keep this in sync with README.md -->
//!
//! ```
//! extern crate ar_row;
//! extern crate ar_row_derive;
//! extern crate datafusion_orc;
//!
//! use std::fs::File;
//! use std::num::NonZeroU64;
//!
//! use datafusion_orc::projection::ProjectionMask;
//! use datafusion_orc::{ArrowReader, ArrowReaderBuilder};
//!
//! use ar_row::deserialize::{ArRowDeserialize, ArRowStruct};
//! use ar_row::row_iterator::RowIterator;
//! use ar_row_derive::ArRowDeserialize;
//!
//! // Define structure
//! #[derive(ArRowDeserialize, Clone, Default, Debug, PartialEq, Eq)]
//! struct Test1 {
//!     long1: Option<i64>,
//! }
//!
//! // Open file
//! let orc_path = "../test_data/TestOrcFile.test1.orc";
//! let file = File::open(orc_path).expect("could not open .orc");
//! let builder = ArrowReaderBuilder::try_new(file).expect("could not make builder");
//! let projection = ProjectionMask::named_roots(
//!     builder.file_metadata().root_data_type(),
//!     &["long1"],
//! );
//! let reader = builder.with_projection(projection).build();
//! let mut rows: Vec<Option<Test1>> = RowIterator::new(reader.map(|batch| batch.unwrap()))
//!     .expect("Could not create iterator")
//!     .collect();
//!
//! assert_eq!(
//!     rows,
//!     vec![
//!         Some(Test1 {
//!             long1: Some(9223372036854775807)
//!         }),
//!         Some(Test1 {
//!             long1: Some(9223372036854775807)
//!         })
//!     ]
//! );
//! ```
//!
//! It is also possible to nest structures:
//!
//! <!-- Keep this in sync with README.md -->
//!
//! ```
//! extern crate ar_row;
//! extern crate ar_row_derive;
//!
//! use ar_row_derive::ArRowDeserialize;
//!
//! #[derive(ArRowDeserialize, Default, Debug, PartialEq)]
//! struct Test1Option {
//!     boolean1: Option<bool>,
//!     byte1: Option<i8>,
//!     short1: Option<i16>,
//!     int1: Option<i32>,
//!     long1: Option<i64>,
//!     float1: Option<f32>,
//!     double1: Option<f64>,
//!     bytes1: Option<Box<[u8]>>,
//!     string1: Option<String>,
//!     list: Option<Vec<Option<Test1ItemOption>>>,
//! }
//!
//! #[derive(ArRowDeserialize, Default, Debug, PartialEq)]
//! struct Test1ItemOption {
//!     int1: Option<i32>,
//!     string1: Option<String>,
//! }
//! ```

extern crate proc_macro;
extern crate proc_macro2;
extern crate quote;
extern crate syn;

use proc_macro::TokenStream;
use proc_macro2::Ident;
use quote::{format_ident, quote};
use syn::*;

/// `#[derive(ArRowDeserialize)] struct T { ... }` implements
/// [`ArRowDeserialize`](../ar_row/deserialize/struct.ArRowDeserialize.html),
/// [`CheckableDataType`](../ar_row/deserialize/struct.CheckableDataType.html), and
/// [`ArRowStruct`](../ar_row/deserialize/struct.ArRowStruct.html) for `T`
///
/// This automatically gives implementations for `Option<T>` and `Vec<T>` as well.
#[proc_macro_derive(ArRowDeserialize)]
pub fn ar_row_deserialize(input: TokenStream) -> TokenStream {
    let ast = parse_macro_input!(input as DeriveInput);

    let tokens = match ast.data {
        Data::Struct(DataStruct {
            fields: Fields::Named(FieldsNamed { named, .. }),
            ..
        }) => impl_struct(
            &ast.ident,
            named
                .iter()
                .map(|field| {
                    field
                        .ident
                        .as_ref()
                        .expect("#ident must not have anonymous fields")
                })
                .collect(),
            named.iter().map(|field| &field.ty).collect(),
        ),
        Data::Struct(DataStruct { .. }) => panic!("#ident must have named fields"),
        _ => panic!("#ident must be a structure"),
    };

    //eprintln!("{}", tokens);

    tokens
}

fn impl_struct(ident: &Ident, field_names: Vec<&Ident>, field_types: Vec<&Type>) -> TokenStream {
    let num_fields = field_names.len();
    let unescaped_field_names: Vec<_> = field_names
        .iter()
        .map(|field_name| format_ident!("{}", field_name))
        .collect();

    let check_datatype_impl = quote!(
        impl ::ar_row::deserialize::CheckableDataType for #ident {
            fn check_datatype(datatype: &::ar_row::arrow::datatypes::DataType) -> Result<(), String> {
                use ::ar_row::arrow::datatypes::DataType;
                match datatype {
                    DataType::Struct(fields) => {
                        let mut fields = fields.iter().enumerate();
                        let mut errors = Vec::new();
                        #(
                            match fields.next() {
                                Some((i, field)) => {
                                    if field.name() != stringify!(#unescaped_field_names) {
                                        errors.push(format!(
                                                "Field #{} must be called {}, not {}",
                                                i, stringify!(#unescaped_field_names), field.name()))
                                    }
                                    else if let Err(s) = <#field_types>::check_datatype(field.data_type()) {
                                        errors.push(format!(
                                            "Field {} cannot be decoded: {}",
                                            stringify!(#unescaped_field_names), s));
                                    }
                                },
                                None => errors.push(format!(
                                    "Field {} is missing",
                                    stringify!(#unescaped_field_names)))
                            }
                        )*

                        if errors.is_empty() {
                            Ok(())
                        }
                        else {
                            Err(format!(
                                "{} cannot be decoded:\n\t{}",
                                stringify!(#ident),
                                errors.join("\n").replace("\n", "\n\t")))
                        }
                    }
                    _ => Err(format!(
                        "{} must be decoded from DataType::Struct, not {:?}",
                        stringify!(#ident),
                        datatype))
                }
            }
        }
    );

    let orc_struct_impl = quote!(
        impl ::ar_row::deserialize::ArRowStruct for #ident {
            fn columns_with_prefix(prefix: &str) -> Vec<String> {
                let mut columns = Vec::with_capacity(#num_fields);

                // Hack to get types. Hopefully the compiler notices we don't
                // actually use it at runtime.
                let instance: #ident = Default::default();

                #({
                    #[inline(always)]
                    fn add_columns<FieldType: ::ar_row::deserialize::ArRowStruct>(columns: &mut Vec<String>, prefix: &str, _: FieldType) {
                        let mut field_name_prefix = prefix.to_string();
                        if prefix.len() != 0 {
                            field_name_prefix.push_str(".");
                        }
                        field_name_prefix.push_str(stringify!(#unescaped_field_names));
                        columns.extend(FieldType::columns_with_prefix(&field_name_prefix));
                    }
                    add_columns(&mut columns, prefix, instance.#field_names);
                })*
                columns
            }
        }
    );

    let prelude = quote!(
        use ::std::sync::Arc;
        use ::std::convert::TryInto;
        use ::std::collections::HashMap;

        use ::ar_row::arrow::array::Array;
        use ::ar_row::deserialize::DeserializationError;
        use ::ar_row::deserialize::ArRowDeserialize;
        use ::ar_row::deserialize::DeserializationTarget;

        let src = src.as_struct_opt().ok_or_else(|| {
            DeserializationError::MismatchedColumnDataType(format!(
                "Could not cast {:?} array to struct array",
                src.data_type(),
            ))
        })?;
        let columns = src.columns();
        assert_eq!(
            columns.len(),
            #num_fields,
            "{} has {} fields, but got {} columns.",
            stringify!(ident), #num_fields, columns.len());
        let mut columns = columns.into_iter();

        if src.len() > dst.len() {
            println!("{} src = {} dst = {}", stringify!(#ident), src.len(), dst.len());
            return Err(::ar_row::deserialize::DeserializationError::MismatchedLength { src: src.len(), dst: dst.len() });
        }
    );

    let read_from_array_impl = quote!(
        impl ::ar_row::deserialize::ArRowDeserialize for #ident {
            fn read_from_array<'a, 'b, T> (
                src: impl ::ar_row::arrow::array::Array + ::ar_row::arrow::array::AsArray, mut dst: &'b mut T
            ) -> Result<usize, ::ar_row::deserialize::DeserializationError>
            where
                &'b mut T: ::ar_row::deserialize::DeserializationTarget<'a, Item=#ident> + 'b {
                #prelude

                match src.nulls() {
                    None => {
                        for struct_ in dst.iter_mut() {
                            *struct_ = Default::default()
                        }
                    },
                    Some(nulls) => {
                        for (struct_, b) in dst.iter_mut().zip(nulls) {
                            if b {
                                *struct_ = Default::default()
                            }
                        }
                    }
                }

                #(
                    let column: &Arc<_> = columns.next().expect(
                        &format!("Failed to get '{}' column", stringify!(#field_names)));
                    ArRowDeserialize::read_from_array::<ar_row::deserialize::MultiMap<&mut T, _>>(
                        column.clone(),
                        &mut dst.map(|struct_| &mut struct_.#field_names),
                    )?;
                )*

                Ok(src.len())
            }
        }
    );

    let read_options_from_array_impl = quote!(
        impl ::ar_row::deserialize::ArRowDeserializeOption for #ident {
            fn read_options_from_array<'a, 'b, T> (
                src: impl ::ar_row::arrow::array::Array + ::ar_row::arrow::array::AsArray, mut dst: &'b mut T
            ) -> Result<usize, ::ar_row::deserialize::DeserializationError>
            where
                &'b mut T: ::ar_row::deserialize::DeserializationTarget<'a, Item=Option<#ident>> + 'b {
                #prelude

                match src.nulls() {
                    None => {
                        for struct_ in dst.iter_mut() {
                            *struct_ = Some(Default::default())
                        }
                    },
                    Some(nulls) => {
                        for (struct_, b) in dst.iter_mut().zip(nulls) {
                            if !b {
                                *struct_ = Some(Default::default())
                            }
                        }
                    }
                }

                #(
                    let column: &Arc<_> = columns.next().expect(
                        &format!("Failed to get '{}' column", stringify!(#field_names)));
                    ArRowDeserialize::read_from_array::<::ar_row::deserialize::MultiMap<&mut T, _>>(
                        column.clone(),
                        &mut dst.map(|struct_| &mut unsafe { struct_.as_mut().unwrap_unchecked() }.#field_names),
                    )?;
                )*

                Ok(src.len())
            }
        }
    );

    quote!(
        #check_datatype_impl
        #orc_struct_impl

        #read_from_array_impl
        #read_options_from_array_impl
    )
    .into()
}
