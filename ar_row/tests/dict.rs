// Copyright (C) 2024 The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::sync::Arc;

use ar_row::arrow::array::{Array, DictionaryArray, Int8Array};
use ar_row::arrow::datatypes::Int8Type;

use ar_row::deserialize::ArRowDeserialize;

#[test]
fn test_utf8_dict() {
    let array: DictionaryArray<Int8Type> = vec!["a", "a", "b", "c", "a"].into_iter().collect();
    assert_eq!(
        array.keys(),
        &Int8Array::from(vec![Some(0), Some(0), Some(1), Some(2), Some(0)])
    );

    let array: Arc<dyn Array> = Arc::new(array);

    let rows: Vec<_> = <String>::from_array(array).unwrap();

    assert_eq!(
        rows,
        vec![
            "a".to_string(),
            "a".to_string(),
            "b".to_string(),
            "c".to_string(),
            "a".to_string()
        ]
    );
}

#[test]
fn test_utf8_dict_opt() {
    let array: DictionaryArray<Int8Type> = vec![Some("a"), None, Some("b"), Some("c"), Some("a")]
        .into_iter()
        .collect();
    assert_eq!(
        array.keys(),
        &Int8Array::from(vec![Some(0), None, Some(1), Some(2), Some(0)])
    );

    let array: Arc<dyn Array> = Arc::new(array);

    let rows: Vec<_> = <Option<String>>::from_array(array.clone()).unwrap();

    assert_eq!(
        rows,
        vec![
            Some("a".to_string()),
            None,
            Some("b".to_string()),
            Some("c".to_string()),
            Some("a".to_string())
        ]
    );

    assert!(<String>::from_array(array).is_err());
}
