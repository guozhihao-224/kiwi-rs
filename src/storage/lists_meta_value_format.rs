//  Copyright (c) 2017-present, arana-db Community.  All rights reserved.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use crate::storage::{
    base_value_format::{DataType, InternalValue, ParsedInternalValue},
    storage_define::{
        LIST_VALUE_INDEX_LENGTH, SUFFIX_RESERVE_LENGTH, TIMESTAMP_LENGTH, TYPE_LENGTH,
        VERSION_LENGTH,
    },
};
use bytes::{BufMut, Bytes, BytesMut};
use chrono::Utc;

const INITIAL_LEFT_INDEX: u64 = 9_223_372_036_854_775_807;
const INITIAL_RIGHT_INDEX: u64 = 9_223_372_036_854_775_808;

/*
 *| type  | list_len | version | left index | right index | reserve |  cdate | timestamp |
 *|  1B   |    4B    |    8B   |     8B     |      8B     |   16B   |    8B  |     8B    |
 */
pub struct ListsMetaValue {
    inner: InternalValue,
    left_index: u64,
    right_index: u64,
}

impl ListsMetaValue {
    pub fn new<T>(user_value: T) -> Self
    where
        T: Into<Bytes>,
    {
        Self {
            inner: InternalValue::new(DataType::List, user_value),
            left_index: INITIAL_LEFT_INDEX,
            right_index: INITIAL_RIGHT_INDEX,
        }
    }

    pub fn encode(&self) -> BytesMut {
        let needed = TYPE_LENGTH
            + self.inner.user_value.len()
            + VERSION_LENGTH
            + LIST_VALUE_INDEX_LENGTH
            + SUFFIX_RESERVE_LENGTH
            + 2 * TIMESTAMP_LENGTH;
        let mut buf = BytesMut::with_capacity(needed);

        buf.put_u8(self.inner.data_type as u8);
        buf.extend_from_slice(&self.inner.user_value);
        buf.put_u64_le(self.inner.version);
        buf.put_u64_le(self.left_index);
        buf.put_u64_le(self.right_index);
        buf.extend_from_slice(&self.inner.reserve);
        buf.put_u64_le(self.inner.ctime);
        buf.put_u64_le(self.inner.etime);

        buf
    }

    pub fn update_version(&mut self) -> u64 {
        let now = Utc::now().timestamp_micros() as u64;
        self.inner.version = match self.inner.version >= now {
            true => self.inner.version + 1,
            false => now,
        };
        self.inner.version
    }

    pub fn left_index(&self) -> u64 {
        self.left_index
    }

    pub fn modify_left_index(&mut self, index: u64) {
        self.left_index -= index;
    }

    pub fn right_index(&self) -> u64 {
        self.right_index
    }

    pub fn modify_right_index(&mut self, index: u64) {
        self.right_index += index;
    }
}

pub struct ParsedListsMetaValue {
    base: ParsedInternalValue,
    count: u64,
    left_index: u64,
    right_index: u64,
}

impl ParsedListsMetaValue {
    pub fn new() {}

    pub fn strip_suffix() {}

    pub fn set_version_to_value() {}

    pub fn set_ctime_to_value() {}

    pub fn set_etime_to_value() {}

    pub fn set_index_to_value() {}

    pub fn initial_meta_value(&mut self) -> u64 {
        
    }

    pub fn is_valid(&self) -> bool {
        !self.base.is_stale() && self.count != 0
    }

    pub fn count(&self) -> u64 {
        self.count
    }

    pub fn set_count(&mut self, count: u64) {}

    pub fn modify_count() {}

    pub fn update_version() {}

    pub fn left_index(&self) -> u64 {
        self.left_index
    }

    pub fn set_left_index() {}

    pub fn modify_left_index() {}

    pub fn right_index(&self) -> u64 {
        self.right_index
    }

    pub fn set_right_index() {}

    pub fn modify_right_index() {}
}

#[cfg(test)]
mod lists_meta_value_tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_new_lists_meta_value() {
        let value = ListsMetaValue::new("test_value");
        assert_eq!(value.left_index(), INITIAL_LEFT_INDEX);
        assert_eq!(value.right_index(), INITIAL_RIGHT_INDEX);
    }

    #[test]
    fn test_encode() {
        let value = ListsMetaValue::new("test");
        let encoded = value.encode();

        let expected_len = TYPE_LENGTH
            + "test".len()
            + VERSION_LENGTH
            + LIST_VALUE_INDEX_LENGTH
            + SUFFIX_RESERVE_LENGTH
            + 2 * TIMESTAMP_LENGTH;
        assert_eq!(encoded.len(), expected_len);

        assert_eq!(encoded[0], DataType::List as u8);
        assert_eq!(&encoded[1..5], b"test");
    }

    #[test]
    fn test_update_version() {
        let mut value = ListsMetaValue::new("test");
        let first_version = value.update_version();

        assert!(first_version > 0);

        thread::sleep(Duration::from_micros(1));
        let second_version = value.update_version();

        assert!(second_version > first_version);
    }

    #[test]
    fn test_index_modifications() {
        let mut value = ListsMetaValue::new("test");

        let original_left = value.left_index();
        value.modify_left_index(1);
        assert_eq!(value.left_index(), original_left - 1);

        let original_right = value.right_index();
        value.modify_right_index(1);
        assert_eq!(value.right_index(), original_right + 1);
    }

    #[test]
    fn test_consecutive_index_modifications() {
        let mut value = ListsMetaValue::new("test");

        value.modify_left_index(1);
        value.modify_left_index(2);
        assert_eq!(value.left_index(), INITIAL_LEFT_INDEX - 3);

        value.modify_right_index(1);
        value.modify_right_index(2);
        assert_eq!(value.right_index(), INITIAL_RIGHT_INDEX + 3);
    }
}
