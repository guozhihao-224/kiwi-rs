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

use super::{
    error::{Result, StorageError},
    storage_define::{
        BASE_META_VALUE_COUNT_LENGTH, BASE_META_VALUE_SUFFIX_LENGTH, LISTS_META_VALUE_SUFFIX_LENGTH,
    },
};

const INITIAL_LEFT_INDEX: u64 = 9_223_372_036_854_775_807;
const INITIAL_RIGHT_INDEX: u64 = 9_223_372_036_854_775_808;

/*
 *| type  | count | version | left index | right index | reserve |  cdate | timestamp |
 *|  1B   |  4B   |    8B   |     8B     |      8B     |   16B   |    8B  |     8B    |
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
            + 2 * LIST_VALUE_INDEX_LENGTH
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
    pub fn new<T>(internal_value: T) -> Result<Self>
    where
        T: Into<BytesMut>,
    {
        let value = internal_value.into();
        let value_len = value.len();
        // TODO : 这里需要校验一下value的长度
        if value.len() < LISTS_META_VALUE_SUFFIX_LENGTH {
            return Err(StorageError::InvalidFormat(format!(
                "invalid lists meta value length: {} < {}",
                value.len(),
                LISTS_META_VALUE_SUFFIX_LENGTH,
            )));
        }

        let data_type = value[0].try_into()?;

        let mut pos = TYPE_LENGTH;

        let count_range = pos..pos + BASE_META_VALUE_COUNT_LENGTH;
        let mut count_bytes = [0u8; BASE_META_VALUE_COUNT_LENGTH];
        count_bytes.copy_from_slice(&value[pos..pos + BASE_META_VALUE_COUNT_LENGTH]);
        let count = u64::from_le_bytes(count_bytes);
        pos += BASE_META_VALUE_COUNT_LENGTH;

        let mut version_bytees = [0u8; VERSION_LENGTH];
        version_bytees.copy_from_slice(&value[pos..pos + VERSION_LENGTH]);
        let version = u64::from_le_bytes(version_bytees);
        pos += VERSION_LENGTH;

        let mut left_index_bytes = [0u8; LIST_VALUE_INDEX_LENGTH];
        left_index_bytes.copy_from_slice(&value[pos..pos + LIST_VALUE_INDEX_LENGTH]);
        let left_index = u64::from_le_bytes(left_index_bytes);
        pos += LIST_VALUE_INDEX_LENGTH;

        let mut right_index_bytes = [0u8; LIST_VALUE_INDEX_LENGTH];
        right_index_bytes.copy_from_slice(&value[pos..pos + LIST_VALUE_INDEX_LENGTH]);
        let right_index = u64::from_le_bytes(right_index_bytes);
        pos += LIST_VALUE_INDEX_LENGTH;

        let reserve_range = pos..pos + SUFFIX_RESERVE_LENGTH;
        pos += SUFFIX_RESERVE_LENGTH;

        let mut ctime_bytes = [0u8; TIMESTAMP_LENGTH];
        ctime_bytes.copy_from_slice(&value[pos..pos + TIMESTAMP_LENGTH]);
        let ctime = u64::from_le_bytes(ctime_bytes);
        pos += TIMESTAMP_LENGTH;

        let mut etime_bytes = [0u8; TIMESTAMP_LENGTH];
        etime_bytes.copy_from_slice(&value[pos..pos + TIMESTAMP_LENGTH]);
        let etime = u64::from_le_bytes(etime_bytes);

        Ok(Self {
            base: ParsedInternalValue::new(
                value,
                data_type,
                count_range,
                reserve_range,
                version,
                ctime,
                etime,
            ),
            count,
            left_index,
            right_index,
        })
    }

    // TODO: 不确定是否需要这个
    pub fn strip_suffix(&mut self) {}

    pub fn set_version_to_value(&mut self) {
        let version_start = TYPE_LENGTH + BASE_META_VALUE_COUNT_LENGTH;
        let version_bytes = self.base.version.to_le_bytes();
        let dst = &mut self.base.value[version_start..version_start + VERSION_LENGTH];
        dst.copy_from_slice(&version_bytes);
    }

    pub fn set_ctime(&mut self, ctime: u64) {
        self.base.ctime = ctime;
        self.set_ctime_to_value();
    }

    pub fn set_ctime_to_value(&mut self) {
        let ctime_start = self.base.value.len() - 2 * TIMESTAMP_LENGTH;
        let ctime_bytes = self.base.ctime.to_le_bytes();
        let dst = &mut self.base.value[ctime_start..ctime_start + TIMESTAMP_LENGTH];
        dst.copy_from_slice(&ctime_bytes);
    }

    pub fn set_etime(&mut self, ctime: u64) {
        self.base.etime = ctime;
        self.set_etime_to_value();
    }

    pub fn set_etime_to_value(&mut self) {
        let etime_start = self.base.value.len() - TIMESTAMP_LENGTH;
        let etime_bytes = self.base.etime.to_le_bytes();
        let dst = &mut self.base.value[etime_start..etime_start + TIMESTAMP_LENGTH];
        dst.copy_from_slice(&etime_bytes);
    }

    pub fn set_index_to_value(&mut self) {}

    pub fn initial_meta_value(&mut self) -> u64 {
        self.set_count(0);
        self.set_left_index(INITIAL_LEFT_INDEX);
        self.set_right_index(INITIAL_RIGHT_INDEX);
        0
    }

    pub fn is_valid(&self) -> bool {
        !self.base.is_stale() && self.count != 0
    }

    pub fn count(&self) -> u64 {
        self.count
    }

    pub fn set_count(&mut self, count: u64) {}

    pub fn modify_count(&mut self, delta: u64) {}

    pub fn update_version(&mut self) -> u64 {}

    pub fn left_index(&self) -> u64 {
        self.left_index
    }

    pub fn set_left_index(&mut self, index: u64) {}

    pub fn modify_left_index(&mut self, index: u64) {}

    pub fn right_index(&self) -> u64 {
        self.right_index
    }

    pub fn set_right_index(&mut self, index: u64) {}

    pub fn modify_right_index(&mut self, index: u64) {}
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
