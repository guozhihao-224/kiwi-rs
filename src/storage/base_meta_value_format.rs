// Copyright 2024 The Kiwi-rs Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//  of patent rights can be found in the PATENTS file in the same directory.

use crate::storage::{
    base_value_format::{DataType, InternalValue, ParsedInternalValue},
    error::{Result, StorageError},
    storage_define::{
        BASE_META_VALUE_SUFFIX_LENGTH, SUFFIX_RESERVE_LENGTH, TIMESTAMP_LENGTH, TYPE_LENGTH,
        VERSION_LENGTH,
    },
};
use bytes::{BufMut, Bytes, BytesMut};
use chrono::Utc;

use super::storage_define::BASE_META_VALUE_COUNT_LENGTH;

#[allow(dead_code)]
type HashesMetaValue = BaseMetaValue;
#[allow(dead_code)]
type ParsedHashesMetaValue = ParsedBaseMetaValue;
#[allow(dead_code)]
type SetsMetaValue = BaseMetaValue;
#[allow(dead_code)]
type ParsedSetsMetaValue = ParsedBaseMetaValue;
#[allow(dead_code)]
type ZSetsMetaValue = BaseMetaValue;
#[allow(dead_code)]
type ParsedZSetsMetaValue = ParsedBaseMetaValue;

/*
 * | type | len | version | reserve | cdate | timestamp |
 * |  1B  | 4B  |    8B   |   16B   |   8B  |     8B    |
 */
#[allow(dead_code)]
pub struct BaseMetaValue {
    pub inner: InternalValue,
}

#[allow(dead_code)]
impl BaseMetaValue {
    pub fn new<T>(user_value: T) -> Self
    where
        T: Into<Bytes>,
    {
        Self {
            inner: InternalValue::new(DataType::None, user_value),
        }
    }

    pub fn update_version(&mut self) -> u64 {
        let now = Utc::now().timestamp_micros() as u64;
        self.inner.version = match self.inner.version >= now {
            true => self.inner.version + 1,
            false => now,
        };
        self.inner.version
    }

    fn encode(&self) -> BytesMut {
        // type(1) + user_value + version(8) + reserve(16) + ctime(8) + etime(8)
        let needed = TYPE_LENGTH
            + self.inner.user_value.len()
            + VERSION_LENGTH
            + SUFFIX_RESERVE_LENGTH
            + 2 * TIMESTAMP_LENGTH;
        let mut buf = BytesMut::with_capacity(needed);

        buf.put_u8(self.inner.data_type as u8);
        buf.extend_from_slice(&self.inner.user_value);
        buf.put_u64_le(self.inner.version);
        buf.extend_from_slice(&self.inner.reserve);
        buf.put_u64_le(self.inner.ctime);
        buf.put_u64_le(self.inner.etime);

        buf
    }
}

#[allow(dead_code)]
pub struct ParsedBaseMetaValue {
    base: ParsedInternalValue,
    count: i32,
}

#[allow(dead_code)]
impl ParsedBaseMetaValue {
    pub fn new<T>(internal_value: T) -> Result<Self>
    where
        T: Into<BytesMut>,
    {
        let value = internal_value.into();
        let value_len = value.len();
        if value.len() < BASE_META_VALUE_SUFFIX_LENGTH {
            return Err(StorageError::InvalidFormat(format!(
                "invalid meta value length: {} < {}",
                value.len(),
                BASE_META_VALUE_SUFFIX_LENGTH,
            )));
        }

        let data_type = value[0].try_into()?;

        let count_bytes: [u8; 4] = value[1..5]
            .try_into()
            .map_err(|_| StorageError::InvalidFormat("invalid count bytes".to_string()))?;
        let count = u32::from_le_bytes(count_bytes) as i32;

        let user_value_size = value_len - TYPE_LENGTH - BASE_META_VALUE_SUFFIX_LENGTH;
        let user_value_start = TYPE_LENGTH;
        let user_value_end = user_value_start + user_value_size;
        let user_value_range = user_value_start..user_value_end;

        let version_start = user_value_end;
        let version_end = version_start + VERSION_LENGTH;
        let version_bytes: [u8; 8] = value[version_start..version_end]
            .try_into()
            .map_err(|_| StorageError::InvalidFormat("invalid version bytes".to_string()))?;
        let version = u64::from_le_bytes(version_bytes);

        let reserve_start = version_end;
        let reserve_end = reserve_start + SUFFIX_RESERVE_LENGTH;
        let reserve_range = reserve_start..reserve_end;

        let ctime_start = reserve_end;
        let ctime_end = ctime_start + TIMESTAMP_LENGTH;
        let ctime_bytes: [u8; 8] = value[ctime_start..ctime_end]
            .try_into()
            .map_err(|_| StorageError::InvalidFormat("invalid ctime bytes".to_string()))?;
        let ctime = u64::from_le_bytes(ctime_bytes);

        let etime_start = ctime_end;
        let etime_end = etime_start + TIMESTAMP_LENGTH;
        let etime_bytes: [u8; 8] = value[etime_start..etime_end]
            .try_into()
            .map_err(|_| StorageError::InvalidFormat("invalid etime bytes".to_string()))?;
        let etime = u64::from_le_bytes(etime_bytes);

        Ok(Self {
            base: ParsedInternalValue::new(
                value,
                data_type,
                user_value_range,
                reserve_range,
                version,
                ctime,
                etime,
            ),
            count,
        })
    }

    pub fn initial_meta_value(&mut self) -> u64 {
        self.set_count(0);
        self.set_etime(0);
        self.set_ctime(0);
        self.update_version()
    }

    fn set_version_to_value(&mut self) {
        let suffix_start = self.base.value.len() - BASE_META_VALUE_SUFFIX_LENGTH;
        let version_bytes = self.base.version.to_le_bytes();
        let dst = &mut self.base.value[suffix_start..suffix_start + VERSION_LENGTH];
        dst.copy_from_slice(&version_bytes);
    }

    fn set_ctime_to_value(&mut self) {
        let suffix_start = self.base.value.len() - 2 * TIMESTAMP_LENGTH;
        let ctime_bytes = self.base.ctime.to_le_bytes();
        let dst = &mut self.base.value[suffix_start..suffix_start + TIMESTAMP_LENGTH];
        dst.copy_from_slice(&ctime_bytes)
    }

    fn set_etime_to_value(&mut self) {
        let suffix_start = self.base.value.len() - TIMESTAMP_LENGTH;
        let etime_bytes = self.base.etime.to_le_bytes();
        let dst = &mut self.base.value[suffix_start..suffix_start + TIMESTAMP_LENGTH];
        dst.copy_from_slice(&etime_bytes)
    }

    fn set_count_to_value(&mut self) {
        let suffix_start = TYPE_LENGTH;
        let count_bytes = self.count.to_le_bytes();
        let dst = &mut self.base.value[suffix_start..suffix_start + BASE_META_VALUE_COUNT_LENGTH];
        dst.copy_from_slice(&count_bytes);
    }

    pub fn is_valid(&self) -> bool {
        !self.base.is_stale() && self.count != 0
    }

    pub fn check_set_count(&self, count: usize) -> bool {
        count <= u32::MAX as usize
    }

    pub fn count(&self) -> i32 {
        self.count
    }

    pub fn set_count(&mut self, count: i32) {
        self.count = count;
    }

    pub fn set_etime(&mut self, etime: u64) {
        self.base.etime = etime;
        self.set_etime_to_value();
    }

    pub fn set_ctime(&mut self, ctime: u64) {
        self.base.ctime = ctime;
        self.set_ctime_to_value();
    }

    pub fn check_modify_count(&mut self, delta: i32) -> bool {
        self.count
            .checked_add(delta)
            .map(|new_count| new_count >= 0 && new_count <= i32::MAX)
            .unwrap_or(false)
    }

    pub fn modify_count(&mut self, delta: i32) {
        self.count = self.count.saturating_add(delta);
        let count_bytes = self.count.to_le_bytes();
        let dst = &mut self.base.value[TYPE_LENGTH..TYPE_LENGTH + BASE_META_VALUE_COUNT_LENGTH];
        dst.copy_from_slice(&count_bytes);
    }

    pub fn update_version(&mut self) -> u64 {
        let now = Utc::now().timestamp_micros() as u64;
        self.base.version = match self.base.version >= now {
            true => self.base.version + 1,
            false => now,
        };

        self.set_version_to_value();
        self.base.version
    }
}

#[cfg(test)]
mod base_meta_value_tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_new_base_meta_value() {
        let value = BaseMetaValue::new("test_value");
        assert_eq!(value.inner.data_type, DataType::None);
        assert_eq!(&value.inner.user_value[..], b"test_value");
        assert_eq!(value.inner.version, 0);
    }

    #[test]
    fn test_update_version() {
        let mut value = BaseMetaValue::new("test");

        let first_version = value.update_version();
        assert!(first_version > 0);

        thread::sleep(Duration::from_micros(1));

        let second_version = value.update_version();
        assert!(second_version > first_version);

        value.inner.version = u64::MAX - 1;
        let large_version = value.update_version();
        assert_eq!(large_version, u64::MAX);
    }

    #[test]
    fn test_encode() {
        let test_value = "test";
        let mut value = BaseMetaValue::new(test_value);
        value.update_version();

        let encoded = value.encode();

        let expected_len = TYPE_LENGTH
            + test_value.len()
            + VERSION_LENGTH
            + SUFFIX_RESERVE_LENGTH
            + 2 * TIMESTAMP_LENGTH;
        assert_eq!(encoded.len(), expected_len);

        let mut pos = 0;

        assert_eq!(encoded[pos], DataType::None as u8);
        pos += TYPE_LENGTH;

        assert_eq!(&encoded[pos..pos + test_value.len()], test_value.as_bytes());
        pos += test_value.len();

        let version_bytes = &encoded[pos..pos + VERSION_LENGTH];
        let version = (&version_bytes[0..8])
            .try_into()
            .map(u64::from_le_bytes)
            .unwrap();
        assert_eq!(version, value.inner.version);
        pos += VERSION_LENGTH;

        assert_eq!(
            &encoded[pos..pos + SUFFIX_RESERVE_LENGTH],
            &value.inner.reserve[..]
        );
        pos += SUFFIX_RESERVE_LENGTH;

        let ctime_bytes = &encoded[pos..pos + TIMESTAMP_LENGTH];
        let ctime = (&ctime_bytes[0..8])
            .try_into()
            .map(u64::from_le_bytes)
            .unwrap();
        assert_eq!(ctime, value.inner.ctime);
        pos += TIMESTAMP_LENGTH;

        let etime_bytes = &encoded[pos..pos + TIMESTAMP_LENGTH];
        let etime = (&etime_bytes[0..8])
            .try_into()
            .map(u64::from_le_bytes)
            .unwrap();
        assert_eq!(etime, value.inner.etime);
    }

    #[test]
    fn test_empty_value() {
        let value = BaseMetaValue::new("");
        let encoded = value.encode();

        let expected_len =
            TYPE_LENGTH + VERSION_LENGTH + SUFFIX_RESERVE_LENGTH + 2 * TIMESTAMP_LENGTH;
        assert_eq!(encoded.len(), expected_len);
    }
}
