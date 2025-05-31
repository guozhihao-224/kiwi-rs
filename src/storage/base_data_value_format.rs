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

use super::storage_define::BASE_DATA_VALUE_SUFFIX_LENGTH;
use crate::delegate_parsed_value;
use crate::storage::base_value_format::InternalValue;
use crate::storage::base_value_format::{DataType, ParsedInternalValue};
use crate::storage::error::{Result, StorageError};
use crate::storage::storage_define::{SUFFIX_RESERVE_LENGTH, TIMESTAMP_LENGTH};
use bytes::{Buf, BufMut, Bytes, BytesMut};

/*
 * hash/set/zset/list data value format
 * | value | reserve | ctime |
 * |       |   16B   |   8B  |
 */
#[allow(dead_code)]
pub struct BaseDataValue {
    pub inner: InternalValue,
}

#[allow(dead_code)]
impl BaseDataValue {
    pub fn new<T>(user_value: T) -> Self
    where
        T: Into<Bytes>,
    {
        Self {
            inner: InternalValue::new(DataType::None, user_value),
        }
    }

    pub fn encode(&self) -> BytesMut {
        let needed = self.inner.user_value.len() + SUFFIX_RESERVE_LENGTH + TIMESTAMP_LENGTH;
        let mut buf = BytesMut::with_capacity(needed);

        buf.put_slice(&self.inner.user_value);
        buf.put_slice(&self.inner.reserve);
        buf.put_u64_le(self.inner.ctime);

        buf
    }
}

delegate_parsed_value!(ParsedBaseDataValue);
pub struct ParsedBaseDataValue {
    pub base: ParsedInternalValue,
}

#[allow(dead_code)]
impl ParsedBaseDataValue {
    pub fn new<T>(internal_value: T) -> Result<Self>
    where
        T: Into<BytesMut>,
    {
        let value = internal_value.into();
        debug_assert!(value.len() >= SUFFIX_RESERVE_LENGTH + TIMESTAMP_LENGTH);
        if value.len() < SUFFIX_RESERVE_LENGTH + TIMESTAMP_LENGTH {
            return Err(StorageError::InvalidFormat(format!(
                "invalid string value length: {} < {}",
                value.len(),
                SUFFIX_RESERVE_LENGTH + TIMESTAMP_LENGTH,
            )));
        }

        let data_type: DataType = value[0].try_into()?;
        let user_value_size = value.len() - SUFFIX_RESERVE_LENGTH - TIMESTAMP_LENGTH;
        let user_value_range = 0..user_value_size;
        let reserve_range = user_value_size..user_value_size + SUFFIX_RESERVE_LENGTH;
        let ctime_start = user_value_size + SUFFIX_RESERVE_LENGTH;
        let ctime = (&value[ctime_start..]).get_u64_le();

        Ok(Self {
            base: ParsedInternalValue::new(
                value,
                data_type,
                user_value_range,
                reserve_range,
                0,
                ctime,
                0,
            ),
        })
    }

    pub fn set_ctime_to_value(&mut self) {
        let suffix_start = self.base.value.len() - TIMESTAMP_LENGTH;
        let ctime_bytes = self.base.ctime.to_le_bytes();
        let dst = &mut self.base.value[suffix_start..suffix_start + TIMESTAMP_LENGTH];
        dst.copy_from_slice(&ctime_bytes);
    }

    pub fn strip_suffix(&mut self) {
        let new_len = self
            .base
            .value
            .len()
            .saturating_sub(BASE_DATA_VALUE_SUFFIX_LENGTH);
        self.base.value.truncate(new_len);
    }
}

// #[cfg(test)]
// mod tests {
//     use super::*;

//     #[test]
//     fn test_base_value_encode_and_decode() {
//         let test_value = Slice::new_with_str("test_value");

//         let mut value = BaseDataValue::new(&test_value);
//         let encoded_data = value.encode();

//         let decode_data = ParsedBaseDataValue::new(&encoded_data);

//         assert_eq!(decode_data.user_value().as_bytes(), test_value.as_bytes());
//     }
// }

// #[cfg(test)]
// mod base_data_value_test {
//     use super::*;
//     use rocksdb::{ReadOptions, WriteBatch, WriteOptions, DB};
//     #[test]
//     fn test_new_base_data_value() {
//         let path = "/tmp/my_rocksdb";

//         // 设置 Options：这里使用默认配置
//         let mut opts = Options::default();
//         opts.create_if_missing(true);

//         // 打开数据库
//         let db = DB::open(&path, &opts).expect("Failed to open database");
//         db.get_opt(key, readopts)

//         let value = BaseDataValue::new("test_value");
//         assert_eq!(value.inner.data_type, DataType::None);
//         assert_eq!(&value.inner.user_value[..], b"test_value");
//     }

//     #[test]
//     fn test_encode() {
//         let test_value = "hello";
//         let value = BaseDataValue::new(test_value);
//         let encoded = value.encode();

//         let expected_len = test_value.len() + SUFFIX_RESERVE_LENGTH + TIMESTAMP_LENGTH;
//         assert_eq!(encoded.len(), expected_len);

//         assert_eq!(&encoded[..test_value.len()], test_value.as_bytes());

//         let reserve_start = test_value.len();
//         let reserve_end = reserve_start + SUFFIX_RESERVE_LENGTH;
//         assert_eq!(
//             &encoded[reserve_start..reserve_end],
//             &value.inner.reserve[..]
//         );

//         let timestamp_bytes = &encoded[reserve_end..];
//         let timestamp = (&timestamp_bytes[0..8])
//             .try_into()
//             .map(u64::from_le_bytes)
//             .unwrap();
//         assert_eq!(timestamp, value.inner.ctime);
//     }

//     #[test]
//     fn test_empty_value() {
//         let value = BaseDataValue::new("");
//         let encoded = value.encode();

//         assert_eq!(encoded.len(), SUFFIX_RESERVE_LENGTH + TIMESTAMP_LENGTH);
//     }

//     #[test]
//     fn test_with_different_types() {
//         let cases = vec!["string", "123", "!@#$%", "中文测试"];

//         for test_case in cases {
//             let value = BaseDataValue::new(test_case);
//             let encoded = value.encode();

//             assert_eq!(
//                 encoded.len(),
//                 test_case.as_bytes().len() + SUFFIX_RESERVE_LENGTH + TIMESTAMP_LENGTH
//             );
//             assert_eq!(&encoded[..test_case.as_bytes().len()], test_case.as_bytes());
//         }
//     }
// }
