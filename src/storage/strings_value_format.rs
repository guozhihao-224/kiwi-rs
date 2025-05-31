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

use crate::{
    delegate_parsed_value,
    storage::{
        base_value_format::{DataType, InternalValue, ParsedInternalValue},
        error::{Result, StorageError},
        storage_define::{
            STRING_VALUE_SUFFIXLENGTH, SUFFIX_RESERVE_LENGTH, TIMESTAMP_LENGTH, TYPE_LENGTH,
        },
    },
};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::ops::Range;

/*
 * | type | value | reserve | cdate | timestamp |
 * |  1B  |       |   16B   |   8B  |     8B    |
 */
#[derive(Debug, Clone)]
pub struct StringValue {
    pub inner: InternalValue,
}

impl StringValue {
    pub fn new<T>(user_value: T) -> Self
    where
        T: Into<Bytes>,
    {
        Self {
            inner: InternalValue::new(DataType::String, user_value),
        }
    }

    pub fn encode(&self) -> BytesMut {
        let needed = TYPE_LENGTH
            + self.inner.user_value.len()
            + SUFFIX_RESERVE_LENGTH
            + 2 * TIMESTAMP_LENGTH;
        let mut buf = BytesMut::with_capacity(needed);

        buf.put_u8(DataType::String as u8);
        buf.put_slice(&self.inner.user_value);
        buf.put_bytes(0, SUFFIX_RESERVE_LENGTH);
        buf.put_u64_le(self.inner.ctime);
        buf.put_u64_le(self.inner.etime);

        buf
    }
}

pub struct ParsedStringsValue {
    base: ParsedInternalValue,
}

delegate_parsed_value!(ParsedStringsValue);
#[allow(dead_code)]
impl ParsedStringsValue {
    pub fn new<T>(internal_value: T) -> Result<Self>
    where
        T: Into<BytesMut>,
    {
        let value: BytesMut = internal_value.into();
        debug_assert!(value.len() >= STRING_VALUE_SUFFIXLENGTH);
        if value.len() < STRING_VALUE_SUFFIXLENGTH {
            return Err(StorageError::InvalidFormat(format!(
                "invalid string value length: {} < {}",
                value.len(),
                STRING_VALUE_SUFFIXLENGTH,
            )));
        }

        let data_type = DataType::try_from(value[0])?;

        let user_value_len = value.len() - TYPE_LENGTH - STRING_VALUE_SUFFIXLENGTH;
        let user_value_start = TYPE_LENGTH;
        let user_value_end = user_value_start + user_value_len;
        let user_value_range = user_value_start..user_value_end;

        let suffix_start = user_value_end;
        let reserve_start = suffix_start;
        let reserve_end = reserve_start + SUFFIX_RESERVE_LENGTH;
        let reserve_range = reserve_start..reserve_end;

        let mut time_reader = &value[reserve_end..];
        debug_assert!(time_reader.len() >= 2 * TIMESTAMP_LENGTH);
        if time_reader.len() < 2 * TIMESTAMP_LENGTH {
            return Err(StorageError::InvalidFormat(format!(
                "invalid string value length: {} < {}",
                time_reader.len(),
                2 * TIMESTAMP_LENGTH,
            )));
        }
        let ctime = time_reader.get_u64_le();
        let etime = time_reader.get_u64_le();

        Ok(Self {
            base: ParsedInternalValue::new(
                value,
                data_type,
                user_value_range,
                reserve_range,
                0,
                ctime,
                etime,
            ),
        })
    }

    pub fn strip_suffix(&mut self) {
        self.base.value.advance(TYPE_LENGTH);

        let len = self.base.value.len();
        if len >= STRING_VALUE_SUFFIXLENGTH {
            self.base.value.truncate(len - STRING_VALUE_SUFFIXLENGTH);
        }
    }

    pub fn set_ctime_to_value(&mut self) {
        let suffix_start =
            self.base.value.len() - STRING_VALUE_SUFFIXLENGTH + SUFFIX_RESERVE_LENGTH;

        let ctime_bytes = self.base.ctime.to_le_bytes();
        let dst = &mut self.base.value[suffix_start..suffix_start + TIMESTAMP_LENGTH];
        dst.copy_from_slice(&ctime_bytes);
    }

    pub fn set_etime_to_value(&mut self) {
        let suffix_start = self.base.value.len() - STRING_VALUE_SUFFIXLENGTH
            + SUFFIX_RESERVE_LENGTH
            + TIMESTAMP_LENGTH;

        let bytes = self.base.etime.to_le_bytes();
        let dst = &mut self.base.value[suffix_start..suffix_start + TIMESTAMP_LENGTH];
        dst.copy_from_slice(&bytes);
    }
}

#[cfg(test)]
mod strings_value_tests {
    use super::*;

    #[test]
    fn test_new_string_value() {
        let value = StringValue::new("test_value");
        assert_eq!(value.inner.data_type, DataType::String);
        assert_eq!(&value.inner.user_value[..], b"test_value");
    }

    #[test]
    fn test_encode() {
        let test_value = "hello";
        let value = StringValue::new(test_value);
        let encoded = value.encode();

        let expected_len =
            TYPE_LENGTH + test_value.len() + SUFFIX_RESERVE_LENGTH + 2 * TIMESTAMP_LENGTH;
        assert_eq!(encoded.len(), expected_len);

        assert_eq!(encoded[0], DataType::String as u8);

        assert_eq!(&encoded[1..6], test_value.as_bytes());

        let reserve_start = 1 + test_value.len();
        let reserve_end = reserve_start + SUFFIX_RESERVE_LENGTH;
        assert!(encoded[reserve_start..reserve_end].iter().all(|&x| x == 0));

        let ctime_start = reserve_end;
        let ctime_bytes = &encoded[ctime_start..ctime_start + 8];
        let ctime = (&ctime_bytes[0..8])
            .try_into()
            .map(u64::from_le_bytes)
            .unwrap();
        assert_eq!(ctime, value.inner.ctime);

        let etime_start = ctime_start + 8;
        let etime_bytes = &encoded[etime_start..etime_start + 8];
        let etime = (&etime_bytes[0..8])
            .try_into()
            .map(u64::from_le_bytes)
            .unwrap();
        assert_eq!(etime, value.inner.etime);
    }

    #[test]
    fn test_empty_string() {
        let value = StringValue::new("");
        let encoded = value.encode();

        assert_eq!(
            encoded.len(),
            TYPE_LENGTH + SUFFIX_RESERVE_LENGTH + 2 * TIMESTAMP_LENGTH
        );
        assert_eq!(encoded[0], DataType::String as u8);
    }

    #[test]
    fn test_with_special_strings() {
        let test_cases = vec![
            "Hello, World!",
            "123456789",
            "!@#$%^&*()",
            "中文测试",
            "\n\r\t",
            "🦀",
        ];

        for test_str in test_cases {
            let value = StringValue::new(test_str);
            let encoded = value.encode();

            assert_eq!(encoded[0], DataType::String as u8);
            assert_eq!(
                &encoded[1..1 + test_str.as_bytes().len()],
                test_str.as_bytes()
            );

            let expected_len = TYPE_LENGTH
                + test_str.as_bytes().len()
                + SUFFIX_RESERVE_LENGTH
                + 2 * TIMESTAMP_LENGTH;
            assert_eq!(encoded.len(), expected_len);
        }
    }
}
