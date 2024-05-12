mod decode;
mod encode;

use std::{
    collections::BTreeMap,
    ops::{Deref, DerefMut},
};

use bytes::BytesMut;
use enum_dispatch::enum_dispatch;
use thiserror::Error;

#[derive(Debug, Error, PartialEq)]
pub enum RespError {
    #[error("Invalid frame: {0}")]
    InvalidFrame(String),

    #[error("Invalid frame type: {0}")]
    InvalidFrameType(String),

    #[error("Invalid frame length: {0}")]
    InvalidFrameLength(isize),

    #[error("Frame not complete")]
    NotComplete,

    #[error("Invalid UTF-8 string to parse Int error: {0}")]
    ParseIntError(#[from] std::num::ParseIntError),

    #[error("Invalid UTF-8 string to parse Float error: {0}")]
    ParseDoubleError(#[from] std::num::ParseFloatError),
}

#[enum_dispatch]
pub trait RespEncoder {
    fn encode(self) -> Vec<u8>;
}

// 解码 RESP 协议
pub trait RespDecoder: Sized {
    // 用于解码 RESP 协议的前缀
    const PREFIX: &'static str;
    // 解码 RESP 协议
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError>;
    // 期望的长度
    fn expect_length(buf: &[u8]) -> Result<usize, RespError>;
}

#[derive(Debug, PartialEq, PartialOrd, Clone)]
#[enum_dispatch(RespEncoder)]
pub enum RespFrame {
    SimpleString(SimpleString),
    Error(SimpleError),
    Integer(i64),
    BulkString(BulkString),
    Array(RespArray),
    Null(RespNull),
    NullArray(RespNullArray),
    NullBulkString(RespNullBulkString),
    Boolean(bool),
    Double(f64),
    Map(RespMap),
    Set(RespSet),
}

#[derive(Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Clone)]
pub struct SimpleString(String);
#[derive(Debug, PartialEq, Eq, Hash, PartialOrd, Clone)]
pub struct SimpleError(String);
#[derive(Debug, PartialEq, Eq, Hash, PartialOrd, Clone)]
pub struct BulkString(pub(crate) Vec<u8>);
#[derive(Debug, PartialEq, PartialOrd, Clone)]
pub struct RespArray(pub(crate) Vec<RespFrame>);
#[derive(Debug, PartialEq, Eq, Hash, PartialOrd, Clone)]
pub struct RespNull;
#[derive(Debug, PartialEq, Eq, Hash, PartialOrd, Clone)]
pub struct RespNullArray;
#[derive(Debug, PartialEq, Eq, Hash, PartialOrd, Clone)]
pub struct RespNullBulkString;
#[derive(Debug, PartialEq, PartialOrd, Clone)]
pub struct RespMap(BTreeMap<String, RespFrame>);
#[derive(Debug, PartialEq, PartialOrd, Clone)]
pub struct RespSet(Vec<RespFrame>);

impl Deref for SimpleString {
    type Target = String;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Deref for SimpleError {
    type Target = String;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Deref for BulkString {
    type Target = Vec<u8>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

// impl AsRef<[u8]> for BulkString {
//     fn as_ref(&self) -> &[u8] {
//         &self.0
//     }
// }

impl AsRef<str> for BulkString {
    fn as_ref(&self) -> &str {
        std::str::from_utf8(&self.0).unwrap()
    }
}

impl AsRef<str> for SimpleString {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl Deref for RespArray {
    type Target = Vec<RespFrame>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Deref for RespMap {
    type Target = BTreeMap<String, RespFrame>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for RespMap {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl Deref for RespSet {
    type Target = Vec<RespFrame>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl SimpleString {
    pub fn new(s: impl Into<String>) -> Self {
        SimpleString(s.into())
    }
}

impl SimpleError {
    pub fn new(s: impl Into<String>) -> Self {
        SimpleError(s.into())
    }
}

impl BulkString {
    pub fn new(s: impl Into<Vec<u8>>) -> Self {
        BulkString(s.into())
    }
}

impl RespArray {
    pub fn new(s: impl Into<Vec<RespFrame>>) -> Self {
        RespArray(s.into())
    }
}

impl RespMap {
    pub fn new() -> Self {
        RespMap(BTreeMap::new())
    }
}

impl Default for RespMap {
    fn default() -> Self {
        RespMap::new()
    }
}

impl RespSet {
    pub fn new(s: impl Into<Vec<RespFrame>>) -> Self {
        // let set = BTreeSet::from_iter(s.into().into_iter());
        RespSet(s.into())
    }
}

impl From<&str> for SimpleString {
    fn from(s: &str) -> Self {
        SimpleString(s.to_string())
    }
}

impl From<&str> for RespFrame {
    fn from(value: &str) -> Self {
        SimpleString(value.into()).into()
    }
}

impl From<&[u8]> for BulkString {
    fn from(value: &[u8]) -> Self {
        BulkString(value.into())
    }
}

impl From<&[u8]> for RespFrame {
    fn from(value: &[u8]) -> Self {
        BulkString(value.into()).into()
    }
}

impl<const N: usize> From<&[u8; N]> for BulkString {
    fn from(value: &[u8; N]) -> Self {
        BulkString(value.into())
    }
}

impl<const N: usize> From<&[u8; N]> for RespFrame {
    fn from(value: &[u8; N]) -> Self {
        BulkString(value.into()).into()
    }
}

impl From<String> for SimpleString {
    fn from(s: String) -> Self {
        SimpleString(s)
    }
}
