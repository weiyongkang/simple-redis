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

pub trait RespDecoder: Sized {
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError>;
}

#[derive(Debug, PartialEq, PartialOrd)]
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
#[derive(Debug, PartialEq, Eq, Hash, PartialOrd)]
pub struct SimpleError(String);
#[derive(Debug, PartialEq, Eq, Hash, PartialOrd)]
pub struct BulkString(Vec<u8>);
#[derive(Debug, PartialEq, PartialOrd)]
pub struct RespArray(Vec<RespFrame>);
#[derive(Debug, PartialEq, Eq, Hash, PartialOrd)]
pub struct RespNull;
#[derive(Debug, PartialEq, Eq, Hash, PartialOrd)]
pub struct RespNullArray;
#[derive(Debug, PartialEq, Eq, Hash, PartialOrd)]
pub struct RespNullBulkString;
#[derive(Debug, PartialEq, PartialOrd)]
pub struct RespMap(BTreeMap<String, RespFrame>);
#[derive(Debug, PartialEq, PartialOrd)]
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

// impl From<SimpleString> for RespFrame {
//     fn from(s:SimpleString) -> Self {
//         RespFrame::SimpleString(s)
//     }
// }

// impl From<SimpleError> for RespFrame {
//     fn from(s:SimpleError) -> Self {
//         RespFrame::Error(s)
//     }
// }

// impl From<i64> for RespFrame {
//     fn from(s:i64) -> Self {
//         RespFrame::Integer(s)
//     }
// }

// impl From<BulkString> for RespFrame {
//     fn from(s:BulkString) -> Self {
//         RespFrame::BulkString(s)
//     }
// }

// impl From<RespArray> for RespFrame {
//     fn from(s:RespArray) -> Self {
//         RespFrame::Array(s)
//     }
// }

// impl From<RespNull> for RespFrame {
//     fn from(s:RespNull) -> Self {
//         RespFrame::Null(s)
//     }
// }

// impl From<RespNullArray> for RespFrame {
//     fn from(s:RespNullArray) -> Self {
//         RespFrame::NullArray(s)
//     }
// }

// impl From<RespNullBulkString> for RespFrame {
//     fn from(s:RespNullBulkString) -> Self {
//         RespFrame::NullBulkString(s)
//     }
// }

// impl From<bool> for RespFrame {
//     fn from(s:bool) -> Self {
//         RespFrame::Boolean(s)
//     }
// }

// impl From<f64> for RespFrame {
//     fn from(s:f64) -> Self {
//         RespFrame::Double(s)
//     }
// }

// impl From<RespMap> for RespFrame {
//     fn from(s:RespMap) -> Self {
//         RespFrame::Map(s)
//     }
// }

// impl From<RespSet> for RespFrame {
//     fn from(s:RespSet) -> Self {
//         RespFrame::Set(s)
//     }
// }
