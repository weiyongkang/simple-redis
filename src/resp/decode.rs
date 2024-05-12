use crate::resp::*;
use bytes::{Buf, BytesMut};

const CRLF: &[u8] = b"\r\n";
const CRLF_LEN: usize = CRLF.len();

// 定义 RESP 解码器
// 从 RESP 协议中解析帧，帧数据格式是 Bytes 格式，每次解析一个帧，返回一个 RespFrame，然后 指针移动到下一个帧的位置
impl RespDecoder for RespFrame {
    const PREFIX: &'static str = "";
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        let mut iter = buf.iter().peekable();
        match iter.peek() {
            Some(b'+') => {
                let s: Self = SimpleString::decode(buf)?.into();
                Ok(s)
            }
            Some(b'-') => {
                let s: Self = SimpleError::decode(buf)?.into();
                Ok(s)
            }
            Some(b':') => {
                let s: Self = i64::decode(buf)?.into();
                Ok(s)
            }
            Some(b'$') => match RespNullBulkString::decode(buf) {
                Ok(s) => Ok(s.into()),
                Err(RespError::NotComplete) => {
                    let s: Self = BulkString::decode(buf)?.into();
                    Ok(s)
                }
                Err(_) => {
                    let s: Self = BulkString::decode(buf)?.into();
                    Ok(s)
                }
            },
            Some(b'*') => {
                // try null array first
                match RespNullArray::decode(buf) {
                    Ok(s) => Ok(s.into()),
                    Err(RespError::NotComplete) => Err(RespError::NotComplete),
                    Err(_) => {
                        let s: Self = RespArray::decode(buf)?.into();
                        Ok(s)
                    }
                }
            }
            Some(b'%') => {
                let s: Self = RespMap::decode(buf)?.into();
                Ok(s)
            }
            Some(b'~') => {
                let s: Self = RespSet::decode(buf)?.into();
                Ok(s)
            }
            Some(b'_') => {
                let s: Self = RespNull::decode(buf)?.into();
                Ok(s)
            }
            Some(b'#') => {
                let s: Self = bool::decode(buf)?.into();
                Ok(s)
            }
            Some(b',') => {
                let s: Self = f64::decode(buf)?.into();
                Ok(s)
            }
            None => Err(RespError::NotComplete),
            _ => Err(RespError::InvalidFrameType(format!(
                "expect_length: unknown frame type: {:?}",
                buf
            ))),
        }
    }

    fn expect_length(buf: &[u8]) -> Result<usize, RespError> {
        let mut iter = buf.iter().peekable();
        match iter.peek() {
            Some(b'+') => SimpleString::expect_length(buf),
            Some(b'-') => SimpleError::expect_length(buf),
            Some(b':') => i64::expect_length(buf),
            Some(b'$') => BulkString::expect_length(buf),
            Some(b'*') => RespArray::expect_length(buf),
            Some(b'%') => RespMap::expect_length(buf),
            Some(b'~') => RespSet::expect_length(buf),
            Some(b'_') => RespNull::expect_length(buf),
            Some(b'#') => bool::expect_length(buf),
            Some(b',') => f64::expect_length(buf),
            _ => Err(RespError::NotComplete),
        }
    }
}

impl RespDecoder for SimpleString {
    const PREFIX: &'static str = "+";

    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        let end = extract_simple_frame_data(buf, Self::PREFIX)?;
        let data = buf.split_to(end + CRLF_LEN);
        let s = String::from_utf8_lossy(&data[1..end]);
        Ok(SimpleString(s.into()))
    }

    fn expect_length(buf: &[u8]) -> Result<usize, RespError> {
        let end = extract_simple_frame_data(buf, Self::PREFIX)?;
        Ok(end + CRLF_LEN)
    }
}

impl RespDecoder for SimpleError {
    const PREFIX: &'static str = "-";
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        let end = extract_simple_frame_data(buf, Self::PREFIX)?;
        let data = buf.split_to(end + CRLF_LEN);
        let s = String::from_utf8_lossy(&data[1..end]);
        Ok(SimpleError(s.into()))
    }

    fn expect_length(buf: &[u8]) -> Result<usize, RespError> {
        let end = extract_simple_frame_data(buf, Self::PREFIX)?;
        Ok(end + CRLF_LEN)
    }
}

impl RespDecoder for i64 {
    const PREFIX: &'static str = ":";
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        let end = extract_simple_frame_data(buf, Self::PREFIX)?;
        let data = buf.split_to(end + CRLF_LEN);
        let s = String::from_utf8_lossy(&data[1..end]);
        Ok(s.parse()?)
    }
    fn expect_length(buf: &[u8]) -> Result<usize, RespError> {
        let end = extract_simple_frame_data(buf, Self::PREFIX)?;
        Ok(end + CRLF_LEN)
    }
}

impl RespDecoder for f64 {
    const PREFIX: &'static str = ",";
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        let end = extract_simple_frame_data(buf, Self::PREFIX)?;
        let data = buf.split_to(end + CRLF_LEN);
        let s = String::from_utf8_lossy(&data[1..end]);
        Ok(s.parse()?)
    }
    fn expect_length(buf: &[u8]) -> Result<usize, RespError> {
        let end = extract_simple_frame_data(buf, Self::PREFIX)?;
        Ok(end + CRLF_LEN)
    }
}

impl RespDecoder for RespNull {
    const PREFIX: &'static str = "_";
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        extend_fixed_data(buf, "_\r\n", "Null")?;
        Ok(RespNull)
    }
    fn expect_length(_: &[u8]) -> Result<usize, RespError> {
        Ok(3)
    }
}

impl RespDecoder for RespNullArray {
    const PREFIX: &'static str = "*";
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        extend_fixed_data(buf, "*-1\r\n", "NullArrays")?;
        Ok(RespNullArray)
    }
    fn expect_length(_: &[u8]) -> Result<usize, RespError> {
        Ok(4)
    }
}

impl RespDecoder for RespNullBulkString {
    const PREFIX: &'static str = "$";
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        extend_fixed_data(buf, "$-1\r\n", "NullBulkString")?;
        Ok(RespNullBulkString)
    }
    fn expect_length(_: &[u8]) -> Result<usize, RespError> {
        Ok(5)
    }
}

impl RespDecoder for bool {
    const PREFIX: &'static str = "#";
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        match extend_fixed_data(buf, "#t\r\n", "Bool") {
            Ok(_) => Ok(true),
            Err(RespError::NotComplete) => Err(RespError::NotComplete),
            Err(_) => match extend_fixed_data(buf, "#f\r\n", "Bool") {
                Ok(_) => Ok(false),
                Err(e) => Err(e),
            },
        }
    }
    fn expect_length(_: &[u8]) -> Result<usize, RespError> {
        Ok(4)
    }
}

impl RespDecoder for BulkString {
    const PREFIX: &'static str = "$";
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        let (end, len) = parse_length(buf, Self::PREFIX)?;
        let remained = &buf[end + CRLF_LEN..];
        if remained.len() < len + CRLF_LEN {
            return Err(RespError::NotComplete);
        }

        buf.advance(end + CRLF_LEN);
        let data = buf.split_to(len + CRLF_LEN);
        Ok(BulkString(data[..len].to_vec()))
    }

    fn expect_length(buf: &[u8]) -> Result<usize, RespError> {
        let (end, len) = parse_length(buf, Self::PREFIX)?;
        Ok(end + CRLF_LEN + len + CRLF_LEN)
    }
}

/**
 * 从 RESP 协议中解析数组，数组的格式如下：
 * - array: "*<length-for-elements>\r\n<element-1>..<element-n>"
 *
 */
// - array: "*<length-for-elements>\r\n<element-1>..<element-n>"
impl RespDecoder for RespArray {
    const PREFIX: &'static str = "*"; // 数据前缀

    // 解析 RESP 数组
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        let (end, len) = parse_length(buf, Self::PREFIX)?;
        let total = calc_total_length(buf, end, len, Self::PREFIX)?;
        if buf.len() < total {
            return Err(RespError::NotComplete);
        }
        buf.advance(end + CRLF_LEN);
        let mut array = Vec::with_capacity(len);
        for _ in 0..len {
            let frame = RespFrame::decode(buf)?;
            array.push(frame);
        }
        Ok(RespArray::new(array))
    }

    // 期望的长度
    fn expect_length(buf: &[u8]) -> Result<usize, RespError> {
        let (end, len) = parse_length(buf, Self::PREFIX)?;
        calc_total_length(buf, end, len, Self::PREFIX)
    }
}

// - map: "%<length-for-elements>\r\n<key-1><value-1>..<key-n><value-n>"
impl RespDecoder for RespMap {
    const PREFIX: &'static str = "%";
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        let (end, len) = parse_length(buf, Self::PREFIX)?;
        let total = calc_total_length(buf, end, len, Self::PREFIX)?;
        if buf.len() < total {
            return Err(RespError::NotComplete);
        }
        buf.advance(end + CRLF_LEN);
        let mut map = RespMap::new();
        for _ in 0..len {
            let key = SimpleString::decode(buf)?;
            let value = RespFrame::decode(buf)?;
            map.insert(key.0, value);
        }
        Ok(map)
    }

    fn expect_length(buf: &[u8]) -> Result<usize, RespError> {
        let (end, len) = parse_length(buf, Self::PREFIX)?;
        calc_total_length(buf, end, len, Self::PREFIX)
    }
}

// -set: "~<length-for-elements>\r\n<element-1>..<element-n>"
impl RespDecoder for RespSet {
    const PREFIX: &'static str = "~";
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        let (end, len) = parse_length(buf, Self::PREFIX)?;
        let total = calc_total_length(buf, end, len, Self::PREFIX)?;
        if buf.len() < total {
            return Err(RespError::NotComplete);
        }
        buf.advance(end + CRLF_LEN); // skip the prefix
        let mut set = Vec::with_capacity(len);
        for _ in 0..len {
            let frame = RespFrame::decode(buf)?;
            set.push(frame);
        }

        Ok(RespSet::new(set))
    }

    fn expect_length(buf: &[u8]) -> Result<usize, RespError> {
        let (end, len) = parse_length(buf, Self::PREFIX)?;
        calc_total_length(buf, end, len, Self::PREFIX)
    }
}

// 提取固定长度数据, 返回数据的长度,并且 buf 指针移动
fn extend_fixed_data(buf: &mut BytesMut, expect: &str, expect_type: &str) -> Result<(), RespError> {
    if buf.len() < expect.len() {
        return Err(RespError::NotComplete);
    }

    if !buf.starts_with(expect.as_bytes()) {
        return Err(RespError::InvalidFrameType(format!(
            "Expecting '{}', got {:?}",
            expect_type, buf
        )));
    }
    buf.advance(expect.len());
    Ok(())
}

// 提取简单帧数据, 返回数据的长度
fn extract_simple_frame_data(buf: &[u8], prefix: &str) -> Result<usize, RespError> {
    if buf.len() < 3 {
        return Err(RespError::NotComplete);
    }

    if !buf.starts_with(prefix.as_bytes()) {
        return Err(RespError::InvalidFrameType(format!(
            "Expecting '{}', got {:?}",
            prefix, buf
        )));
    }
    // search for "\r\n"

    let end = find_crlf(buf, 1).ok_or(RespError::NotComplete)?;
    Ok(end)
}

// 查找第n个CRLF的位置
fn find_crlf(buf: &[u8], nth: usize) -> Option<usize> {
    let mut count = 0;
    for i in 0..buf.len() - 1 {
        if buf[i] == b'\r' && buf[i + 1] == b'\n' {
            count += 1;
            if count == nth {
                return Some(i);
            }
        }
    }
    None
}

// 获得前缀后的长度，和 元素 的长度
fn parse_length(buf: &[u8], prefix: &str) -> Result<(usize, usize), RespError> {
    let end = extract_simple_frame_data(buf, prefix)?;
    let s = String::from_utf8_lossy(&buf[prefix.len()..end]);
    Ok((end, s.parse()?))
}

// 获得去掉前缀后的长度，然后根据长度计算包括CRLF的总长度, 用于判断是否完整, 以及截取数据
fn calc_total_length(buf: &[u8], end: usize, len: usize, prefix: &str) -> Result<usize, RespError> {
    let mut total = end + CRLF_LEN;
    let mut data = &buf[total..];
    match prefix {
        "*" | "~" => {
            for _ in 0..len {
                let len = RespFrame::expect_length(data)?;
                data = &data[len..];
                total += len;
            }
            Ok(total)
        }
        "%" => {
            for _ in 0..len {
                let len = SimpleString::expect_length(data)?;
                data = &data[len..];
                total += len;

                let len = RespFrame::expect_length(data)?;
                data = &data[len..];
                total += len;
            }
            Ok(total)
        }
        _ => Ok(len + CRLF_LEN),
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;
    use bytes::BufMut;

    #[test]
    fn test_simple_string_decode() -> Result<()> {
        let mut buf = BytesMut::from("+hello\r\n");
        let s: SimpleString = SimpleString::new("hello");
        assert_eq!(SimpleString::decode(&mut buf).unwrap(), s);

        buf.extend_from_slice(b"+hello\r");

        let ret = SimpleString::decode(&mut buf);

        assert_eq!(ret.unwrap_err(), RespError::NotComplete);

        buf.put_u8(b'\n');

        let ret: SimpleString = SimpleString::decode(&mut buf)?;

        assert_eq!(ret, s);
        Ok(())
    }

    #[test]
    fn test_simple_error_decode() -> Result<()> {
        let mut buf = BytesMut::from("-error\r\n");
        let s: SimpleError = SimpleError::new("error");
        assert_eq!(SimpleError::decode(&mut buf).unwrap(), s);
        Ok(())
    }

    #[test]
    fn test_integer_decode() -> Result<()> {
        let mut buf = BytesMut::from(":1000\r\n");
        let s: i64 = 1000;
        assert_eq!(i64::decode(&mut buf).unwrap(), s);
        Ok(())
    }

    #[test]
    fn test_bool_decode() -> Result<()> {
        let mut buf = BytesMut::from("#t\r\n");
        let s: bool = true;
        assert_eq!(bool::decode(&mut buf).unwrap(), s);

        let mut buf = BytesMut::from("#f\r\n");
        let s: bool = false;
        assert_eq!(bool::decode(&mut buf).unwrap(), s);
        Ok(())
    }

    #[test]
    fn test_bulk_string_decode() -> Result<()> {
        let mut buf = BytesMut::from("$5\r\nhello\r\n");
        let s = BulkString::new(b"hello".to_vec());
        println!("{:?}", s);
        assert_eq!(BulkString::decode(&mut buf).unwrap(), s);
        Ok(())
    }

    #[test]
    fn test_null_bulk_string_decode() -> Result<()> {
        let mut buf = BytesMut::from("$-1\r\n");
        let s = RespNullBulkString;
        assert_eq!(RespNullBulkString::decode(&mut buf).unwrap(), s);
        Ok(())
    }

    #[test]
    fn test_null_decode() -> Result<()> {
        let mut buf = BytesMut::from("_\r\n");
        let s = RespNull;
        assert_eq!(RespNull::decode(&mut buf).unwrap(), s);
        buf.extend_from_slice(b"_\r");
        // RespNull::decode(&mut buf)?;
        Ok(())
    }

    #[test]
    fn test_null_array_decode() -> Result<()> {
        let mut buf = BytesMut::from("*-1\r\n");
        let s = RespNullArray;
        assert_eq!(RespNullArray::decode(&mut buf).unwrap(), s);
        Ok(())
    }

    #[test]
    fn test_array_decode() -> Result<()> {
        let mut buf = BytesMut::from("*2\r\n+hello\r\n-error\r\n");
        let s = RespArray::new(vec![
            SimpleString::new("hello").into(),
            SimpleError::new("error").into(),
        ]);
        assert_eq!(RespArray::decode(&mut buf).unwrap(), s);
        Ok(())
    }

    #[test]
    fn test_map_decode() -> Result<()> {
        let mut buf = BytesMut::from("%1\r\n+hello\r\n+world\r\n");
        let mut map = RespMap::new();
        map.insert("hello".into(), SimpleString::new("world").into());
        assert_eq!(RespMap::decode(&mut buf).unwrap(), map);
        Ok(())
    }

    #[test]
    fn test_set_decode() -> Result<()> {
        let mut buf = BytesMut::from("~3\r\n+hello\r\n-error\r\n:1000\r\n");
        let s = RespSet::new(vec![
            SimpleString::new("hello").into(),
            SimpleError::new("error").into(),
            1000.into(),
        ]);
        assert_eq!(RespSet::decode(&mut buf).unwrap(), s);
        Ok(())
    }

    #[test]
    fn test_f64_decode() -> Result<()> {
        let mut buf = BytesMut::from(",1000.0\r\n");
        let s: f64 = 1000.0;
        assert_eq!(f64::decode(&mut buf).unwrap(), s);
        Ok(())
    }

    #[test]
    fn test_bytes_mut() -> Result<()> {
        let mut buf = BytesMut::from("10000000_00000_00000\r\n");

        let _ = buf.split_to(10);

        println!("{:?}", buf);

        Ok(())
    }
}
