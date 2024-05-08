use super::*;

// - integer :[<+|->]<value>\r\n
impl RespEncoder for i64 {
    fn encode(self) -> Vec<u8> {
        format!(":{:+}\r\n", self).into_bytes()
    }
}

// - simple string: "+<value>\r\n"
impl RespEncoder for SimpleString {
    fn encode(self) -> Vec<u8> {
        format!("+{}\r\n", self.deref()).into_bytes()
    }
}

// - error: "-<value>\r\n"
impl RespEncoder for SimpleError {
    fn encode(self) -> Vec<u8> {
        format!("-{}\r\n", self.0).into_bytes()
    }
}

// - bulk string: "$<length>\r\n<value>\r\n"
impl RespEncoder for BulkString {
    fn encode(self) -> Vec<u8> {
        let mut buf: Vec<_> = Vec::with_capacity(self.len() + 16);
        buf.extend_from_slice(&format!("${}\r\n", self.len()).into_bytes());
        buf.extend_from_slice(&self);
        buf.extend_from_slice(b"\r\n");
        buf
    }
}

// - null bulk string: "$-1\r\n"
impl RespEncoder for RespNullBulkString {
    fn encode(self) -> Vec<u8> {
        b"$-1\r\n".to_vec()
    }
}

// - null: "_\r\n"
impl RespEncoder for RespNull {
    fn encode(self) -> Vec<u8> {
        b"_\r\n".to_vec()
    }
}

// - null array: "*-1\r\n"
impl RespEncoder for RespNullArray {
    fn encode(self) -> Vec<u8> {
        b"*-1\r\n".to_vec()
    }
}

// - boolean: "#<value>\r\n"
impl RespEncoder for bool {
    fn encode(self) -> Vec<u8> {
        format!("#{}\r\n", if self { "t" } else { "f" }).into_bytes()
    }
}

// - douber:  ,[<+|->]<integral>[.<fractional>][<E|e>[sign]<exponent>]\r\n
impl RespEncoder for f64 {
    fn encode(self) -> Vec<u8> {
        let ret = if self.abs() > 1e+8 || self.abs() < 1e-8 {
            format!(",{:+e}\r\n", self)
        } else {
            let sign = if self.is_sign_positive() { "+" } else { "" };
            format!(",{}{}\r\n", sign, self)
        };
        ret.into_bytes()
    }
}

// - array: "*<length-for-elements>\r\n<element-1>..<element-n>"
const ARRAY_CAP: usize = 4096;
impl RespEncoder for RespArray {
    fn encode(self) -> Vec<u8> {
        let mut buf: Vec<_> = Vec::with_capacity(ARRAY_CAP);
        buf.extend_from_slice(&format!("*{}\r\n", self.len()).into_bytes());
        for frame in self.0 {
            buf.extend_from_slice(&frame.encode());
        }
        buf
    }
}

const BUF_CAP: usize = 4096;
// - map: %<number-of-entries>\r\n<key-1><value-1>...<key-n><value-n>
impl RespEncoder for RespMap {
    fn encode(self) -> Vec<u8> {
        let mut buf: Vec<_> = Vec::with_capacity(BUF_CAP);
        buf.extend_from_slice(&format!("%{}\r\n", self.len()).into_bytes());
        for (key, value) in self.0 {
            buf.extend_from_slice(&SimpleString::new(key).encode());
            buf.extend_from_slice(&value.encode());
        }
        buf
    }
}

// -set: ~<number-of-elements>\r\n<element-1>...<element-n>
impl RespEncoder for RespSet {
    fn encode(self) -> Vec<u8> {
        let mut buf: Vec<_> = Vec::with_capacity(BUF_CAP);
        buf.extend_from_slice(&format!("~{}\r\n", self.len()).into_bytes());
        for frame in self.0 {
            buf.extend_from_slice(&frame.encode());
        }
        buf
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_simple_string_encode() {
        use super::*;
        let s: RespFrame = SimpleString::new("hello").into();
        assert_eq!(s.encode(), b"+hello\r\n");
    }

    #[test]
    fn test_simple_error_encode() {
        let s: RespFrame = SimpleError::new("error").into();
        assert_eq!(s.encode(), b"-error\r\n");
    }

    #[test]
    fn test_integer_encode() {
        let s: RespFrame = 123.into();
        assert_eq!(s.encode(), b":+123\r\n");
        let s: RespFrame = (-123).into();
        assert_eq!(s.encode(), b":-123\r\n");
    }

    #[test]
    fn test_bulk_string_encode() {
        let s: RespFrame = BulkString::new(b"hello".to_vec()).into();
        assert_eq!(s.encode(), b"$5\r\nhello\r\n");
    }

    #[test]
    fn test_built_string_null_encode() {
        let s: RespFrame = RespNullBulkString.into();
        assert_eq!(s.encode(), b"$-1\r\n");
    }

    #[test]
    fn test_null_encode() {
        let s: RespFrame = RespNull.into();
        assert_eq!(s.encode(), b"_\r\n");
    }

    #[test]
    fn test_null_array_encode() {
        let s: RespFrame = RespNullArray.into();
        assert_eq!(s.encode(), b"*-1\r\n");
    }

    #[test]
    fn test_boolean_encode() {
        let s: RespFrame = true.into();
        assert_eq!(s.encode(), b"#t\r\n");
        let s: RespFrame = false.into();
        assert_eq!(s.encode(), b"#f\r\n");
    }

    #[test]
    fn test_array_encode() {
        let array: Vec<RespFrame> = vec![1.into(), 2.into(), 3.into()];
        let s: RespFrame = RespArray::new(array).into();
        // println!("{}",String::from_utf8_lossy(&s.encode()));
        assert_eq!(s.encode(), b"*3\r\n:+1\r\n:+2\r\n:+3\r\n");
    }

    #[test]
    fn test_map_encode() {
        let mut s: RespMap = RespMap::new();
        s.insert("hello".into(), BulkString::new(b"world".to_vec()).into());
        s.insert("foo".into(), (-123456.789).into());
        let s: RespFrame = s.into();
        // println!("{}",String::from_utf8_lossy(&s.encode()));
        assert_eq!(
            s.encode(),
            b"%2\r\n+foo\r\n,-123456.789\r\n+hello\r\n$5\r\nworld\r\n"
        );
    }

    #[test]
    fn test_set_encode() {
        let array: Vec<RespFrame> = vec![
            1.into(),
            2.into(),
            3.into(),
            SimpleString::new("hello").into(),
        ];
        let s: RespSet = RespSet::new(array);
        // println!("{}",String::from_utf8_lossy(&s.encode()));
        assert_eq!(s.encode(), b"~4\r\n:+1\r\n:+2\r\n:+3\r\n+hello\r\n");
    }

    #[test]
    fn test_f64_encode() {
        let s: RespFrame = 123.456.into();
        assert_eq!(s.encode(), b",+123.456\r\n");
        let s: RespFrame = 1.23456e+8.into();
        assert_eq!(s.encode(), b",+1.23456e8\r\n");
        let s: RespFrame = (-1.23456e-9).into();
        assert_eq!(s.encode(), b",-1.23456e-9\r\n");
    }
}
