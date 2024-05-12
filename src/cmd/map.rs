use crate::{
    cmd::{extract_args, validate_command},
    Backend, RespArray, RespFrame, RespNull,
};

use super::{CommandError, CommandExecutor, Get, Set, RESP_OK};

impl CommandExecutor for Get {
    fn execute(self, backend: &Backend) -> RespFrame {
        match backend.get(&self.key) {
            Some(value) => value,
            None => RespFrame::Null(RespNull),
        }
    }
}

impl CommandExecutor for Set {
    fn execute(self, backend: &Backend) -> RespFrame {
        backend.set(self.key, self.value.clone());
        RESP_OK.clone()
    }
}

impl TryFrom<RespArray> for Get {
    type Error = CommandError;
    fn try_from(value: RespArray) -> Result<Self, Self::Error> {
        validate_command(&value, &["get"], 1)?;
        let mut args = extract_args(value, 1)?.into_iter();
        match args.next() {
            Some(RespFrame::BulkString(s)) => Ok(Get {
                key: String::from_utf8(s.0)?,
            }),
            _ => Err(CommandError::InvalidArgument("Invalid key".to_string())),
        }
    }
}

impl TryFrom<RespArray> for Set {
    type Error = CommandError;
    fn try_from(value: RespArray) -> Result<Self, Self::Error> {
        validate_command(&value, &["set"], 2)?;
        let mut args = extract_args(value, 1)?.into_iter();
        match (args.next(), args.next()) {
            (Some(RespFrame::BulkString(key)), Some(value)) => Ok(Set {
                key: String::from_utf8(key.0)?,
                value,
            }),
            _ => Err(CommandError::InvalidArgument(
                "Invalid key or value".to_string(),
            )),
        }
    }
}

mod tests {

    #[allow(unused_imports)]
    use bytes::BytesMut;

    #[allow(unused_imports)]
    use crate::{resp, BulkString, RespDecoder};

    #[allow(unused_imports)]
    use super::*;

    #[test]
    fn test_get_try_from_resp_array() -> anyhow::Result<()> {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(b"*2\r\n$3\r\nget\r\n$3\r\nkey\r\n");
        let cmd: RespArray = RespArray::decode(&mut buf)?;
        eprintln!("{:?}", cmd);
        let get: Get = cmd.try_into()?;
        assert_eq!(get.key, "key");
        Ok(())
    }

    #[test]
    fn test_set_try_from_resp_array() -> anyhow::Result<()> {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(b"*3\r\n$3\r\nset\r\n$3\r\nkey\r\n$5\r\nvalue\r\n");
        let cmd: RespArray = RespArray::decode(&mut buf)?;
        eprintln!("{:?}", cmd);
        let set: Set = cmd.try_into()?;
        assert_eq!(set.key, "key");
        assert_eq!(set.value, RespFrame::BulkString(BulkString::new("value")));
        Ok(())
    }

    #[test]
    fn test_get_execute() {
        let backend = Backend::new();
        let get = Get {
            key: "key".to_string(),
        };
        let resp = get.execute(&backend);
        assert_eq!(resp, RespFrame::Null(RespNull));
    }

    #[test]
    fn test_set_execute() {
        let backend = Backend::new();
        let set = Set {
            key: "key".to_string(),
            value: RespFrame::BulkString(BulkString::new("value")),
        };
        let resp = set.execute(&backend);
        assert_eq!(resp, RESP_OK.clone());
        let resp = backend.get("key").unwrap();
        assert_eq!(resp, RespFrame::BulkString(BulkString::new("value")));
    }

    #[test]
    fn test_get_set_execute() {
        let backend = Backend::new();
        let set = Set {
            key: "key".to_string(),
            value: RespFrame::BulkString(BulkString::new("value")),
        };
        let resp = set.execute(&backend);
        assert_eq!(resp, RESP_OK.clone());
        let get = Get {
            key: "key".to_string(),
        };
        let resp = get.execute(&backend);
        assert_eq!(resp, RespFrame::BulkString(BulkString::new("value")));
    }
}
