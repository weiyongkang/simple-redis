use crate::{
    cmd::{extract_args, validate_command},
    RespArray, RespFrame, RespMap,
};

use super::{CommandError, CommandExecutor, HGet, HGetAll, HSet, RESP_OK};

impl CommandExecutor for HGet {
    fn execute(self, backend: &crate::Backend) -> RespFrame {
        match backend.hget(&self.key, &self.field) {
            Some(value) => value,
            None => RespFrame::Null(crate::RespNull),
        }
    }
}

impl CommandExecutor for HSet {
    fn execute(self, backend: &crate::Backend) -> RespFrame {
        backend.hset(self.key, self.field, self.value.clone());
        RESP_OK.clone()
    }
}

impl CommandExecutor for HGetAll {
    fn execute(self, backend: &crate::Backend) -> RespFrame {
        let hmap = backend.hgetall(&self.key);
        match hmap {
            Some(m) => {
                let mut frames = RespMap::new();
                for v in m.iter() {
                    let key = v.key().to_string();
                    let value = v.value().clone();
                    frames.insert(key, value);
                }
                frames.into()
            }
            None => RespFrame::Array(crate::RespArray::new(Vec::new())),
        }
    }
}

impl TryFrom<RespArray> for HGet {
    type Error = CommandError;
    fn try_from(value: RespArray) -> Result<Self, Self::Error> {
        validate_command(&value, &["hget"], 2)?;
        let mut args = extract_args(value, 1)?.into_iter();
        match (args.next(), args.next()) {
            (Some(RespFrame::BulkString(key)), Some(RespFrame::BulkString(field))) => Ok(HGet {
                key: String::from_utf8(key.0)?,
                field: String::from_utf8(field.0)?,
            }),
            _ => Err(CommandError::InvalidArgument(
                "Invalid key or field".to_string(),
            )),
        }
    }
}

impl TryFrom<RespArray> for HSet {
    type Error = CommandError;
    fn try_from(value: RespArray) -> Result<Self, Self::Error> {
        validate_command(&value, &["hset"], 3)?;
        let mut args = extract_args(value, 1)?.into_iter();
        match (args.next(), args.next(), args.next()) {
            (Some(RespFrame::BulkString(key)), Some(RespFrame::BulkString(field)), Some(value)) => {
                Ok(HSet {
                    key: String::from_utf8(key.0)?,
                    field: String::from_utf8(field.0)?,
                    value,
                })
            }
            _ => Err(CommandError::InvalidArgument(
                "Invalid key, field or value".to_string(),
            )),
        }
    }
}

impl TryFrom<RespArray> for HGetAll {
    type Error = CommandError;
    fn try_from(value: RespArray) -> Result<Self, Self::Error> {
        validate_command(&value, &["hgetall"], 1)?;
        let mut args = extract_args(value, 1)?.into_iter();
        match args.next() {
            Some(RespFrame::BulkString(key)) => Ok(HGetAll {
                key: String::from_utf8(key.0)?,
            }),
            _ => Err(CommandError::InvalidArgument("Invalid key".to_string())),
        }
    }
}

#[cfg(test)]
mod tests {

    use bytes::BytesMut;

    use crate::{BulkString, RespDecoder};

    use super::*;

    #[test]
    fn test_hget_try_from_resp_array() -> anyhow::Result<()> {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(b"*3\r\n$4\r\nhget\r\n$3\r\nkey\r\n$5\r\nfield\r\n");
        let cmd: RespArray = RespArray::decode(&mut buf)?;
        eprintln!("{:?}", cmd);
        let hget: HGet = cmd.try_into()?;
        assert_eq!(hget.key, "key");
        assert_eq!(hget.field, "field");
        Ok(())
    }

    #[test]
    fn test_hset_try_from_resp_array() -> anyhow::Result<()> {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(b"*4\r\n$4\r\nhset\r\n$3\r\nkey\r\n$5\r\nfield\r\n$5\r\nvalue\r\n");
        let cmd: RespArray = RespArray::decode(&mut buf)?;
        eprintln!("{:?}", cmd);
        let hset: HSet = cmd.try_into()?;
        let value = BulkString::new("value".to_string());
        assert_eq!(hset.key, "key");
        assert_eq!(hset.field, "field");
        assert_eq!(hset.value, RespFrame::BulkString(value));
        Ok(())
    }

    #[test]
    fn test_hgetall_try_from_resp_array() -> anyhow::Result<()> {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(b"*2\r\n$7\r\nhgetall\r\n$3\r\nkey\r\n");
        let cmd: RespArray = RespArray::decode(&mut buf)?;
        eprintln!("{:?}", cmd);
        let hgetall: HGetAll = cmd.try_into()?;
        assert_eq!(hgetall.key, "key");
        Ok(())
    }
}
