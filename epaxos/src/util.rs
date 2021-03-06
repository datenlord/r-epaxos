use std::{ops::DerefMut, sync::Arc};

use serde::{de::DeserializeOwned, Serialize};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::Mutex,
};

use crate::types::{Command, SharedInstance};

pub(crate) async fn send_message<M, T>(conn: &mut T, message: &M)
where
    M: Serialize,
    T: AsyncWriteExt + Unpin,
{
    // TODO: Report message content while meeting error
    let content = bincode::serialize(message)
        .map_err(|e| panic!("failed to serialize the message, {}", e))
        .unwrap();
    let len = (content.len() as u64).to_be_bytes();

    // FIXME: handle network error
    let _ = conn.write(&len).await;
    let _ = conn.write(&content).await;
}

pub(crate) async fn send_message_arc<M>(conn: &Arc<Mutex<TcpStream>>, message: &M)
where
    M: Serialize,
{
    let mut conn = conn.lock().await;
    let conn = conn.deref_mut();

    send_message(conn, message).await;
}

pub(crate) async fn send_message_arc2<M>(conn: &Arc<Mutex<TcpStream>>, message: &Arc<M>)
where
    M: Serialize,
{
    send_message_arc(conn, message.as_ref()).await;
}

pub(crate) async fn recv_message<M, T>(conn: &mut T) -> Option<M>
where
    M: DeserializeOwned,
    T: AsyncReadExt + Unpin,
{
    let mut len_buf: [u8; 8] = [0; 8];

    let read_size = read_from_stream(conn, &mut len_buf).await;
    if read_size != 8 {
        return None;
    }

    let expected_len = u64::from_be_bytes(len_buf);
    let mut buf: Vec<u8> = vec![0; expected_len as usize];

    let read_size = read_from_stream(conn, &mut buf).await;
    if read_size != expected_len as usize {
        return None;
    }

    Some(
        bincode::deserialize(&buf)
            .map_err(|e| panic!("Deserialize message failed, {} ", e))
            .unwrap(),
    )
}

async fn read_from_stream<T>(stream: &mut T, buf: &mut [u8]) -> usize
where
    T: AsyncReadExt + Unpin,
{
    let expect_len = buf.len();
    let mut has_read: usize = 0;
    while has_read != expect_len {
        let read_size = stream
            .read(&mut buf[has_read..])
            .await
            .map_err(|e| panic!("tcp link should read {} bytes message, {}", expect_len, e))
            .unwrap();
        // peer has closed
        if read_size == 0 {
            break;
        }

        has_read += read_size;
    }
    has_read
}

pub(crate) async fn instance_exist<C>(ins: &Option<SharedInstance<C>>) -> bool
where
    C: Command + Clone,
{
    if ins.is_some() {
        let ins = ins.as_ref().unwrap();
        let ins_read = ins.get_instance_read().await;
        ins_read.is_some()
    } else {
        false
    }
}
