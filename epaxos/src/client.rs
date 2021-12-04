use std::{fmt::Debug, marker::PhantomData};

use crate::{
    config::Configure,
    message::{Message, Propose},
    types::Command,
    util,
};
use async_trait::async_trait;
use log::trace;
use serde::Serialize;
use tokio::net::TcpStream;

#[async_trait]
pub trait RpcClient<C>
where
    C: Command,
{
    // TODO: check return value
    async fn propose(&mut self, cmds: Vec<C>);
}

pub struct TcpRpcClient<C>
where
    C: Command,
{
    #[allow(dead_code)]
    // this filed will be used in the membership change
    conf: Configure,
    stream: TcpStream,
    phantom: PhantomData<C>,
}

impl<C> TcpRpcClient<C>
where
    C: Command + Serialize + Debug + Send + Sync + 'static,
{
    pub async fn new(conf: Configure, id: usize) -> Self {
        let conn_str = conf
            .peer
            .get(id)
            .or_else(|| panic!("id {} is not in the configure scope", id))
            .unwrap();
        let stream = TcpStream::connect(conn_str)
            .await
            .map_err(|_e| panic!("connec to {} epaxos peer failed", id))
            .unwrap();

        Self {
            conf,
            stream,
            phantom: PhantomData,
        }
    }
}

#[async_trait]
impl<C> RpcClient<C> for TcpRpcClient<C>
where
    C: Command + Serialize + Debug + Send + Sync + 'static,
{
    async fn propose(&mut self, cmds: Vec<C>) {
        trace!("start propose");
        let propose = Message::Propose(Propose { cmds });
        util::send_message(&mut self.stream, &propose).await;
    }
}
