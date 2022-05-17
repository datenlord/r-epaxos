use std::{
    collections::HashMap,
    fmt::Debug,
    marker::PhantomData,
    sync::{Arc, Mutex},
};

use crate::{
    config::Configure,
    message::{Message, Propose, SingleCmdResult},
    types::Command,
    util,
};
use async_trait::async_trait;
use log::trace;
use serde::{de::DeserializeOwned, Serialize};
use tokio::{io::WriteHalf, net::TcpStream, sync::oneshot};

#[async_trait]
pub trait RpcClient<C>
where
    C: Command,
{
    async fn propose(&mut self, cmds: Vec<C>) -> Vec<Result<C::ER, String>>;
}

struct ResultSender<C: Command>(oneshot::Sender<Vec<Result<C::ER, String>>>);
struct ResultReceiver<C: Command>(oneshot::Receiver<Vec<Result<C::ER, String>>>);

pub struct TcpRpcClient<C>
where
    C: Command,
{
    #[allow(dead_code)]
    // this filed will be used in the membership change
    conf: Configure,
    stream: WriteHalf<TcpStream>,
    phantom: PhantomData<C>,
    req_map: Arc<Mutex<HashMap<String, ResultSender<C>>>>,
}

impl<C> TcpRpcClient<C>
where
    C: Command + Serialize + DeserializeOwned + Debug + Send + Sync + 'static,
{
    fn register(&self) -> (String, ResultReceiver<C>) {
        let mut map = self.req_map.lock().unwrap();
        let (tx, rx) = oneshot::channel();
        loop {
            let uuid = uuid::Uuid::new_v4();
            let uuid_str = uuid.urn().to_string();
            if map.contains_key(&uuid_str) {
                continue;
            }
            map.insert(uuid_str.to_owned(), ResultSender(tx));
            return (uuid_str, ResultReceiver(rx));
        }
    }

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

        let req_map: Arc<Mutex<HashMap<String, ResultSender<C>>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let req_map_clone = req_map.clone();
        let (mut read_stream, write_stream) = tokio::io::split(stream);
        tokio::spawn(async move {
            loop {
                let response: Message<C> = util::recv_message(&mut read_stream).await;
                if let Message::ProposeResponse(pr) = response {
                    let mut locked_map = req_map_clone.lock().unwrap();
                    let tx = locked_map.remove(&pr.cmd_id);

                    let results = pr
                        .results
                        .into_iter()
                        .map(|r| match r {
                            SingleCmdResult::ExecuteResult(execute_result) => Ok(execute_result),
                            SingleCmdResult::Error(error_msg) => Err(error_msg),
                        })
                        .collect();
                    tx.map(|tx| tx.0.send(results));
                }
            }
        });

        Self {
            conf,
            stream: write_stream,
            phantom: PhantomData,
            req_map,
        }
    }
}

#[async_trait]
impl<C> RpcClient<C> for TcpRpcClient<C>
where
    C: Command + Serialize + DeserializeOwned + Debug + Send + Sync + 'static,
{
    async fn propose(&mut self, cmds: Vec<C>) -> Vec<Result<C::ER, String>> {
        trace!("start propose");
        let (id, rx) = self.register();
        let propose = Message::Propose(Propose { cmd_id: id, cmds });
        util::send_message(&mut self.stream, &propose).await;
        rx.0.await.unwrap()
    }
}
