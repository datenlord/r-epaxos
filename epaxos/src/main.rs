pub mod client;
pub mod config;
pub mod error;
pub mod message;
pub mod server;
mod types;
mod util;

use std::time::Duration;

use client::{RpcClient, TcpRpcClient};
use config::Configure;
use log::debug;
use serde::{Deserialize, Serialize};
use server::Server;
use tokio::{io, time::timeout};
use types::Command;

#[derive(Debug, Clone, Serialize, Deserialize)]
enum TestCommandOp {
    Read,
    Write,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TestCommand {
    key: String,
    value: Option<String>,
    op: TestCommandOp,
}

#[async_trait::async_trait]
impl Command for TestCommand {
    type K = String;

    fn key(&self) -> &Self::K {
        &self.key
    }

    async fn execute<F>(&self, f: F) -> Result<(), error::ExecuteError>
    where
        F: Fn(&Self) -> Result<(), error::ExecuteError> + Send,
    {
        f(self)
    }
}

#[tokio::main]
async fn main() -> io::Result<()> {
    env_logger::init();
    let peer = vec![
        "localhost:9000".to_owned(),
        "localhost:9001".to_owned(),
        "localhost:9002".to_owned(),
    ];

    let mut server = Vec::with_capacity(3);
    for c in (0..3).map(|id| Configure::new(3, peer.to_vec(), id)) {
        server.push(Server::<TestCommand>::new(c).await);
    }

    let handles: Vec<_> = server
        .into_iter()
        .map(|s| {
            tokio::spawn(timeout(Duration::from_secs(10), async move {
                s.run().await;
            }))
        })
        .collect();

    debug!("spawn servers");

    let mut client = TcpRpcClient::<TestCommand>::new(Configure::new(3, peer, 0), 0).await;
    client
        .propose(vec![TestCommand {
            key: "k1".to_owned(),
            value: Some("v1".to_owned()),
            op: TestCommandOp::Write,
        }])
        .await;

    debug!("after client propose");
    // The server will never end until someone kills is
    for h in handles {
        let _ = h.await?;
    }

    Ok(())
}
