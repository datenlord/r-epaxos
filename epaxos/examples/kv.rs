use std::{io, time::Duration};

use log::{debug, info};
use rpaxos::{
    client::{RpcClient, TcpRpcClient},
    config::Configure,
    error,
    server::DefaultServer,
    Command, CommandExecutor,
};
use serde::{Deserialize, Serialize};
use tokio::time::timeout;

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
}

#[derive(Clone, Copy, Debug)]
struct TestCommandExecutor {}

#[async_trait::async_trait]
impl CommandExecutor<TestCommand> for TestCommandExecutor {
    async fn execute(&self, cmd: &TestCommand) -> Result<(), error::ExecuteError> {
        info!("execute command {:?}", cmd);
        Ok(())
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

    let cmd_exe = TestCommandExecutor {};

    let mut server = Vec::with_capacity(3);
    for c in (0..3).map(|id| Configure::new(3, peer.to_vec(), id, 0)) {
        server.push(DefaultServer::<TestCommand, TestCommandExecutor>::new(c, cmd_exe).await);
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

    let mut client = TcpRpcClient::<TestCommand>::new(Configure::new(3, peer, 0, 0), 0).await;
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
