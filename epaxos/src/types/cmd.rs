use async_trait::async_trait;

use crate::error::ExecuteError;
use std::hash::Hash;

#[cfg(test)]
use super::InstanceId;
#[cfg(test)]
use tokio::sync::mpsc::Sender;

#[async_trait]
pub trait Command: Sized {
    /// K is used to tell confliction
    type K: Eq + Hash + Send + Sync + Clone + 'static;

    fn key(&self) -> &Self::K;

    async fn execute<E>(&self, e: &E) -> Result<(), ExecuteError>
    where
        E: CommandExecutor<Self> + Send + Sync,
    {
        <E as CommandExecutor<Self>>::execute(e, self).await
    }
}

#[async_trait]
pub trait CommandExecutor<C>
where
    C: Command,
{
    async fn execute(&self, cmd: &C) -> Result<(), ExecuteError>;
}

#[cfg(test)]
#[derive(PartialEq, Eq, Clone, Copy, Debug)]
pub(crate) struct MockCommandId {
    instance_id: InstanceId,
    cmd_id: usize,
}

#[cfg(test)]
impl MockCommandId {
    pub(crate) fn new(instance_id: InstanceId, cmd_id: usize) -> Self {
        Self {
            instance_id,
            cmd_id,
        }
    }
}

#[cfg(test)]
#[derive(Clone, Debug)]
pub(crate) struct MockCommand {
    key: String,
    value: MockCommandId,
}

#[cfg(test)]
impl MockCommand {
    pub(crate) fn new(key: &str, value: MockCommandId) -> Self {
        Self {
            key: key.to_owned(),
            value,
        }
    }
    pub(crate) fn value(&self) -> MockCommandId {
        self.value
    }
}

#[cfg(test)]
#[async_trait]
impl Command for MockCommand {
    type K = String;

    fn key(&self) -> &Self::K {
        &self.key
    }
}

#[cfg(test)]
#[derive(Clone, Debug)]
pub(crate) struct MockCommandExecutor {
    cmd_sender: Option<Sender<MockCommandId>>,
}

#[cfg(test)]
impl MockCommandExecutor {
    pub(crate) fn new(cmd_sender: Option<Sender<MockCommandId>>) -> Self {
        Self { cmd_sender }
    }
}

#[cfg(test)]
#[async_trait]
impl CommandExecutor<MockCommand> for MockCommandExecutor {
    async fn execute(&self, cmd: &MockCommand) -> Result<(), ExecuteError> {
        if self.cmd_sender.is_some() {
            let _ = self.cmd_sender.as_ref().unwrap().send(cmd.value()).await;
        }
        Ok(())
    }
}
