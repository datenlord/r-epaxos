use crate::error::ExecuteError;
use std::hash::Hash;

#[async_trait::async_trait]
pub trait Command: Sized {
    /// K is used to tell confliction
    type K: Eq + Hash + Send + Sync + Clone + 'static;

    fn key(&self) -> &Self::K;

    async fn execute<E>(&self, e: &E) -> Result<(), ExecuteError>
    where
        E: CommandExecutor<Self> + Send + Sync;
}

#[async_trait::async_trait]
pub trait CommandExecutor<C>
where
    C: Command,
{
    async fn execute(&self, cmd: &C) -> Result<(), ExecuteError>;
}
