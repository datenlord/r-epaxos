use std::io;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum CommitError {}

#[derive(Error, Debug)]
pub enum ExecuteError {
    #[error("invalid command {0} ")]
    InvalidCommand(String),
    #[error("meet io related error")]
    IoError(#[from] io::Error),
}

#[derive(Error, Debug)]
pub enum RpcError {
    #[error("meet io related error")]
    IoError(#[from] io::Error),
}
