use crate::types::{Ballot, Command, InstanceId, LeaderId, LocalInstanceId, Seq};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct PreAccept<C>
where
    C: Command,
{
    pub(crate) leader_id: LeaderId,
    pub(crate) instance_id: InstanceId,
    pub(crate) seq: Seq,
    pub(crate) ballot: Ballot,
    pub(crate) cmds: Vec<C>,
    pub(crate) deps: Vec<Option<LocalInstanceId>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct PreAcceptReply {
    pub(crate) instance_id: InstanceId,
    pub(crate) seq: Seq,
    pub(crate) ballot: Ballot,
    pub(crate) ok: bool,
    pub(crate) deps: Vec<Option<LocalInstanceId>>,
    pub(crate) committed_deps: Vec<Option<LocalInstanceId>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct PreAcceptOk {
    pub(crate) instance_id: InstanceId,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct Accept {
    pub(crate) leader_id: LeaderId,
    pub(crate) instance_id: InstanceId,
    pub(crate) ballot: Ballot,
    pub(crate) seq: Seq,
    pub(crate) cmd_cnt: usize,
    pub(crate) deps: Vec<Option<LocalInstanceId>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct AcceptReply {
    pub(crate) instance_id: InstanceId,
    pub(crate) ok: bool,
    pub(crate) ballot: Ballot,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct Commit<C>
where
    C: Command,
{
    pub(crate) leader_id: LeaderId,
    pub(crate) instance_id: InstanceId,
    pub(crate) seq: Seq,
    pub(crate) cmds: Vec<C>,
    pub(crate) deps: Vec<Option<LocalInstanceId>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct CommitShort {
    leader_id: LeaderId,
    instance_id: InstanceId,
    seq: Seq,
    cmd_cnt: usize,
    deps: Vec<InstanceId>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Propose<C>
where
    C: Command + Serialize,
{
    pub(crate) cmds: Vec<C>,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) enum Message<C>
where
    C: Command + Serialize,
{
    PreAccept(PreAccept<C>),
    PreAcceptReply(PreAcceptReply),
    PreAcceptOk(PreAcceptOk),
    Accept(Accept),
    AcceptReply(AcceptReply),
    Commit(Commit<C>),
    CommitShort(CommitShort),
    Propose(Propose<C>),
}
