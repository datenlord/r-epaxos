use crate::types::{Ballot, Command, InstanceId, LeaderId, LocalInstanceId, SeqNum};

pub(crate) struct PreAccept<C>
where
    C: Command,
{
    leader_id: LeaderId,
    instance_id: InstanceId,
    seq: SeqNum,
    ballot: Ballot,
    cmds: Vec<C>,
    deps: Vec<InstanceId>,
}

pub(crate) struct PreAcceptReply {
    instance_id: InstanceId,
    seq: SeqNum,
    ballot: Ballot,
    ok: bool,
    deps: Vec<InstanceId>,
    committed_deps: Vec<InstanceId>,
}

pub(crate) struct PreAcceptOk {
    id: LocalInstanceId,
}

pub(crate) struct Accept {
    leader_id: LeaderId,
    instance_id: InstanceId,
    ballot: Ballot,
    seq: SeqNum,
    cmd_cnt: u32,
    deps: Vec<InstanceId>,
}

pub(crate) struct AcceptReply {
    instance_id: InstanceId,
    ok: bool,
    ballot: Ballot,
}

pub(crate) struct Commit<C>
where
    C: Command,
{
    leader_id: LeaderId,
    instance_id: InstanceId,
    seq: SeqNum,
    cmds: Vec<C>,
    deps: Vec<InstanceId>,
}

pub(crate) struct CommitShort {
    leader_id: LeaderId,
    instance_id: InstanceId,
    seq: SeqNum,
    cmd_cnt: u32,
    deps: Vec<InstanceId>,
}

pub(crate) struct Propose<C>
where
    C: Command,
{
    cmds: Vec<C>,
}
