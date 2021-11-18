use crate::error::ExecuteError;

pub trait Command {
    type K;
    type V;

    fn key() -> Self::K;

    fn is_write() -> bool;

    fn execute() -> Result<(), ExecuteError>;
}

/// The status of the instance, which is recorded in the instance space
enum InstanceStatus {
    PreAccepted,
    PreAcceptedEq,
    Accepted,
    Committed,
    Executed,
}

/// The Replica Id
pub(crate) struct ReplicaId {
    id: u32,
}

impl From<u32> for ReplicaId {
    fn from(id: u32) -> Self {
        ReplicaId {
            id
        }
    }
}

/// The local instance id
pub(crate) struct LocalInstanceId {
    id: u32,
}

/// The global instance id
pub(crate) struct InstanceId {
    replica: ReplicaId,
    inner: LocalInstanceId,
}

/// The seq num, which break the dependent circle while executing
pub(crate) struct SeqNum {
    seq: u32,
}

/// Ballot number
pub(crate) struct Ballot {
    ballot_num: u32,
}

/// The instance stored in the instance space
pub struct Instance<C>
where
    C: Command,
{
    seq: SeqNum,
    ballot: Ballot,
    cmds: Vec<C>,
    deps: Vec<InstanceId>,
    status: InstanceStatus,
}

/// The leader id for a instance propose, used in the message passing
pub(crate) struct LeaderId {
    id: u32,
}

pub(crate) struct AcceptorId {
    id: u32,
}