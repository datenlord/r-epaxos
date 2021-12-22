use pro_macro::FromInner;
use serde::{Deserialize, Serialize};

use crate::config::Configure;

/// The Replica Id
#[derive(Debug, Copy, Clone, FromInner, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct ReplicaId(usize);

/// The local instance id
#[derive(Debug, Copy, Clone, FromInner, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct LocalInstanceId(usize);

/// The global instance id
#[derive(Debug, Serialize, Deserialize, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct InstanceId {
    pub(crate) replica: ReplicaId,
    pub(crate) local: LocalInstanceId,
}

impl InstanceId {
    pub(crate) fn new(replica: ReplicaId, local: LocalInstanceId) -> Self {
        Self { replica, local }
    }
}

/// The seq num, which break the dependent circle while executing
#[derive(Debug, Copy, Clone, FromInner, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct Seq(usize);

/// Ballot number
#[derive(Debug, Copy, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct Ballot {
    epoch: usize,
    base: usize,
    replica: ReplicaId,
}

impl Ballot {
    pub(crate) fn new(replica: ReplicaId, conf: &Configure) -> Ballot {
        Ballot {
            replica,
            epoch: conf.epoch,
            base: 0,
        }
    }

    pub(crate) fn is_init(&self) -> bool {
        self.base == 0
    }

    #[cfg(test)]
    pub(crate) fn new_with_epoch(replica: ReplicaId, epoch: usize) -> Ballot {
        Ballot {
            replica,
            epoch,
            base: 0,
        }
    }
}

#[cfg(test)]
impl Default for Ballot {
    fn default() -> Self {
        Self {
            epoch: Default::default(),
            base: Default::default(),
            replica: ReplicaId(0),
        }
    }
}

/// The leader id for a instance propose, used in the message passing
#[derive(Debug, FromInner, Serialize, Deserialize)]
pub(crate) struct LeaderId(usize);

/// The acceptor id
#[derive(FromInner, Serialize, Deserialize)]
pub(crate) struct AcceptorId(usize);

impl From<ReplicaId> for LeaderId {
    fn from(r: ReplicaId) -> Self {
        Self(*r)
    }
}
