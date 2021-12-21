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
#[derive(Debug, Serialize, Deserialize, Copy, Clone)]
pub(crate) struct InstanceId {
    pub(crate) replica: ReplicaId,
    pub(crate) inner: LocalInstanceId,
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
