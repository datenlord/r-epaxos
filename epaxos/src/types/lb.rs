use crate::config::Configure;

use super::id::{Ballot, ReplicaId};

#[derive(Debug)]
#[cfg_attr(test, derive(Default))]
pub(crate) struct LeaderBook {
    pub(crate) accept_ok: usize,
    pub(crate) preaccept_ok: usize,
    pub(crate) nack: usize,
    pub(crate) max_ballot: Ballot,
    pub(crate) all_equal: bool,
}

impl LeaderBook {
    pub(crate) fn new(replica: ReplicaId, conf: &Configure) -> Self {
        LeaderBook {
            accept_ok: 0,
            preaccept_ok: 0,
            nack: 0,
            max_ballot: Ballot::new(replica, conf),
            all_equal: true,
        }
    }
}
