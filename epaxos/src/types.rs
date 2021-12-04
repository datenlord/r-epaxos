use std::{collections::HashMap, hash::Hash, iter, sync::Arc};

use crate::error::ExecuteError;
use itertools::Itertools;
use pro_macro::FromInner;
use serde::{Deserialize, Serialize};
use tokio::sync::{RwLock, RwLockWriteGuard};

type Conflict<C> = Vec<HashMap<<C as Command>::K, Arc<RwLock<Instance<C>>>>>;

pub struct Replica<C>
where
    C: Command,
{
    pub(crate) id: ReplicaId,
    pub(crate) peer_cnt: usize,
    pub(crate) instance_space: Vec<Vec<Arc<RwLock<Instance<C>>>>>,
    /// Current max instance id for each replica, init values are 0s
    pub(crate) cur_instance: Vec<LocalInstanceId>,
    pub(crate) commited_upto: Vec<Option<LocalInstanceId>>,
    pub(crate) conflict: Conflict<C>,
    pub(crate) max_seq: Seq,
}

impl<C> Replica<C>
where
    C: Command,
{
    pub(crate) fn new(id: usize, peer_cnt: usize) -> Self {
        let mut instance_space = Vec::with_capacity(peer_cnt);
        let mut cur_instance = Vec::with_capacity(peer_cnt);
        let mut commited_upto = Vec::with_capacity(peer_cnt);
        let mut conflict = Vec::with_capacity(peer_cnt);

        (0..peer_cnt).for_each(|_| {
            instance_space.push(vec![]);
            cur_instance.push(0.into());
            commited_upto.push(None);
            conflict.push(HashMap::new())
        });

        Self {
            id: id.into(),
            peer_cnt,
            instance_space,
            cur_instance,
            commited_upto,
            conflict,
            max_seq: 0.into(),
        }
    }

    // Helper function
    pub(crate) fn cur_instance(&self, r: &ReplicaId) -> LocalInstanceId {
        self.cur_instance[**r]
    }

    pub(crate) fn set_cur_instance(&mut self, inst: &InstanceId) {
        self.cur_instance[*inst.replica] = inst.inner;
    }

    pub(crate) fn local_cur_instance(&self) -> &LocalInstanceId {
        self.cur_instance.get(*self.id).unwrap()
    }

    pub(crate) fn inc_local_cur_instance(&mut self) -> &LocalInstanceId {
        let mut ins_id = self.cur_instance.get_mut(*self.id).unwrap();
        ins_id.0 += 1;
        ins_id
    }

    // it's not a pure function
    pub(crate) fn merge_seq_deps(
        &self,
        ins: &mut RwLockWriteGuard<Instance<C>>,
        new_seq: &Seq,
        new_deps: &[Option<LocalInstanceId>],
    ) -> bool {
        let mut equal = true;
        if &ins.seq != new_seq {
            equal = false;
            if new_seq > &ins.seq {
                ins.seq = *new_seq;
            }
        }

        ins.deps.iter_mut().zip(new_deps.iter()).for_each(|(o, n)| {
            if o != n {
                equal = false;
                if o.is_none() || (n.is_some() && o.as_ref().unwrap() < n.as_ref().unwrap()) {
                    *o = *n;
                }
            }
        });
        equal
    }

    pub(crate) async fn get_seq_deps(&self, cmds: &[C]) -> (Seq, Vec<Option<LocalInstanceId>>) {
        let mut new_seq = Seq(0);
        let mut deps: Vec<Option<LocalInstanceId>> =
            iter::repeat_with(|| None).take(self.peer_cnt).collect();
        for (r_id, command) in (0..self.peer_cnt).cartesian_product(cmds.iter()) {
            if r_id != *self.id {
                if let Some(ins) = self.conflict[r_id].get(command.key()) {
                    let conflict_ins = ins.read().await;
                    // update deps
                    deps.insert(
                        r_id,
                        match deps[r_id] {
                            Some(dep_ins_id) => {
                                if conflict_ins.id > dep_ins_id {
                                    Some(conflict_ins.id)
                                } else {
                                    Some(dep_ins_id)
                                }
                            }
                            None => Some(conflict_ins.id),
                        },
                    );
                    // update seq
                    let s = &conflict_ins.seq;
                    if s >= &new_seq {
                        new_seq = (**s + 1).into();
                    }
                }
            }
        }
        (new_seq, deps)
    }

    // TODO: merge with get_seq_deps
    pub(crate) async fn update_seq_deps(
        &self,
        mut seq: Seq,
        mut deps: Vec<Option<LocalInstanceId>>,
        cmds: &[C],
    ) -> (Seq, Vec<Option<LocalInstanceId>>, bool) {
        let mut changed = false;
        for (r_id, command) in (0..self.peer_cnt).cartesian_product(cmds.iter()) {
            if r_id != *self.id {
                if let Some(ins) = self.conflict[r_id].get(command.key()) {
                    let conflict_ins = ins.read().await;
                    if deps[r_id].is_some() && deps[r_id].unwrap() < conflict_ins.id {
                        changed = true;
                        // update deps
                        deps.insert(
                            r_id,
                            match deps[r_id] {
                                Some(dep_ins_id) => {
                                    if conflict_ins.id > dep_ins_id {
                                        Some(conflict_ins.id)
                                    } else {
                                        Some(dep_ins_id)
                                    }
                                }
                                None => Some(conflict_ins.id),
                            },
                        );
                        // update seq
                        let conflict_seq = &conflict_ins.seq;
                        if conflict_seq >= &seq {
                            seq = (**conflict_seq + 1).into();
                        }
                    }
                }
            }
        }
        (seq, deps, changed)
    }

    pub(crate) async fn update_conflict(&mut self, cmds: &[C], new_inst: Arc<RwLock<Instance<C>>>) {
        for c in cmds {
            let new_inst = match self.conflict[*self.id].get(c.key()) {
                None => Some(new_inst.clone()),
                Some(ins) => {
                    if ins.read().await.id < new_inst.read().await.id {
                        Some(new_inst.clone())
                    } else {
                        None
                    }
                }
            };

            if new_inst.is_some() {
                self.conflict[*self.id].insert(c.key().clone(), new_inst.unwrap());
            }
        }
    }
}

#[async_trait::async_trait]
pub trait Command {
    /// K is used to tell confliction
    type K: Eq + Hash + Send + Sync + Clone + 'static;

    fn key(&self) -> &Self::K;

    async fn execute<T>(&self, f: T) -> Result<(), ExecuteError>
    where
        T: Fn(&Self) -> Result<(), ExecuteError> + Send;
}

/// The status of the instance, which is recorded in the instance space
#[derive(Serialize, Deserialize)]
pub(crate) enum InstanceStatus {
    PreAccepted,
    PreAcceptedEq,
    Accepted,
    Committed,
    Executed,
}

/// The Replica Id
#[derive(Debug, Copy, Clone, FromInner, Serialize, Deserialize)]
pub(crate) struct ReplicaId(usize);

/// The local instance id
#[derive(Debug, Copy, Clone, FromInner, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct LocalInstanceId(usize);

/// The global instance id
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct InstanceId {
    pub(crate) replica: ReplicaId,
    pub(crate) inner: LocalInstanceId,
}

/// The seq num, which break the dependent circle while executing
#[derive(Debug, Copy, Clone, FromInner, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct Seq(usize);

/// Ballot number
#[derive(Debug, Copy, Clone, FromInner, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct Ballot(usize);

/// The instance stored in the instance space
pub struct Instance<C>
where
    C: Command,
{
    pub(crate) id: LocalInstanceId,
    pub(crate) seq: Seq,
    pub(crate) ballot: Ballot,
    pub(crate) cmds: Vec<C>,
    pub(crate) deps: Vec<Option<LocalInstanceId>>,
    pub(crate) status: InstanceStatus,
    pub(crate) lb: LeaderBook,
}

pub(crate) struct LeaderBook {
    pub(crate) preaccept_ok: usize,
    pub(crate) nack: usize,
    pub(crate) max_ballot: Ballot,
    pub(crate) all_equal: bool,
}

impl LeaderBook {
    pub(crate) fn new() -> Self {
        LeaderBook {
            preaccept_ok: 0,
            nack: 0,
            max_ballot: Ballot(0),
            all_equal: true,
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
impl Ballot {
    pub(crate) fn new() -> Ballot {
        Ballot(0)
    }
    pub(crate) fn is_init(&self) -> bool {
        self.0 == 0
    }
}
