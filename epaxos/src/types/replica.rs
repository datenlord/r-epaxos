use std::{collections::HashMap, fmt::Debug, iter, marker::PhantomData, sync::Arc};

use itertools::Itertools;
use tokio::sync::mpsc;

use crate::execute::Executor;

use super::{
    cmd::{Command, CommandExecutor},
    id::{InstanceId, LocalInstanceId, ReplicaId, Seq},
    instance::{Instance, SharedInstance},
    space::InstanceSpace,
};

type Conflict<C> = Vec<HashMap<<C as Command>::K, SharedInstance<C>>>;
pub(crate) struct Replica<C, E, S>
where
    C: Command + Clone + Send + Sync + 'static,
    E: CommandExecutor<C> + Clone,
    S: InstanceSpace<C>,
{
    pub(crate) id: ReplicaId,
    pub(crate) peer_cnt: usize,
    pub(crate) instance_space: Arc<S>,
    /// Current max instance id for each replica, init values are 0s
    pub(crate) cur_instance: Vec<LocalInstanceId>,
    pub(crate) commited_upto: Vec<Option<LocalInstanceId>>,
    pub(crate) conflict: Conflict<C>,
    pub(crate) max_seq: Seq,
    pub(crate) exec_send: mpsc::Sender<SharedInstance<C>>,
    phantom: PhantomData<E>,
}

impl<C, E, S> Replica<C, E, S>
where
    C: Command + Debug + Clone + Sync + Send + 'static,
    E: CommandExecutor<C> + Debug + Clone + Sync + Send + 'static,
    S: InstanceSpace<C> + Send + Sync + 'static,
{
    pub(crate) async fn new(id: usize, peer_cnt: usize, cmd_exe: E) -> Self {
        let instance_space = Arc::new(S::new(peer_cnt));
        let mut cur_instance = Vec::with_capacity(peer_cnt);
        let mut commited_upto = Vec::with_capacity(peer_cnt);
        let mut conflict = Vec::with_capacity(peer_cnt);

        for _ in 0..peer_cnt {
            cur_instance.push(0.into());
            commited_upto.push(None);
            conflict.push(HashMap::new())
        }

        let (sender, receiver) = mpsc::channel(10);

        let space_clone = Clone::clone(&instance_space);
        tokio::spawn(async move {
            let executor = Executor::new(space_clone, cmd_exe);
            executor.execute(receiver).await;
        });

        Self {
            id: id.into(),
            peer_cnt,
            instance_space,
            cur_instance,
            commited_upto,
            conflict,
            max_seq: 0.into(),
            exec_send: sender,
            phantom: PhantomData,
        }
    }

    // Helper function
    pub(crate) fn cur_instance(&self, r: &ReplicaId) -> LocalInstanceId {
        self.cur_instance[**r]
    }

    pub(crate) fn set_cur_instance(&mut self, inst: &InstanceId) {
        self.cur_instance[*inst.replica] = inst.local;
    }

    pub(crate) fn local_cur_instance(&self) -> &LocalInstanceId {
        self.cur_instance.get(*self.id).unwrap()
    }

    pub(crate) fn inc_local_cur_instance(&mut self) -> &LocalInstanceId {
        let mut ins_id = self.cur_instance.get_mut(*self.id).unwrap();
        ins_id += 1;
        ins_id
    }

    // it's not a pure function
    pub(crate) fn merge_seq_deps(
        &self,
        ins: &mut Instance<C>,
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
        let mut new_seq = 0.into();
        let mut deps: Vec<Option<LocalInstanceId>> =
            iter::repeat_with(|| None).take(self.peer_cnt).collect();
        for (r_id, command) in (0..self.peer_cnt).cartesian_product(cmds.iter()) {
            if r_id != *self.id {
                if let Some(ins) = self.conflict[r_id].get(command.key()) {
                    let conflict_ins = ins.get_instance_read().await;
                    if conflict_ins.is_none() {
                        continue;
                    }
                    let conflict_ins = SharedInstance::get_raw_read(conflict_ins);
                    // update deps
                    deps[r_id] = match deps[r_id] {
                        Some(dep_ins_id) => {
                            if conflict_ins.local_id() > dep_ins_id {
                                Some(conflict_ins.local_id())
                            } else {
                                Some(dep_ins_id)
                            }
                        }
                        None => Some(conflict_ins.local_id()),
                    };
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
                    let conflict_ins = ins.get_instance_read().await;
                    if conflict_ins.is_none() {
                        continue;
                    }
                    let conflict_ins = SharedInstance::get_raw_read(conflict_ins);
                    if deps[r_id].is_some() && deps[r_id].unwrap() < conflict_ins.local_id() {
                        changed = true;

                        // update deps
                        deps[r_id] = match deps[r_id] {
                            Some(dep_ins_id) => {
                                if conflict_ins.local_id() > dep_ins_id {
                                    Some(conflict_ins.local_id())
                                } else {
                                    Some(dep_ins_id)
                                }
                            }
                            None => Some(conflict_ins.local_id()),
                        };

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

    pub(crate) async fn update_conflict(
        &mut self,
        replica: &ReplicaId,
        cmds: &[C],
        new_inst: SharedInstance<C>,
    ) {
        for c in cmds {
            let new_inst = match self.conflict[**replica].get(c.key()) {
                None => Some(new_inst.clone()),
                Some(ins) => {
                    let ins = ins.get_instance_read().await;
                    if ins.is_some()
                        && SharedInstance::get_raw_read(ins).local_id()
                            >= SharedInstance::get_raw_read(new_inst.get_instance_read().await)
                                .local_id()
                    {
                        None
                    } else {
                        Some(new_inst.clone())
                    }
                }
            };

            if new_inst.is_some() {
                self.conflict[*self.id].insert(c.key().clone(), new_inst.unwrap());
            }
        }
    }
}
