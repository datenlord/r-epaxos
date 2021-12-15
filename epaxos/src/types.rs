use core::cmp::PartialOrd;
use std::{collections::HashMap, hash::Hash, iter, marker::PhantomData, sync::Arc};

use crate::{error::ExecuteError, execute::Executor, util::instance_exist};
use itertools::Itertools;
use pro_macro::FromInner;
use serde::{Deserialize, Serialize};
use tokio::sync::{
    mpsc, Notify, RwLock, RwLockMappedWriteGuard, RwLockReadGuard, RwLockWriteGuard,
};

type Conflict<C> = Vec<HashMap<<C as Command>::K, SharedInstance<C>>>;

pub(crate) struct SharedInstanceInner<C>
where
    C: Command + Clone,
{
    ins: Option<Instance<C>>,
    notify: Option<Vec<Arc<Notify>>>,
}

/// Th shared instance stored in the instance space
#[derive(Clone)]
pub(crate) struct SharedInstance<C>
where
    C: Command + Clone,
{
    inner: Arc<RwLock<SharedInstanceInner<C>>>,
}

impl<C> PartialEq for SharedInstance<C>
where
    C: Command + Clone,
{
    fn eq(&self, other: &Self) -> bool {
        Arc::as_ptr(&self.inner) == Arc::as_ptr(&other.inner)
    }
}

impl<C> Eq for SharedInstance<C> where C: Command + Clone {}

impl<C> Hash for SharedInstance<C>
where
    C: Command + Clone,
{
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        Arc::as_ptr(&self.inner).hash(state);
    }
}

impl<C> SharedInstance<C>
where
    C: Command + Clone,
{
    pub(crate) fn none() -> Self {
        Self::new(None, None)
    }

    pub(crate) fn new(instance: Option<Instance<C>>, notify: Option<Vec<Arc<Notify>>>) -> Self {
        Self {
            inner: Arc::new(RwLock::new(SharedInstanceInner {
                ins: instance,
                notify,
            })),
        }
    }

    pub(crate) async fn match_status(&self, status: &[InstanceStatus]) -> bool {
        let d_ins_read = self.get_instance_read().await;
        if d_ins_read.is_none() {
            return false;
        }

        let d_ins_read_inner = d_ins_read.as_ref().unwrap();
        status.contains(&d_ins_read_inner.status)
    }

    pub(crate) async fn get_instance_read(&'_ self) -> RwLockReadGuard<'_, Option<Instance<C>>> {
        RwLockReadGuard::map(self.inner.read().await, |i| &i.ins)
    }

    pub(crate) async fn get_instance_write(
        &'_ self,
    ) -> RwLockMappedWriteGuard<'_, Option<Instance<C>>> {
        RwLockWriteGuard::map(self.inner.write().await, |i| &mut i.ins)
    }

    pub(crate) fn get_raw_read(
        option_ins: RwLockReadGuard<'_, Option<Instance<C>>>,
    ) -> RwLockReadGuard<'_, Instance<C>> {
        RwLockReadGuard::<Option<Instance<C>>>::map(option_ins, |f| f.as_ref().unwrap())
    }

    pub(crate) async fn get_notify_read(&'_ self) -> RwLockReadGuard<'_, Option<Vec<Arc<Notify>>>> {
        RwLockReadGuard::map(self.inner.read().await, |i| &i.notify)
    }

    pub(crate) async fn clear_notify(&self) {
        let mut inner = self.inner.write().await;
        inner.notify = None;
    }

    pub(crate) async fn add_notify(&self, notify: Arc<Notify>) {
        let mut inner = self.inner.write().await;
        if inner.notify.is_none() {
            inner.notify = Some(vec![notify]);
        } else if let Some(v) = inner.notify.as_mut() {
            v.push(notify);
        }
    }

    pub(crate) async fn notify_commit(&self) {
        // We have check the bound in the if branch
        let notify_vec = self.get_notify_read().await;
        if let Some(vec) = notify_vec.as_ref() {
            vec.iter().for_each(|notify| notify.notify_one());
        }
        drop(notify_vec);
        self.clear_notify().await;
    }
}

// FIXME: maybe hashmap is more fit in this case.
// A (replica_id, instance_id) pair to instance mapping.
// And a big rwlock is not efficient here.
#[derive(Clone)]
pub(crate) struct InstanceSpace<C>
where
    C: Command + Clone,
{
    inner: Arc<RwLock<Vec<Vec<SharedInstance<C>>>>>,
}

impl<C> InstanceSpace<C>
where
    C: Command + Clone,
{
    fn new(peer_cnt: usize) -> Self {
        let mut peer_vec = Vec::with_capacity(peer_cnt);
        (0..peer_cnt).for_each(|_| {
            peer_vec.push(vec![]);
        });

        Self {
            inner: Arc::new(RwLock::new(peer_vec)),
        }
    }

    /// Get the instance, if the it returns a Notify it means the instance
    /// is not ready, and the Notify is stored in the Notify Vec. The
    /// returned Notify is always fresh new, we don't reuse notify to
    /// avoid racing.
    pub(crate) async fn get_instance_or_notify(
        &self,
        replica: &ReplicaId,
        instance_id: &LocalInstanceId,
    ) -> (Option<SharedInstance<C>>, Option<Arc<Notify>>) {
        // TODO: this lock is too big
        let mut space = self.inner.write().await;
        let instance_id = **instance_id;
        let replica_id = **replica;

        let space_len = space[replica_id].len();
        if instance_id >= space_len {
            if Self::need_notify(&None).await {
                space[replica_id]
                    .extend((space_len..(instance_id + 1)).map(|_| SharedInstance::none()));
                let notify = Arc::new(Notify::new());
                space[replica_id][instance_id]
                    .add_notify(notify.clone())
                    .await;
                (None, Some(notify))
            } else {
                (None, None)
            }
        } else {
            // We have check the bound in the if branch
            let instance = space[replica_id][instance_id].clone();
            if Self::need_notify(&Some(instance.clone())).await {
                let notify = Arc::new(Notify::new());
                instance.add_notify(notify.clone()).await;
                (Some(instance), Some(notify))
            } else {
                (Some(instance), None)
            }
        }
    }

    async fn need_notify(ins: &Option<SharedInstance<C>>) -> bool {
        if !instance_exist(ins).await {
            true
        } else {
            let d_ins = ins.as_ref().unwrap();
            let d_ins_read = d_ins.get_instance_read().await;
            let d_ins_read_inner = d_ins_read.as_ref().unwrap();

            !matches!(
                d_ins_read_inner.status,
                InstanceStatus::Committed | InstanceStatus::Executed
            )
        }
    }

    /// Get the instance if it's created, otherwise None is returned
    /// TODO: unify `get_instance` and `get_instance_or_notify`
    pub(crate) async fn get_instance(
        &self,
        replica: &ReplicaId,
        instance_id: &LocalInstanceId,
    ) -> Option<SharedInstance<C>> {
        let space = self.inner.read().await;
        let instance_id = **instance_id;
        let replica_id = **replica;

        if instance_id >= space[replica_id].len() {
            None
        } else {
            // We have check the bound in the if branch
            Some(space[replica_id][instance_id].clone())
        }
    }

    /// Helper function to insert instance into instance space. Because there might be hole
    /// in the space, we fill the hole as None for the later get operation.
    pub(crate) async fn insert_instance(
        &self,
        replica: &ReplicaId,
        instance_id: &LocalInstanceId,
        instance: SharedInstance<C>,
    ) {
        let mut space = self.inner.write().await;
        let instance_id = **instance_id;
        let replica_id = **replica;
        let space_len = space[replica_id].len();
        match instance_id.partial_cmp(&space_len) {
            Some(std::cmp::Ordering::Greater) => {
                space[replica_id]
                    .extend((space_len..(instance_id + 1)).map(|_| SharedInstance::none()));
            }
            Some(std::cmp::Ordering::Less) => {
                space[replica_id][instance_id] = instance;
            }
            Some(std::cmp::Ordering::Equal) => {
                space[replica_id].push(instance);
            }
            None => {}
        }
    }
}

pub struct Replica<C, E>
where
    C: Command + Clone,
    E: CommandExecutor<C> + Clone,
{
    pub(crate) id: ReplicaId,
    pub(crate) peer_cnt: usize,
    pub(crate) instance_space: InstanceSpace<C>,
    /// Current max instance id for each replica, init values are 0s
    pub(crate) cur_instance: Vec<LocalInstanceId>,
    pub(crate) commited_upto: Vec<Option<LocalInstanceId>>,
    pub(crate) conflict: Conflict<C>,
    pub(crate) max_seq: Seq,
    pub(crate) exec_send: mpsc::Sender<SharedInstance<C>>,
    phantom: PhantomData<E>,
}

impl<C, E> Replica<C, E>
where
    C: Command + Sync + Send + Clone + 'static,
    E: CommandExecutor<C> + Clone + Sync + Send + 'static,
{
    pub(crate) async fn new(id: usize, peer_cnt: usize, cmd_exe: E) -> Self {
        let instance_space = InstanceSpace::new(peer_cnt);
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
        let mut new_seq = Seq(0);
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

/// The status of the instance, which is recorded in the instance space
#[derive(Serialize, Deserialize, PartialEq, Eq)]
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
#[derive(Debug, Serialize, Deserialize, Copy, Clone)]
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
    pub(crate) id: InstanceId,
    pub(crate) seq: Seq,
    pub(crate) ballot: Ballot,
    pub(crate) cmds: Vec<C>,
    pub(crate) deps: Vec<Option<LocalInstanceId>>,
    pub(crate) status: InstanceStatus,
    pub(crate) lb: LeaderBook,
}

impl<C> Instance<C>
where
    C: Command,
{
    fn local_id(&self) -> LocalInstanceId {
        self.id.inner
    }
}

pub(crate) struct LeaderBook {
    pub(crate) accept_ok: usize,
    pub(crate) preaccept_ok: usize,
    pub(crate) nack: usize,
    pub(crate) max_ballot: Ballot,
    pub(crate) all_equal: bool,
}

impl LeaderBook {
    pub(crate) fn new() -> Self {
        LeaderBook {
            accept_ok: 0,
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
