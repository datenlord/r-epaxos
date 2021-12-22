use std::sync::Arc;
use std::{fmt::Debug, hash::Hash};

use serde::{Deserialize, Serialize};
use tokio::sync::{Notify, RwLock, RwLockMappedWriteGuard, RwLockReadGuard, RwLockWriteGuard};

use super::{
    cmd::Command,
    id::{Ballot, InstanceId, LocalInstanceId, Seq},
    lb::LeaderBook,
};

#[derive(Debug)]
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
    pub(crate) fn local_id(&self) -> LocalInstanceId {
        self.id.local
    }
}

#[derive(Debug)]
pub(crate) struct SharedInstanceInner<C>
where
    C: Command + Clone,
{
    ins: Option<Instance<C>>,
    notify: Option<Vec<Arc<Notify>>>,
}

/// Th shared instance stored in the instance space
#[derive(Clone, Debug)]
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
        Arc::ptr_eq(&self.inner, &other.inner)
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

/// The status of the instance, which is recorded in the instance space
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone, Copy)]
pub(crate) enum InstanceStatus {
    PreAccepted,
    PreAcceptedEq,
    Accepted,
    Committed,
    Executed,
}
