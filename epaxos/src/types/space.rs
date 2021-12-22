use std::sync::Arc;

use async_trait::async_trait;
#[cfg(test)]
use mockall::automock;
use tokio::sync::{Notify, RwLock};

use crate::util::instance_exist;

use super::{
    cmd::Command,
    id::{LocalInstanceId, ReplicaId},
    instance::{InstanceStatus, SharedInstance},
};

#[cfg_attr(test, automock)]
#[async_trait]
pub(crate) trait InstanceSpace<C>
where
    C: Command + Clone + Send + Sync + 'static,
{
    /// Constructor to create a instance space
    fn new(peer_cnt: usize) -> Self;

    /// Get the instance, if the it returns a Notify it means the instance
    /// is not ready, and the Notify is stored in the Notify Vec. The
    /// returned Notify is always fresh new, so we don't reuse it to
    /// avoid racing.
    async fn get_instance_or_notify(
        &self,
        replica: &ReplicaId,
        instance_id: &LocalInstanceId,
    ) -> (Option<SharedInstance<C>>, Option<Arc<Notify>>);

    /// Get the instance if it's created, otherwise None is returned
    /// TODO: unify `get_instance` and `get_instance_or_notify`
    async fn get_instance(
        &self,
        replica: &ReplicaId,
        instance_id: &LocalInstanceId,
    ) -> Option<SharedInstance<C>>;

    /// Helper function to insert instance into instance space. Because there might be hole
    /// in the space, we fill the hole as None for the later get operation.
    async fn insert_instance(
        &self,
        replica: &ReplicaId,
        instance_id: &LocalInstanceId,
        instance: SharedInstance<C>,
    );
}

// FIXME: maybe hashmap is more fit in this case.
// A (replica_id, instance_id) pair to instance mapping.
// And a big rwlock is not efficient here.
pub struct VecInstanceSpace<C>
where
    C: Command + Clone + Send + Sync + 'static,
{
    inner: RwLock<Vec<Vec<SharedInstance<C>>>>,
}

#[async_trait]
impl<C> InstanceSpace<C> for VecInstanceSpace<C>
where
    C: Command + Clone + Send + Sync + 'static,
{
    fn new(peer_cnt: usize) -> Self {
        let mut peer_vec = Vec::with_capacity(peer_cnt);
        (0..peer_cnt).for_each(|_| {
            peer_vec.push(vec![]);
        });

        Self {
            inner: RwLock::new(peer_vec),
        }
    }

    /// Get the instance, if the it returns a Notify it means the instance
    /// is not ready, and the Notify is stored in the Notify Vec. The
    /// returned Notify is always fresh new, we don't reuse notify to
    /// avoid racing.
    async fn get_instance_or_notify(
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

    /// Get the instance if it's created, otherwise None is returned
    /// TODO: unify `get_instance` and `get_instance_or_notify`
    async fn get_instance(
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
    async fn insert_instance(
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

impl<C> VecInstanceSpace<C>
where
    C: Command + Clone + Send + Sync + 'static,
{
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
}
