use std::{
    cmp::Ordering::{Equal, Greater, Less},
    collections::{HashMap, VecDeque},
    fmt::Debug,
    marker::PhantomData,
    sync::Arc,
};

use petgraph::{
    algo::tarjan_scc,
    graph::{DiGraph, NodeIndex},
};
use tokio::sync::mpsc;

use crate::types::{Command, CommandExecutor, InstanceSpace, InstanceStatus, SharedInstance};

pub(crate) struct Executor<C, E, S>
where
    C: Command + Clone + Send + Sync + 'static,
    E: CommandExecutor<C>,
    S: InstanceSpace<C>,
{
    space: Arc<S>,
    cmd_exe: E,
    _phanto: PhantomData<C>,
}

impl<C, E, S> Executor<C, E, S>
where
    C: Command + Debug + Clone + Send + Sync + 'static,
    E: CommandExecutor<C> + Debug + Clone + Send + Sync + 'static,
    S: InstanceSpace<C> + Send + Sync + 'static,
{
    pub(crate) fn new(space: Arc<S>, cmd_exe: E) -> Self {
        Self {
            space,
            cmd_exe,
            _phanto: PhantomData,
        }
    }

    pub(crate) async fn execute(&self, mut recv: mpsc::Receiver<SharedInstance<C>>) {
        // A inifite loop to pull instance to execute
        loop {
            let ins = recv.recv().await;

            if ins.is_none() {
                // The channel has been closed, stop recving.
                break;
            }

            let mut inner =
                InnerExecutor::new(Arc::<S>::clone(&self.space), &self.cmd_exe, ins.unwrap());
            tokio::spawn(async move {
                let scc = inner.build_scc().await;
                if let Some(scc) = scc {
                    inner.execute(scc).await;
                }
            });
        }
    }
}

struct InnerExecutor<C, E, S>
where
    C: Command + Clone + Send + Sync + 'static,
    E: CommandExecutor<C>,
    S: InstanceSpace<C>,
{
    space: Arc<S>,
    cmd_exe: E,
    start_ins: SharedInstance<C>,
    map: Option<HashMap<SharedInstance<C>, NodeIndex>>,
    graph: Option<DiGraph<SharedInstance<C>, ()>>,
}

impl<C, E, S> InnerExecutor<C, E, S>
where
    C: Command + Debug + Clone + Send + Sync + 'static,
    E: CommandExecutor<C> + Clone + Send + Sync,
    S: InstanceSpace<C> + Send + Sync + 'static,
{
    fn new(space: Arc<S>, cmd_exe: &E, start_ins: SharedInstance<C>) -> Self {
        Self {
            space,
            cmd_exe: cmd_exe.clone(),
            start_ins,
            map: None,
            graph: None,
        }
    }

    /// Build the scc and generate the result vec from an instance. We'll stop inserting instance to
    /// the graph in the following condition:
    /// - the instance's status is EXECUTED, which means every following step is EXECUTED.
    ///
    /// We'll also wait for one instance in the following conditions:
    /// - the instance's status is NOT COMMITTED and NOT EXECUTED.
    /// - the instance is empty.
    ///
    /// The return value is None if there's no instance to execute.
    /// The return value is Some(Vec<...>), which is the scc vec, if there are instances to execute.
    async fn build_scc(&mut self) -> Option<Vec<Vec<NodeIndex>>>
    where
        C: Command + Clone + Send + Sync + 'static,
    {
        // the start_ins is at least in the stage of COMMITTED
        if self
            .start_ins
            .match_status(&[InstanceStatus::Executed])
            .await
        {
            return None;
        }

        let mut queue = VecDeque::new();
        queue.push_back(self.start_ins.clone());

        // initialization for map and graph fields
        self.map = Some(HashMap::<SharedInstance<C>, NodeIndex>::new());
        self.graph = Some(DiGraph::<SharedInstance<C>, ()>::new());

        loop {
            let cur = queue.pop_front();

            // queue is empty
            if cur.is_none() {
                break;
            }
            let cur = cur.unwrap();

            // get node index
            let cur_index = self.get_or_insert_index(&cur);
            let cur_read = cur.get_instance_read().await;
            let cur_read_inner = cur_read.as_ref().unwrap();

            for (r, d) in cur_read_inner.deps.iter().enumerate() {
                if d.is_none() {
                    continue;
                }
                let r = r.into();
                let d = d.as_ref().unwrap();

                let (d_ins, notify) = self.space.get_instance_or_notify(&r, d).await;

                let d_ins = if let Some(n) = notify {
                    n.notified().await;
                    self.space.get_instance(&r, d).await
                } else {
                    d_ins
                };

                assert!(
                    d_ins.is_some(),
                    "instance should not be none after notification"
                );
                let d_ins = d_ins.unwrap();

                if d_ins.match_status(&[InstanceStatus::Committed]).await {
                    // there might be cycle
                    if !self.has_visited(&d_ins) {
                        queue.push_back(d_ins.clone());
                    }

                    let d_index = self.get_or_insert_index(&d_ins);
                    self.add_edge(cur_index, d_index);
                }
            }
        }

        Some(self.generate_scc())
    }

    /// Get the graph index for the instance, if the index is missing we
    /// insert the instance into graph and return the index, otherwise
    /// return the index in the map directly.
    fn get_or_insert_index(&mut self, ins: &SharedInstance<C>) -> NodeIndex
    where
        C: Command + Clone + Send + Sync + 'static,
    {
        let map = self.map.as_mut().unwrap();
        let g = self.graph.as_mut().unwrap();
        if !HashMap::contains_key(map, ins) {
            let index = g.add_node(ins.clone());
            map.insert(ins.clone(), index);
            index
        } else {
            *map.get(ins).unwrap()
        }
    }

    /// Tell whether we have visited the instance while building the dep graph
    fn has_visited(&self, ins: &SharedInstance<C>) -> bool {
        let map = self.map.as_ref().unwrap();
        map.contains_key(ins)
    }

    fn add_edge(&mut self, src: NodeIndex, dst: NodeIndex) {
        let g = self.graph.as_mut().unwrap();
        g.add_edge(src, dst, ());
    }

    fn generate_scc(&self) -> Vec<Vec<NodeIndex>> {
        let g = self.graph.as_ref().unwrap();
        tarjan_scc(g)
    }

    async fn execute(&self, scc: Vec<Vec<NodeIndex>>) {
        let g = self.graph.as_ref().unwrap();
        for each_scc in scc {
            let ins_vec = each_scc.iter().map(|index| &g[*index]);

            let mut sort_vec = Vec::with_capacity(each_scc.len());
            for (id, ins) in ins_vec.enumerate() {
                let ins_read = ins.get_instance_read().await;
                let ins_read = ins_read.as_ref().unwrap();
                sort_vec.push((id, (ins_read.id.replica, ins_read.seq)));
            }

            sort_vec.sort_by(|a, b| {
                // Compare seq
                match a.1 .1.partial_cmp(&b.1 .1) {
                    Some(Greater) => return Greater,
                    Some(Less) => return Less,
                    _ => {}
                };

                // Compare replica id
                match a.1 .0.partial_cmp(&b.1 .0) {
                    Some(Greater) => Greater,
                    Some(Less) => Less,
                    _ => Equal,
                }
            });

            for (id, _) in sort_vec {
                let ins = &g[each_scc[id]];
                let mut ins_write = ins.get_instance_write().await;
                let ins_write = ins_write.as_mut().unwrap();

                // It may be executed by other execution tasks
                if matches!(ins_write.status, InstanceStatus::Committed) {
                    for c in &ins_write.cmds {
                        // FIXME: handle execute error
                        let _ = c.execute(&self.cmd_exe).await;
                    }
                    ins_write.status = InstanceStatus::Executed;
                }
            }
        }
    }

    /// This function is used in the UTs to get the `SharedInstance` from the NodeIndex
    #[cfg(test)]
    fn get_node_from_graph_index(&self, index: NodeIndex) -> SharedInstance<C> {
        self.graph.as_ref().unwrap()[index].clone()
    }
}

#[cfg(test)]
mod test {
    use std::{collections::BTreeMap, sync::Arc};

    use mockall::predicate;
    use tokio::sync::mpsc;

    use crate::types::{
        Ballot, Instance, InstanceId, InstanceSpace, InstanceStatus, LeaderBook, MockCommand,
        MockCommandExecutor, MockCommandId, MockInstanceSpace, Seq, SharedInstance,
    };

    use super::InnerExecutor;

    fn build_space(
        space: &mut MockInstanceSpace<MockCommand>,
        nodes: BTreeMap<InstanceId, (InstanceStatus, Seq, Vec<MockCommand>)>,
        deps: BTreeMap<InstanceId, Vec<InstanceId>>,
    ) -> Vec<SharedInstance<MockCommand>> {
        let mut shared_instance = vec![];
        for (id, (status, seq, cmd)) in nodes {
            let deps = deps
                .get(&id)
                .unwrap_or_else(|| panic!("id {:?} has no deps setting", id));
            let largest_replica = deps.iter().map(|ins| ins.replica).max();

            let deps = match largest_replica {
                Some(top) => {
                    let mut v = Vec::new();
                    v.extend((0..*top + 1).map(|_| None));
                    deps.iter()
                        .for_each(|inst_id| v[*inst_id.replica] = Some(inst_id.local));
                    v
                }
                None => vec![],
            };

            let ins = SharedInstance::new(
                Some(Instance {
                    id: InstanceId::new(id.replica, id.local),
                    seq,
                    ballot: Ballot::new_with_epoch(0.into(), 0),
                    cmds: cmd,
                    deps: deps.to_owned(),
                    status,
                    lb: LeaderBook::default(),
                }),
                None,
            );

            let ins_clone = ins.clone();
            let ins_return = ins.clone();

            space
                .expect_get_instance_or_notify()
                .with(predicate::eq(id.replica), predicate::eq(id.local))
                .returning(move |_rep, _id| (Some(ins.clone()), None));

            space
                .expect_get_instance()
                .with(predicate::eq(id.replica), predicate::eq(id.local))
                .returning(move |_rep, _id| Some(ins_clone.clone()));

            shared_instance.push(ins_return);
        }

        shared_instance
    }

    #[tokio::test]
    async fn build_scc_from_executed() {
        let mut space = MockInstanceSpace::<MockCommand>::default();
        let cmd_exe = MockCommandExecutor::new(None);

        let mut deps = BTreeMap::new();
        let mut status = BTreeMap::new();
        deps.insert(InstanceId::new(0.into(), 0.into()), vec![]);
        status.insert(
            InstanceId::new(0.into(), 0.into()),
            (InstanceStatus::Executed, 0.into(), vec![]),
        );
        build_space(&mut space, status, deps);

        let shared_ins = space.get_instance(&0.into(), &0.into()).await.unwrap();
        let mut inner_exe = InnerExecutor::new(Arc::new(space), &cmd_exe, shared_ins);
        assert_eq!(inner_exe.build_scc().await, None);
    }

    #[tokio::test]
    async fn build_scc_cycle() {
        let mut space = MockInstanceSpace::<MockCommand>::default();
        let cmd_exe = MockCommandExecutor::new(None);

        let mut deps = BTreeMap::new();
        let mut nodes = BTreeMap::new();
        deps.insert(
            InstanceId::new(0.into(), 0.into()),
            vec![InstanceId::new(1.into(), 0.into())],
        );
        deps.insert(
            InstanceId::new(1.into(), 0.into()),
            vec![InstanceId::new(2.into(), 0.into())],
        );
        deps.insert(
            InstanceId::new(2.into(), 0.into()),
            vec![InstanceId::new(0.into(), 0.into())],
        );
        nodes.insert(
            InstanceId::new(0.into(), 0.into()),
            (InstanceStatus::Committed, 0.into(), vec![]),
        );
        nodes.insert(
            InstanceId::new(1.into(), 0.into()),
            (InstanceStatus::Committed, 0.into(), vec![]),
        );
        nodes.insert(
            InstanceId::new(2.into(), 0.into()),
            (InstanceStatus::Committed, 0.into(), vec![]),
        );
        build_space(&mut space, nodes, deps);

        let shared_ins = space.get_instance(&0.into(), &0.into()).await.unwrap();
        let mut inner_exe = InnerExecutor::new(Arc::new(space), &cmd_exe, shared_ins);
        let scc = inner_exe.build_scc().await;

        assert!(scc.is_some());
        let scc = scc.unwrap();
        assert_eq!(scc.len(), 1);
        let cycle = &scc[0];
        assert_eq!(cycle.len(), 3);
    }

    #[tokio::test]
    async fn build_scc_line() {
        let mut space = MockInstanceSpace::<MockCommand>::default();
        let cmd_exe = MockCommandExecutor::new(None);

        let mut deps = BTreeMap::new();
        let mut nodes = BTreeMap::new();
        deps.insert(
            InstanceId::new(0.into(), 0.into()),
            vec![InstanceId::new(1.into(), 0.into())],
        );
        deps.insert(
            InstanceId::new(1.into(), 0.into()),
            vec![InstanceId::new(2.into(), 0.into())],
        );
        deps.insert(InstanceId::new(2.into(), 0.into()), vec![]);
        nodes.insert(
            InstanceId::new(0.into(), 0.into()),
            (InstanceStatus::Committed, 0.into(), vec![]),
        );
        nodes.insert(
            InstanceId::new(1.into(), 0.into()),
            (InstanceStatus::Committed, 0.into(), vec![]),
        );
        nodes.insert(
            InstanceId::new(2.into(), 0.into()),
            (InstanceStatus::Committed, 0.into(), vec![]),
        );
        let origin_nodes = build_space(&mut space, nodes, deps);

        let shared_ins = space.get_instance(&0.into(), &0.into()).await.unwrap();
        let mut inner_exe = InnerExecutor::new(Arc::new(space), &cmd_exe, shared_ins);
        let scc = inner_exe.build_scc().await;

        assert!(scc.is_some());
        let scc = scc.unwrap();
        assert_eq!(scc.len(), 3);
        assert_eq!(scc[0].len(), 1);
        assert_eq!(scc[1].len(), 1);
        assert_eq!(scc[2].len(), 1);

        assert_eq!(
            inner_exe.get_node_from_graph_index(scc[0][0]),
            origin_nodes[2]
        );
        assert_eq!(
            inner_exe.get_node_from_graph_index(scc[1][0]),
            origin_nodes[1]
        );
        assert_eq!(
            inner_exe.get_node_from_graph_index(scc[2][0]),
            origin_nodes[0]
        );
    }

    #[tokio::test]
    async fn build_scc_line_executed() {
        let mut space = MockInstanceSpace::<MockCommand>::default();
        let cmd_exe = MockCommandExecutor::new(None);

        let mut deps = BTreeMap::new();
        let mut nodes = BTreeMap::new();
        deps.insert(
            InstanceId::new(0.into(), 0.into()),
            vec![InstanceId::new(1.into(), 0.into())],
        );
        deps.insert(
            InstanceId::new(1.into(), 0.into()),
            vec![InstanceId::new(2.into(), 0.into())],
        );
        deps.insert(InstanceId::new(2.into(), 0.into()), vec![]);
        nodes.insert(
            InstanceId::new(0.into(), 0.into()),
            (InstanceStatus::Committed, 0.into(), vec![]),
        );
        nodes.insert(
            InstanceId::new(1.into(), 0.into()),
            (InstanceStatus::Committed, 0.into(), vec![]),
        );
        nodes.insert(
            InstanceId::new(2.into(), 0.into()),
            (InstanceStatus::Executed, 0.into(), vec![]),
        );
        let origin_nodes = build_space(&mut space, nodes, deps);

        let shared_ins = space.get_instance(&0.into(), &0.into()).await.unwrap();
        let mut inner_exe = InnerExecutor::new(Arc::new(space), &cmd_exe, shared_ins);
        let scc = inner_exe.build_scc().await;

        assert!(scc.is_some());
        let scc = scc.unwrap();
        assert_eq!(scc.len(), 2);
        assert_eq!(scc[0].len(), 1);
        assert_eq!(scc[1].len(), 1);

        assert_eq!(
            inner_exe.get_node_from_graph_index(scc[0][0]),
            origin_nodes[1]
        );
        assert_eq!(
            inner_exe.get_node_from_graph_index(scc[1][0]),
            origin_nodes[0]
        );
    }

    #[tokio::test]
    async fn executed_line() {
        let mut space = MockInstanceSpace::<MockCommand>::default();
        let (sender, mut receiver) = mpsc::channel(10);
        let cmd_exe = MockCommandExecutor::new(Some(sender));

        let mut deps = BTreeMap::new();
        let mut nodes = BTreeMap::new();
        deps.insert(
            InstanceId::new(0.into(), 0.into()),
            vec![InstanceId::new(1.into(), 0.into())],
        );
        deps.insert(
            InstanceId::new(1.into(), 0.into()),
            vec![InstanceId::new(2.into(), 0.into())],
        );
        deps.insert(InstanceId::new(2.into(), 0.into()), vec![]);

        let mut mock_ids = vec![];
        let ins_id = InstanceId::new(0.into(), 0.into());
        let cmd_id = MockCommandId::new(ins_id, 0);
        mock_ids.push(cmd_id);
        nodes.insert(
            ins_id,
            (
                InstanceStatus::Committed,
                0.into(),
                vec![MockCommand::new("", cmd_id)],
            ),
        );

        let ins_id = InstanceId::new(1.into(), 0.into());
        let cmd_id = MockCommandId::new(ins_id, 0);
        mock_ids.push(cmd_id);
        nodes.insert(
            ins_id,
            (
                InstanceStatus::Committed,
                0.into(),
                vec![MockCommand::new("", cmd_id)],
            ),
        );

        let ins_id = InstanceId::new(2.into(), 0.into());
        let cmd_id = MockCommandId::new(ins_id, 0);
        mock_ids.push(cmd_id);
        nodes.insert(
            ins_id,
            (
                InstanceStatus::Committed,
                0.into(),
                vec![MockCommand::new("", cmd_id)],
            ),
        );
        let _ = build_space(&mut space, nodes, deps);

        let shared_ins = space.get_instance(&0.into(), &0.into()).await.unwrap();
        let mut inner_exe = InnerExecutor::new(Arc::new(space), &cmd_exe, shared_ins);
        let scc = inner_exe.build_scc().await;

        inner_exe.execute(scc.unwrap()).await;

        assert_eq!(receiver.recv().await.unwrap(), mock_ids[2]);
        assert_eq!(receiver.recv().await.unwrap(), mock_ids[1]);
        assert_eq!(receiver.recv().await.unwrap(), mock_ids[0]);
    }

    #[tokio::test]
    async fn executed_cycle() {
        let mut space = MockInstanceSpace::<MockCommand>::default();
        let (sender, mut receiver) = mpsc::channel(10);
        let cmd_exe = MockCommandExecutor::new(Some(sender));

        let mut deps = BTreeMap::new();
        let mut nodes = BTreeMap::new();
        deps.insert(
            InstanceId::new(0.into(), 0.into()),
            vec![InstanceId::new(1.into(), 0.into())],
        );
        deps.insert(
            InstanceId::new(1.into(), 0.into()),
            vec![InstanceId::new(2.into(), 0.into())],
        );
        deps.insert(
            InstanceId::new(2.into(), 0.into()),
            vec![InstanceId::new(0.into(), 0.into())],
        );

        let mut mock_ids = vec![];
        let ins_id = InstanceId::new(0.into(), 0.into());
        let cmd_id = MockCommandId::new(ins_id, 0);
        mock_ids.push(cmd_id);
        nodes.insert(
            ins_id,
            (
                InstanceStatus::Committed,
                0.into(),
                vec![MockCommand::new("", cmd_id)],
            ),
        );

        let ins_id = InstanceId::new(1.into(), 0.into());
        let cmd_id = MockCommandId::new(ins_id, 0);
        mock_ids.push(cmd_id);
        nodes.insert(
            ins_id,
            (
                InstanceStatus::Committed,
                0.into(),
                vec![MockCommand::new("", cmd_id)],
            ),
        );

        let ins_id = InstanceId::new(2.into(), 0.into());
        let cmd_id = MockCommandId::new(ins_id, 0);
        mock_ids.push(cmd_id);
        nodes.insert(
            ins_id,
            (
                InstanceStatus::Committed,
                0.into(),
                vec![MockCommand::new("", cmd_id)],
            ),
        );
        let _ = build_space(&mut space, nodes, deps);

        let shared_ins = space.get_instance(&0.into(), &0.into()).await.unwrap();
        let mut inner_exe = InnerExecutor::new(Arc::new(space), &cmd_exe, shared_ins);
        let scc = inner_exe.build_scc().await;

        inner_exe.execute(scc.unwrap()).await;

        assert_eq!(receiver.recv().await.unwrap(), mock_ids[0]);
        assert_eq!(receiver.recv().await.unwrap(), mock_ids[1]);
        assert_eq!(receiver.recv().await.unwrap(), mock_ids[2]);
    }
}
