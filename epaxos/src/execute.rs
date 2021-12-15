use std::{
    cmp::Ordering::{Equal, Greater, Less},
    collections::{HashMap, VecDeque},
};

use petgraph::{
    algo::tarjan_scc,
    graph::{DiGraph, NodeIndex},
};
use tokio::sync::mpsc;

use crate::types::{Command, CommandExecutor, InstanceSpace, InstanceStatus, SharedInstance};

pub(crate) struct Executor<C, E>
where
    C: Command + Clone + Send + Sync + 'static,
    E: CommandExecutor<C>,
{
    space: InstanceSpace<C>,
    cmd_exe: E,
}

impl<C, E> Executor<C, E>
where
    C: Command + Clone + Send + Sync + 'static,
    E: CommandExecutor<C> + Clone + Send + Sync + 'static,
{
    pub(crate) fn new(space: InstanceSpace<C>, cmd_exe: E) -> Self {
        Self { space, cmd_exe }
    }
    // TODO:
    // Wait missing instance and invalid state instance
    pub(crate) async fn execute(&self, mut recv: mpsc::Receiver<SharedInstance<C>>) {
        // A inifite loop to pull instance to execute
        loop {
            let ins = recv.recv().await;

            if ins.is_none() {
                // The channel has been closed, stop recving.
                break;
            }

            let mut inner = InnerExecutor::new(&self.space, &self.cmd_exe, ins.unwrap());
            tokio::spawn(async move {
                let scc = inner.build_scc().await;
                if let Some(scc) = scc {
                    inner.execute(scc).await;
                }
            });
        }
    }
}

struct InnerExecutor<C, E>
where
    C: Command + Clone + Send + Sync + 'static,
    E: CommandExecutor<C>,
{
    space: InstanceSpace<C>,
    cmd_exe: E,
    start_ins: SharedInstance<C>,
    map: Option<HashMap<SharedInstance<C>, NodeIndex>>,
    graph: Option<DiGraph<SharedInstance<C>, ()>>,
}

impl<C, E> InnerExecutor<C, E>
where
    C: Command + Clone + Send + Sync + 'static,
    E: CommandExecutor<C> + Clone + Send + Sync,
{
    fn new(space: &InstanceSpace<C>, cmd_exe: &E, start_ins: SharedInstance<C>) -> Self {
        Self {
            space: space.clone(),
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

                // every instance in the queue should not be in the map
                if !self.has_visited(&d_ins)
                    && d_ins.match_status(&[InstanceStatus::Committed]).await
                {
                    queue.push_back(d_ins.clone());
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
                match a.1 .1.partial_cmp(&b.1 .1) {
                    Some(Greater) => return Greater,
                    Some(Less) => return Less,
                    _ => {}
                };

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
}
