use crate::{
    config::Configure,
    error::RpcError,
    message::{Accept, Commit, Message, PreAccept, PreAcceptOk, PreAcceptReply, Propose},
    types::{
        Ballot, Command, Instance, InstanceId, InstanceStatus, LeaderBook, LeaderId, Replica,
        ReplicaId,
    },
    util,
};
use futures::stream::{self, StreamExt};
use log::trace;
use serde::{de::DeserializeOwned, Serialize};
use std::sync::Arc;
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{Mutex, RwLock},
    task::JoinHandle,
};

pub struct Server<C>
where
    C: Command,
{
    rpc_server: RpcServer<C>,
}

impl<C> Server<C>
where
    C: Command + std::fmt::Debug + Clone + Send + Sync + Serialize + DeserializeOwned + 'static,
{
    pub async fn new(conf: Configure) -> Self {
        let inner = Arc::new(InnerServer::new(conf));
        let rpc_server = RpcServer::new(inner.conf(), inner.clone()).await;
        Self { rpc_server }
    }

    pub async fn run(&self) {
        let _ = self.rpc_server.serve().await;
    }
}

pub struct InnerServer<C>
where
    C: Command,
{
    conf: Configure,
    // Connection List is modified once on initialization,
    // and connection linked to self should be empty
    conns: RwLock<Vec<Option<Arc<Mutex<TcpStream>>>>>,
    replica: Arc<Mutex<Replica<C>>>,
}

impl<C> InnerServer<C>
where
    C: Command + std::fmt::Debug + Clone + Serialize + Send + Sync + 'static,
{
    pub(crate) fn new(conf: Configure) -> Self {
        let peer_cnt = conf.peer_cnt;
        let id = conf.index;
        Self {
            conf,
            conns: RwLock::new(vec![]),
            replica: Arc::new(Mutex::new(Replica::new(id, peer_cnt as usize))),
        }
    }

    pub(crate) fn conf(&self) -> &Configure {
        &self.conf
    }

    pub(crate) async fn handle_message(&self, message: Message<C>)
    where
        C: Command + Serialize,
    {
        match message {
            Message::PreAccept(pa) => self.handle_preaccept(pa).await,
            Message::PreAcceptReply(par) => self.handle_preaccept_reply(par).await,
            Message::PreAcceptOk(pao) => self.handle_preaccept_ok(pao).await,
            Message::Accept(a) => self.handle_accept(a).await,
            Message::AcceptReply(_) => todo!(),
            Message::Commit(cm) => self.handle_commit(cm).await,
            Message::CommitShort(_) => todo!(),
            Message::Propose(p) => self.handle_propose(p).await,
        }
    }

    async fn handle_accept(&self, _a: Accept) {}

    async fn handle_commit(&self, cm: Commit<C>) {
        trace!("handle commit");
        let mut r = self.replica.lock().await;

        if cm.instance_id.inner >= r.cur_instance(&cm.instance_id.replica) {
            r.set_cur_instance(&InstanceId {
                replica: cm.instance_id.replica,
                inner: (*cm.instance_id.inner + 1).into(),
            });
        }

        let ins = r.instance_space[*cm.instance_id.replica].get(*cm.instance_id.inner);

        match ins {
            Some(ins) => {
                let mut ins = ins.write().await;
                ins.seq = cm.seq;
                ins.deps = cm.deps;
                ins.status = InstanceStatus::Committed;
            }
            None => {
                let new_instance = Arc::new(RwLock::new(Instance {
                    id: cm.instance_id.inner,
                    seq: cm.seq,
                    ballot: Ballot::new(),
                    cmds: cm.cmds.to_vec(),
                    deps: cm.deps,
                    status: InstanceStatus::Committed,
                    lb: LeaderBook::new(),
                }));
                r.instance_space[*cm.instance_id.replica]
                    .insert(*cm.instance_id.inner, new_instance.clone());
                r.update_conflict(&cm.cmds, new_instance).await;
            }
        }

        // TODO: sync to disk
    }

    async fn handle_preaccept_reply(&self, par: PreAcceptReply) {
        trace!("handle preaccept reply");
        let r = self.replica.lock().await;

        let ins = r.instance_space[*par.instance_id.replica].get(*par.instance_id.inner);

        if ins.is_none() {
            panic!("This instance should already in the space");
        }
        let mut ins = ins.unwrap().write().await;

        if !matches!(ins.status, InstanceStatus::PreAccepted) {
            // We've translated to the later states
            return;
        }

        if ins.ballot != par.ballot {
            // Other advanced (larger ballot) leader is handling
            return;
        }

        if !par.ok {
            ins.lb.nack += 1;
            if par.ballot > ins.lb.max_ballot {
                ins.lb.max_ballot = par.ballot;
            }
            return;
        }

        ins.lb.preaccept_ok += 1;

        let equal = r.merge_seq_deps(&mut ins, &par.seq, &par.deps);
        if ins.lb.preaccept_ok > 1 {
            ins.lb.all_equal = ins.lb.all_equal && equal;
        }

        if ins.lb.preaccept_ok >= r.peer_cnt / 2 && ins.lb.all_equal && ins.ballot.is_init() {
            ins.status = InstanceStatus::Committed;
            // TODO: sync to disk
            self.bdcst_message(
                r.id,
                Message::<C>::Commit(Commit {
                    leader_id: r.id.into(),
                    instance_id: par.instance_id,
                    seq: ins.seq,
                    cmds: ins.cmds.to_vec(),
                    deps: ins.deps.to_vec(),
                }),
            )
            .await;
        } else if ins.lb.preaccept_ok >= r.peer_cnt / 2 {
            ins.status = InstanceStatus::Accepted;
            self.bdcst_message(
                r.id,
                Message::<C>::Accept(Accept {
                    leader_id: r.id.into(),
                    instance_id: par.instance_id,
                    ballot: ins.ballot,
                    seq: ins.seq,
                    cmd_cnt: ins.cmds.len(),
                    deps: ins.deps.to_vec(),
                }),
            )
            .await;
        }
    }

    async fn handle_preaccept_ok(&self, pao: PreAcceptOk) {
        trace!("handle preaccept ok");
        let r = self.replica.lock().await;

        let ins = r.instance_space[*pao.instance_id.replica].get(*pao.instance_id.inner);

        if ins.is_none() {
            panic!("This instance should already in the space");
        }
        let mut ins = ins.unwrap().write().await;

        if !matches!(ins.status, InstanceStatus::PreAccepted) {
            // We've translated to the later states
            return;
        }

        if !ins.ballot.is_init() {
            // only the first leader can send ok
            return;
        }

        ins.lb.preaccept_ok += 1;

        // TODO: remove duplicate code
        if ins.lb.preaccept_ok >= r.peer_cnt / 2 && ins.lb.all_equal && ins.ballot.is_init() {
            ins.status = InstanceStatus::Committed;
            // TODO: sync to disk
            self.bdcst_message(
                r.id,
                Message::<C>::Commit(Commit {
                    leader_id: r.id.into(),
                    instance_id: pao.instance_id,
                    seq: ins.seq,
                    cmds: ins.cmds.to_vec(),
                    deps: ins.deps.to_vec(),
                }),
            )
            .await;
        } else if ins.lb.preaccept_ok >= r.peer_cnt / 2 {
            ins.status = InstanceStatus::Accepted;
            self.bdcst_message(
                r.id,
                Message::<C>::Accept(Accept {
                    leader_id: r.id.into(),
                    instance_id: pao.instance_id,
                    ballot: ins.ballot,
                    seq: ins.seq,
                    cmd_cnt: ins.cmds.len(),
                    deps: ins.deps.to_vec(),
                }),
            )
            .await;
        }
    }

    async fn handle_preaccept(&self, pa: PreAccept<C>) {
        trace!("handle preaccept {:?}", pa);
        let mut r = self.replica.lock().await;
        if let Some(inst) = r.instance_space[*pa.instance_id.replica].get(*pa.instance_id.inner) {
            let inst_read = inst.read().await;

            // We've got accpet or commit before, don't reply
            if matches!(
                inst_read.status,
                InstanceStatus::Committed | InstanceStatus::Accepted
            ) {
                // Later message may not contain commands, we should fill it here
                if inst_read.cmds.is_empty() {
                    drop(inst_read);
                    let mut inst_write = inst.write().await;
                    inst_write.cmds = pa.cmds;
                }
                return;
            }

            // smaller Ballot number
            if pa.ballot < inst_read.ballot {
                self.reply(
                    r.id,
                    &pa.leader_id,
                    Message::<C>::PreAcceptReply(PreAcceptReply {
                        instance_id: pa.instance_id,
                        seq: inst_read.seq,
                        ballot: inst_read.ballot,
                        ok: false,
                        deps: inst_read.deps.to_vec(),
                        committed_deps: r.commited_upto.to_vec(),
                    }),
                )
                .await;
                return;
            }
        }

        if pa.instance_id.inner > r.cur_instance(&pa.instance_id.replica) {
            r.set_cur_instance(&pa.instance_id);
        }

        // FIXME: We'd better not copy dep vec
        let (seq, deps, changed) = r.update_seq_deps(pa.seq, pa.deps.to_vec(), &pa.cmds).await;

        let status = if changed {
            InstanceStatus::PreAccepted
        } else {
            InstanceStatus::PreAcceptedEq
        };

        let uncommited_deps = r
            .commited_upto
            .iter()
            .enumerate()
            .map(|cu| {
                if let Some(cu_id) = cu.1 {
                    if let Some(d) = deps[cu.0] {
                        if cu_id < &d {
                            return true;
                        }
                    }
                }
                false
            })
            .filter(|a| *a)
            .count()
            > 0;

        let new_instance = Arc::new(RwLock::new(Instance {
            id: pa.instance_id.inner,
            seq,
            ballot: pa.ballot,
            // FIXME: should not copy
            cmds: pa.cmds.to_vec(),
            // FIXME: Should not copy if we send reply ok later
            deps: deps.to_vec(),
            status,
            lb: LeaderBook::new(),
        }));
        r.instance_space[*pa.instance_id.replica]
            .insert(*pa.instance_id.inner, new_instance.clone());

        r.update_conflict(&pa.cmds, new_instance).await;

        // TODO: sync to disk

        // Send Reply
        if changed
            || uncommited_deps
            || *pa.instance_id.replica != *pa.leader_id
            || !pa.ballot.is_init()
        {
            self.reply(
                r.id,
                &pa.leader_id,
                Message::<C>::PreAcceptReply(PreAcceptReply {
                    instance_id: pa.instance_id,
                    seq,
                    ballot: pa.ballot,
                    ok: true,
                    deps,
                    // Should not copy
                    committed_deps: r.commited_upto.to_vec(),
                }),
            )
            .await;
        } else {
            trace!("reply preaccept ok");
            self.reply(
                r.id,
                &pa.leader_id,
                Message::<C>::PreAcceptOk(PreAcceptOk {
                    instance_id: pa.instance_id,
                }),
            )
            .await;
        }
    }

    async fn handle_propose(&self, p: Propose<C>) {
        trace!("handle process");
        let mut r = self.replica.lock().await;
        let inst_no = *r.local_cur_instance();
        r.inc_local_cur_instance();
        let (seq, deps) = r.get_seq_deps(&p.cmds).await;

        let new_inst = Arc::new(RwLock::new(Instance {
            id: inst_no,
            seq,
            ballot: 0.into(),
            cmds: p.cmds,
            deps,
            status: InstanceStatus::PreAccepted,
            lb: LeaderBook::new(),
        }));
        r.update_conflict(&new_inst.read().await.cmds, new_inst.clone())
            .await;

        let r_id = *r.id;
        r.instance_space[r_id].insert(*inst_no, new_inst.clone());

        if seq > r.max_seq {
            r.max_seq = (*seq + 1).into();
        }

        // TODO: Flush the content to disk
        self.bdcst_message(
            r.id,
            Message::PreAccept(PreAccept {
                leader_id: r.id.into(),
                instance_id: InstanceId {
                    replica: r.id,
                    inner: inst_no,
                },
                seq,
                ballot: 0.into(),
                // FIXME: should not copy/clone vec
                cmds: new_inst.read().await.cmds.to_vec(),
                deps: new_inst.read().await.deps.to_vec(),
            }),
        )
        .await;
    }

    async fn reply(&self, replica: ReplicaId, leader: &LeaderId, message: Message<C>) {
        let mut all_connect = self.conns.read().await;
        if all_connect.is_empty() {
            drop(all_connect);
            self.init_connections(&replica).await;
            all_connect = self.conns.read().await;
        }
        // Illeage leader id
        if **leader >= all_connect.len() {
            panic!(
                "all connection number is {}, but the leader id is {}",
                all_connect.len(),
                **leader
            );
        }

        // Should not panic, leader is should be in the range
        let conn = all_connect.get(**leader).unwrap();

        if conn.is_none() {
            // Should not reply to self
            panic!("should not reply to self");
        }

        util::send_message_arc(conn.as_ref().unwrap(), &message).await;
    }

    async fn bdcst_message(&self, replica: ReplicaId, message: Message<C>) {
        let mut conn = self.conns.read().await;
        if conn.is_empty() {
            drop(conn);
            self.init_connections(&replica).await;
            conn = self.conns.read().await;
        }
        let cnt = self.conf.peer.len();

        let message = Arc::new(message);

        let tasks: Vec<JoinHandle<()>> = conn
            .iter()
            .filter(|c| c.is_some())
            .map(|c| {
                let c = c.as_ref().unwrap().clone();
                let message = message.clone();
                tokio::spawn(async move {
                    util::send_message_arc2(&c, &message).await;
                })
            })
            .collect();

        let stream_of_futures = stream::iter(tasks);
        let mut buffered = stream_of_futures.buffer_unordered(self.conf.peer.len());

        for _ in 0..cnt {
            buffered.next().await;
        }
    }

    async fn init_connections(&self, cur_replica: &ReplicaId) {
        let mut conn_write = self.conns.write().await;
        if conn_write.is_empty() {
            for (id, p) in self.conf.peer.iter().enumerate() {
                let stream = TcpStream::connect(p)
                    .await
                    .map_err(|e| panic!("connect to {} failed, {}", p, e))
                    .unwrap();
                conn_write.push(if id == **cur_replica {
                    None
                } else {
                    Some(Arc::new(Mutex::new(stream)))
                });
            }
        }
    }
}

pub(crate) struct RpcServer<C>
where
    C: Command,
{
    server: Arc<InnerServer<C>>,
    listener: TcpListener,
}

impl<C> RpcServer<C>
where
    C: Command + std::fmt::Debug + Clone + Serialize + DeserializeOwned + Send + Sync + 'static,
{
    pub(crate) async fn new(config: &Configure, server: Arc<InnerServer<C>>) -> Self {
        // Configure correctness has been checked
        let listener = TcpListener::bind(config.peer.get(config.index as usize).unwrap())
            .await
            .map_err(|e| panic!("bind server address error, {}", e))
            .unwrap();

        Self { server, listener }
    }

    pub(crate) async fn serve(&self) -> Result<(), RpcError> {
        loop {
            let (mut stream, _) = self.listener.accept().await?;
            let server = self.server.clone();
            tokio::spawn(async move {
                trace!("got a connection");
                loop {
                    let message: Message<C> = util::recv_message(&mut stream).await;
                    server.handle_message(message).await;
                }
            });
        }
    }
}
