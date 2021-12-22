use crate::{
    config::Configure,
    error::RpcError,
    message::{
        Accept, AcceptReply, Commit, Message, PreAccept, PreAcceptOk, PreAcceptReply, Propose,
    },
    types::{
        Ballot, Command, CommandExecutor, Instance, InstanceId, InstanceSpace, InstanceStatus,
        LeaderBook, LeaderId, Replica, ReplicaId, SharedInstance, VecInstanceSpace,
    },
    util::{self, instance_exist},
};
use futures::stream::{self, StreamExt};
use log::trace;
use serde::{de::DeserializeOwned, Serialize};
use std::fmt::Debug;
use std::sync::Arc;
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{Mutex, RwLock},
    task::JoinHandle,
};

/// Hide
pub struct DefaultServer<C, E>
where
    C: Command + Clone + Send + Sync + 'static,
    E: CommandExecutor<C> + Clone,
{
    inner: Server<C, E, VecInstanceSpace<C>>,
}

impl<C, E> DefaultServer<C, E>
where
    C: Command + Debug + Clone + Send + Sync + Serialize + DeserializeOwned + 'static,
    E: CommandExecutor<C> + Debug + Clone + Send + Sync + 'static,
{
    pub async fn new(conf: Configure, cmd_exe: E) -> Self {
        Self {
            inner: Server::new(conf, cmd_exe).await,
        }
    }

    pub async fn run(&self) {
        self.inner.run().await;
    }
}

pub(crate) struct Server<C, E, S>
where
    C: Command + Clone + Send + Sync + 'static,
    E: CommandExecutor<C> + Clone,
    S: InstanceSpace<C>,
{
    rpc_server: RpcServer<C, E, S>,
}

impl<C, E, S> Server<C, E, S>
where
    C: Command + Debug + Clone + Send + Sync + Serialize + DeserializeOwned + 'static,
    E: CommandExecutor<C> + Debug + Clone + Send + Sync + 'static,
    S: InstanceSpace<C> + Send + Sync + 'static,
{
    pub async fn new(conf: Configure, cmd_exe: E) -> Self {
        let inner = Arc::new(InnerServer::new(conf, cmd_exe).await);
        let rpc_server = RpcServer::new(inner.conf(), inner.clone()).await;
        Self { rpc_server }
    }

    pub async fn run(&self) {
        let _ = self.rpc_server.serve().await;
    }
}

pub(crate) struct InnerServer<C, E, S>
where
    C: Command + Clone + Send + Sync + 'static,
    E: CommandExecutor<C> + Clone,
    S: InstanceSpace<C>,
{
    conf: Configure,
    // Connection List is modified once on initialization,
    // and connection linked to self should be empty
    conns: RwLock<Vec<Option<Arc<Mutex<TcpStream>>>>>,
    replica: Arc<Mutex<Replica<C, E, S>>>,
}

impl<C, E, S> InnerServer<C, E, S>
where
    C: Command + Debug + Clone + Serialize + Send + Sync + 'static,
    E: CommandExecutor<C> + Debug + Clone + Send + Sync + 'static,
    S: InstanceSpace<C> + Send + Sync + 'static,
{
    pub(crate) async fn new(conf: Configure, cmd_exe: E) -> Self {
        let peer_cnt = conf.peer_cnt;
        let id = conf.index;
        Self {
            conf,
            conns: RwLock::new(vec![]),
            replica: Arc::new(Mutex::new(
                Replica::new(id, peer_cnt as usize, cmd_exe).await,
            )),
        }
    }

    pub(crate) fn conf(&self) -> &Configure {
        &self.conf
    }

    pub(crate) async fn new_leaderbook(&self) -> LeaderBook {
        LeaderBook::new(self.replica.lock().await.id, &self.conf)
    }

    pub(crate) async fn new_ballot(&self) -> Ballot {
        Ballot::new(self.replica.lock().await.id, &self.conf)
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
            Message::AcceptReply(ar) => self.handle_accept_reply(ar).await,
            Message::Commit(cm) => self.handle_commit(cm).await,
            Message::CommitShort(_) => todo!(),
            Message::Propose(p) => self.handle_propose(p).await,
        }
    }

    async fn handle_accept_reply(&self, ar: AcceptReply) {
        trace!("handle accept reply");

        let r = self.replica.lock().await;
        let orig = r
            .instance_space
            .get_instance(&ar.instance_id.replica, &ar.instance_id.local)
            .await;

        if !instance_exist(&orig).await {
            panic!("The instance {:?} should exist", ar.instance_id);
        }

        let orig = orig.unwrap();
        let mut ins_write = orig.get_instance_write().await;
        let ins = ins_write.as_mut().unwrap();

        if !ar.ok {
            ins.lb.nack += 1;
            if ar.ballot > ins.lb.max_ballot {
                ins.lb.max_ballot = ar.ballot;
            }
            return;
        }

        ins.lb.accept_ok += 1;

        if ins.lb.accept_ok >= r.peer_cnt / 2 {
            ins.status = InstanceStatus::Committed;
            // TODO: sync to disk
            self.bdcst_message(
                r.id,
                Message::<C>::Commit(Commit {
                    leader_id: r.id.into(),
                    instance_id: ar.instance_id,
                    seq: ins.seq,
                    cmds: ins.cmds.to_vec(),
                    deps: ins.deps.to_vec(),
                }),
            )
            .await;

            drop(ins_write);

            // TODO: sync to disk
            let _ = r.exec_send.send(orig.clone()).await;
            orig.notify_commit().await;
        }
    }

    async fn handle_accept(&self, a: Accept) {
        trace!("handle accept");

        let mut r = self.replica.lock().await;

        if a.instance_id.local >= r.cur_instance(&a.instance_id.replica) {
            r.set_cur_instance(&InstanceId::new(
                a.instance_id.replica,
                (*a.instance_id.local + 1).into(),
            ));
        }

        let ins = r
            .instance_space
            .get_instance(&a.instance_id.replica, &a.instance_id.local)
            .await;

        let exist = instance_exist(&ins).await;
        if exist {
            let ins = ins.unwrap();
            let ins_read = ins.get_instance_read().await;
            let ins_read_inner = ins_read.as_ref().unwrap();
            if matches!(
                ins_read_inner.status,
                InstanceStatus::Committed | InstanceStatus::Executed
            ) {
                // We've translated to the later states
                return;
            }

            let ins_ballot = ins_read_inner.ballot;
            if a.ballot < ins_ballot {
                self.reply(
                    r.id,
                    &a.leader_id,
                    Message::<C>::AcceptReply(AcceptReply {
                        instance_id: a.instance_id,
                        ok: false,
                        ballot: ins_ballot,
                    }),
                )
                .await;
                return;
            }

            drop(ins_read);

            let mut ins_write = ins.get_instance_write().await;
            let ins_write_inner = ins_write.as_mut().unwrap();
            ins_write_inner.status = InstanceStatus::Accepted;
            ins_write_inner.seq = a.seq;
            ins_write_inner.deps = a.deps;
        } else {
            // Message reordering ?
            let new_instance = SharedInstance::new(
                Some(Instance {
                    id: a.instance_id,
                    seq: a.seq,
                    ballot: a.ballot,
                    cmds: vec![],
                    deps: a.deps,
                    status: InstanceStatus::Accepted,
                    lb: self.new_leaderbook().await,
                }),
                None,
            );
            r.instance_space
                .insert_instance(&a.instance_id.replica, &a.instance_id.local, new_instance)
                .await;
        }

        // TODO: sync to disk

        self.reply(
            r.id,
            &a.leader_id,
            Message::<C>::AcceptReply(AcceptReply {
                instance_id: a.instance_id,
                ok: true,
                ballot: a.ballot,
            }),
        )
        .await;
    }

    async fn handle_commit(&self, cm: Commit<C>) {
        trace!("handle commit");
        let mut r = self.replica.lock().await;

        if cm.instance_id.local >= r.cur_instance(&cm.instance_id.replica) {
            r.set_cur_instance(&InstanceId {
                replica: cm.instance_id.replica,
                local: (*cm.instance_id.local + 1).into(),
            });
        }

        let ins = r
            .instance_space
            .get_instance(&cm.instance_id.replica, &cm.instance_id.local)
            .await;

        let exist = instance_exist(&ins).await;

        let ins = if exist {
            let orig = ins.unwrap();
            let mut ins = orig.get_instance_write().await;
            let mut ins_inner = ins.as_mut().unwrap();
            ins_inner.seq = cm.seq;
            ins_inner.deps = cm.deps;
            ins_inner.status = InstanceStatus::Committed;
            drop(ins);
            orig
        } else {
            let new_instance = SharedInstance::new(
                Some(Instance {
                    id: cm.instance_id,
                    seq: cm.seq,
                    ballot: self.new_ballot().await,
                    cmds: cm.cmds.to_vec(),
                    deps: cm.deps,
                    status: InstanceStatus::Committed,
                    lb: self.new_leaderbook().await,
                }),
                None,
            );
            r.instance_space
                .insert_instance(
                    &cm.instance_id.replica,
                    &cm.instance_id.local,
                    new_instance.clone(),
                )
                .await;
            r.update_conflict(&cm.instance_id.replica, &cm.cmds, new_instance.clone())
                .await;
            new_instance
        };

        // TODO: sync to disk

        // TODO: Handle Error
        let _ = r.exec_send.send(ins.clone()).await;
        ins.notify_commit().await;
    }

    async fn handle_preaccept_reply(&self, par: PreAcceptReply) {
        trace!("handle preaccept reply");
        let r = self.replica.lock().await;

        let ins = r
            .instance_space
            .get_instance(&par.instance_id.replica, &par.instance_id.local)
            .await;

        if !instance_exist(&ins).await {
            panic!("This instance should already in the space");
        }

        // We've checked the existence
        let orig = ins.unwrap();
        let mut ins_write = orig.get_instance_write().await;
        let mut ins = ins_write.as_mut().unwrap();

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

        let equal = r.merge_seq_deps(ins, &par.seq, &par.deps);
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

            drop(ins_write);
            let _ = r.exec_send.send(orig.clone()).await;
            orig.notify_commit().await;
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

        let ins = r
            .instance_space
            .get_instance(&pao.instance_id.replica, &pao.instance_id.local)
            .await;

        if !instance_exist(&ins).await {
            panic!("This instance should already in the space");
        }

        let ins = ins.unwrap();
        let mut ins_write = ins.get_instance_write().await;
        let mut ins_write_inner = ins_write.as_mut().unwrap();

        if !matches!(ins_write_inner.status, InstanceStatus::PreAccepted) {
            // We've translated to the later states
            return;
        }

        if !ins_write_inner.ballot.is_init() {
            // only the first leader can send ok
            return;
        }

        ins_write_inner.lb.preaccept_ok += 1;

        // TODO: remove duplicate code
        if ins_write_inner.lb.preaccept_ok >= r.peer_cnt / 2
            && ins_write_inner.lb.all_equal
            && ins_write_inner.ballot.is_init()
        {
            ins_write_inner.status = InstanceStatus::Committed;
            // TODO: sync to disk
            self.bdcst_message(
                r.id,
                Message::<C>::Commit(Commit {
                    leader_id: r.id.into(),
                    instance_id: pao.instance_id,
                    seq: ins_write_inner.seq,
                    cmds: ins_write_inner.cmds.to_vec(),
                    deps: ins_write_inner.deps.to_vec(),
                }),
            )
            .await;
            drop(ins_write);
            let _ = r.exec_send.send(ins.clone()).await;
            ins.notify_commit().await;
        } else if ins_write_inner.lb.preaccept_ok >= r.peer_cnt / 2 {
            ins_write_inner.status = InstanceStatus::Accepted;
            self.bdcst_message(
                r.id,
                Message::<C>::Accept(Accept {
                    leader_id: r.id.into(),
                    instance_id: pao.instance_id,
                    ballot: ins_write_inner.ballot,
                    seq: ins_write_inner.seq,
                    cmd_cnt: ins_write_inner.cmds.len(),
                    deps: ins_write_inner.deps.to_vec(),
                }),
            )
            .await;
        }
    }

    async fn handle_preaccept(&self, pa: PreAccept<C>) {
        trace!("handle preaccept {:?}", pa);
        let mut r = self.replica.lock().await;
        let ins = r
            .instance_space
            .get_instance(&pa.instance_id.replica, &pa.instance_id.local)
            .await;

        if instance_exist(&ins).await {
            // TODO: abstract to a macro
            let ins = ins.unwrap();
            let ins_read = ins.get_instance_read().await;
            let ins_read_inner = ins_read.as_ref().unwrap();

            // We've got accpet or commit before, don't reply
            if matches!(
                ins_read_inner.status,
                InstanceStatus::Committed | InstanceStatus::Accepted
            ) {
                // Later message may not contain commands, we should fill it here
                if ins_read_inner.cmds.is_empty() {
                    drop(ins_read);
                    // TODO: abstract to a macro
                    let mut inst_write = ins.get_instance_write().await;
                    let mut inst_write_inner = inst_write.as_mut().unwrap();
                    inst_write_inner.cmds = pa.cmds;
                }
                return;
            }

            // smaller Ballot number
            if pa.ballot < ins_read_inner.ballot {
                self.reply(
                    r.id,
                    &pa.leader_id,
                    Message::<C>::PreAcceptReply(PreAcceptReply {
                        instance_id: pa.instance_id,
                        seq: ins_read_inner.seq,
                        ballot: ins_read_inner.ballot,
                        ok: false,
                        deps: ins_read_inner.deps.to_vec(),
                        committed_deps: r.commited_upto.to_vec(),
                    }),
                )
                .await;
                return;
            }
        }

        if pa.instance_id.local > r.cur_instance(&pa.instance_id.replica) {
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

        let new_instance = SharedInstance::new(
            Some(Instance {
                id: pa.instance_id,
                seq,
                ballot: pa.ballot,
                // FIXME: should not copy
                cmds: pa.cmds.to_vec(),
                // FIXME: Should not copy if we send reply ok later
                deps: deps.to_vec(),
                status,
                lb: self.new_leaderbook().await,
            }),
            None,
        );

        r.instance_space
            .insert_instance(
                &pa.instance_id.replica,
                &pa.instance_id.local,
                new_instance.clone(),
            )
            .await;

        r.update_conflict(&pa.instance_id.replica, &pa.cmds, new_instance)
            .await;

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
        let inst_id = *r.local_cur_instance();
        r.inc_local_cur_instance();
        let (seq, deps) = r.get_seq_deps(&p.cmds).await;

        let new_ins = SharedInstance::new(
            Some(Instance {
                id: InstanceId {
                    local: inst_id,
                    replica: r.id,
                },
                seq,
                ballot: self.new_ballot().await,
                cmds: p.cmds,
                deps,
                status: InstanceStatus::PreAccepted,
                lb: self.new_leaderbook().await,
            }),
            None,
        );

        let new_ins_read = new_ins.get_instance_read().await;
        let new_ins_read_inner = new_ins_read.as_ref().unwrap();

        let r_id = r.id;
        r.update_conflict(&r_id, &new_ins_read_inner.cmds, new_ins.clone())
            .await;

        let r_id = r.id;
        r.instance_space
            .insert_instance(&r_id, &inst_id, new_ins.clone())
            .await;

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
                    local: inst_id,
                },
                seq,
                ballot: self.new_ballot().await,
                // FIXME: should not copy/clone vec
                cmds: new_ins_read_inner.cmds.to_vec(),
                deps: new_ins_read_inner.deps.to_vec(),
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

pub(crate) struct RpcServer<C, E, S>
where
    C: Command + Clone + Send + Sync + 'static,
    E: CommandExecutor<C> + Clone,
    S: InstanceSpace<C>,
{
    server: Arc<InnerServer<C, E, S>>,
    listener: TcpListener,
}

impl<C, E, S> RpcServer<C, E, S>
where
    C: Command + Debug + Clone + Serialize + DeserializeOwned + Send + Sync + 'static,
    E: CommandExecutor<C> + Debug + Clone + Send + Sync + 'static,
    S: InstanceSpace<C> + Send + Sync + 'static,
{
    pub(crate) async fn new(config: &Configure, server: Arc<InnerServer<C, E, S>>) -> Self {
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
