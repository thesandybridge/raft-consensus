use std::collections::HashMap;

pub type NodeId = u64;

#[derive(Clone, Debug)]
pub struct LogEntry {
    pub term: u64,
    pub index: u64,
    pub command: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct RequestVoteArgs {
    pub term: u64,
    pub candidate_id: NodeId,
    pub last_log_index: u64,
    pub last_log_term: u64,
}

#[derive(Debug, Clone)]
pub struct RequestVoteReply {
    pub from: NodeId,
    pub term: u64,
    pub vote_granted: bool,
}

#[derive(Debug, Clone)]
pub struct AppendEntriesArgs {
    pub term: u64,
    pub leader_id: NodeId,
    pub prev_log_index: u64,
    pub prev_log_term: u64,
    pub entries: Vec<LogEntry>,
    pub leader_commit: u64,
}

#[derive(Debug, Clone)]
pub struct AppendEntriesReply {
    pub from: NodeId,
    pub term: u64,
    pub success: bool,
    pub match_index: u64,
}

#[derive(Debug, Clone)]
pub enum Message {
    RequestVote(RequestVoteArgs),
    RequestVoteReply(RequestVoteReply),
    AppendEntries(AppendEntriesArgs),
    AppendEntriesReply(AppendEntriesReply),
}

pub enum Event {
    MessageReceived(Message),
    ElectionTimeout,
    HeartbeatTimeout,
}

pub enum Action {
    SendMessage { to: NodeId, msg: Message },
    BecomeFollower,
    BecomeCandidate,
    BecomeLeader,
    ResetElectionTimer,
    IncrementTerm,
    VoteForSelf,
    UpdateTerm(u64),
    VoteFor(NodeId),
    TruncateLog(usize),
    AppendLogEntry(LogEntry),
    UpdateCommitIndex(u64),
}

pub struct LeaderState {
    pub next_index: HashMap<NodeId, u64>,
    pub match_index: HashMap<NodeId, u64>,
}

pub struct CandidateState {
    pub votes_received: usize,
}

pub struct PersistentState {
    pub current_term: u64,
    pub voted_for: Option<NodeId>,
    pub log: Vec<LogEntry>,
}

pub struct VolatileState {
    pub commit_index: u64,
    pub last_applied: u64,
}

pub enum Role {
    Follower,
    Candidate(CandidateState),
    Leader(LeaderState),
}

/// Pure simulation node — no tokio, no channels.
/// Driven externally by WasmCluster.
pub struct SimNode {
    pub id: NodeId,
    pub role: Role,
    pub persistent: PersistentState,
    pub volatile: VolatileState,
    pub peers: Vec<NodeId>,
    pub election_deadline_ms: u64,
    pub heartbeat_deadline_ms: u64,
    pub outbox: Vec<(NodeId, Message)>,
    pub dead: bool,
}

impl SimNode {
    pub fn new(id: NodeId, peers: Vec<NodeId>, now_ms: u64) -> Self {
        let timeout_ms = 150 + rand::random::<u64>() % 150;
        Self {
            id,
            role: Role::Follower,
            persistent: PersistentState {
                current_term: 0,
                voted_for: None,
                log: Vec::new(),
            },
            volatile: VolatileState {
                commit_index: 0,
                last_applied: 0,
            },
            peers,
            election_deadline_ms: now_ms + timeout_ms,
            heartbeat_deadline_ms: u64::MAX,
            outbox: Vec::new(),
            dead: false,
        }
    }

    pub fn drain_outbox(&mut self) -> Vec<(NodeId, Message)> {
        std::mem::take(&mut self.outbox)
    }

    pub fn handle_message(&mut self, msg: Message) -> Vec<Action> {
        match msg {
            Message::RequestVote(args) => self.handle_request_vote(args),
            Message::RequestVoteReply(reply) => self.handle_request_vote_reply(reply),
            Message::AppendEntries(args) => self.handle_append_entries(args),
            Message::AppendEntriesReply(reply) => self.handle_append_entries_reply(reply),
        }
    }

    pub fn handle_event(&mut self, event: Event) -> Vec<Action> {
        match event {
            Event::MessageReceived(msg) => self.handle_message(msg),
            Event::ElectionTimeout => self.handle_election_timeout(),
            Event::HeartbeatTimeout => self.handle_heartbeat_timeout(),
        }
    }

    pub fn handle_election_timeout(&mut self) -> Vec<Action> {
        match self.role {
            Role::Leader(_) => vec![],
            Role::Follower | Role::Candidate(..) => {
                let mut actions = vec![
                    Action::BecomeCandidate,
                    Action::IncrementTerm,
                    Action::VoteForSelf,
                    Action::ResetElectionTimer,
                ];
                actions.extend(self.peers.iter().map(|&peer_id| {
                    Action::SendMessage {
                        to: peer_id,
                        msg: Message::RequestVote(RequestVoteArgs {
                            term: self.persistent.current_term + 1,
                            candidate_id: self.id,
                            last_log_index: self.persistent.log.last().map(|e| e.index).unwrap_or(0),
                            last_log_term: self.persistent.log.last().map(|e| e.term).unwrap_or(0),
                        }),
                    }
                }));
                actions
            }
        }
    }

    /// Sends proper per-peer AppendEntries (including any unreplicated entries).
    pub fn handle_heartbeat_timeout(&mut self) -> Vec<Action> {
        let leader_state = match &self.role {
            Role::Leader(state) => state,
            _ => return vec![],
        };

        let current_term = self.persistent.current_term;
        let leader_id = self.id;
        let commit_index = self.volatile.commit_index;

        let mut actions = vec![];
        for &peer_id in &self.peers {
            let next_idx = leader_state.next_index.get(&peer_id).copied().unwrap_or(1);
            let prev_log_index = next_idx.saturating_sub(1);
            let prev_log_term = if prev_log_index == 0 {
                0
            } else {
                self.persistent.log.iter()
                    .find(|e| e.index == prev_log_index)
                    .map(|e| e.term)
                    .unwrap_or(0)
            };
            let entries: Vec<LogEntry> = self.persistent.log.iter()
                .filter(|e| e.index >= next_idx)
                .cloned()
                .collect();
            actions.push(Action::SendMessage {
                to: peer_id,
                msg: Message::AppendEntries(AppendEntriesArgs {
                    term: current_term,
                    leader_id,
                    prev_log_index,
                    prev_log_term,
                    entries,
                    leader_commit: commit_index,
                }),
            });
        }
        actions
    }

    fn handle_request_vote(&mut self, args: RequestVoteArgs) -> Vec<Action> {
        let mut actions = vec![];

        if args.term > self.persistent.current_term {
            actions.push(Action::UpdateTerm(args.term));
            actions.push(Action::BecomeFollower);

            let my_last_term = self.persistent.log.last().map(|e| e.term).unwrap_or(0);
            let my_last_index = self.persistent.log.last().map(|e| e.index).unwrap_or(0);
            let log_ok = args.last_log_term > my_last_term
                || (args.last_log_term == my_last_term && args.last_log_index >= my_last_index);

            if log_ok {
                actions.push(Action::VoteFor(args.candidate_id));
                actions.push(Action::ResetElectionTimer);
                actions.push(Action::SendMessage {
                    to: args.candidate_id,
                    msg: Message::RequestVoteReply(RequestVoteReply {
                        from: self.id,
                        term: args.term,
                        vote_granted: true,
                    }),
                });
            } else {
                actions.push(Action::SendMessage {
                    to: args.candidate_id,
                    msg: Message::RequestVoteReply(RequestVoteReply {
                        from: self.id,
                        term: args.term,
                        vote_granted: false,
                    }),
                });
            }
            return actions;
        }

        if args.term < self.persistent.current_term {
            actions.push(Action::SendMessage {
                to: args.candidate_id,
                msg: Message::RequestVoteReply(RequestVoteReply {
                    from: self.id,
                    term: self.persistent.current_term,
                    vote_granted: false,
                }),
            });
            return actions;
        }

        let can_vote = self.persistent.voted_for.is_none()
            || self.persistent.voted_for == Some(args.candidate_id);
        let my_last_term = self.persistent.log.last().map(|e| e.term).unwrap_or(0);
        let my_last_index = self.persistent.log.last().map(|e| e.index).unwrap_or(0);
        let log_ok = args.last_log_term > my_last_term
            || (args.last_log_term == my_last_term && args.last_log_index >= my_last_index);

        if can_vote && log_ok {
            actions.push(Action::VoteFor(args.candidate_id));
            actions.push(Action::ResetElectionTimer);
            actions.push(Action::SendMessage {
                to: args.candidate_id,
                msg: Message::RequestVoteReply(RequestVoteReply {
                    from: self.id,
                    term: self.persistent.current_term,
                    vote_granted: true,
                }),
            });
        } else {
            actions.push(Action::SendMessage {
                to: args.candidate_id,
                msg: Message::RequestVoteReply(RequestVoteReply {
                    from: self.id,
                    term: self.persistent.current_term,
                    vote_granted: false,
                }),
            });
        }
        actions
    }

    fn handle_request_vote_reply(&mut self, reply: RequestVoteReply) -> Vec<Action> {
        let mut actions = vec![];

        let candidate_state = match &mut self.role {
            Role::Candidate(state) => state,
            _ => return vec![],
        };

        if reply.term > self.persistent.current_term {
            actions.push(Action::UpdateTerm(reply.term));
            actions.push(Action::BecomeFollower);
            return actions;
        }
        if reply.term < self.persistent.current_term {
            return actions;
        }
        if reply.vote_granted {
            candidate_state.votes_received += 1;
        }

        let cluster_size = self.peers.len() + 1;
        let majority = (cluster_size / 2) + 1;
        if candidate_state.votes_received >= majority {
            actions.push(Action::BecomeLeader);
        }
        actions
    }

    fn handle_append_entries(&mut self, args: AppendEntriesArgs) -> Vec<Action> {
        let mut actions = vec![];

        if args.term < self.persistent.current_term {
            actions.push(Action::SendMessage {
                to: args.leader_id,
                msg: Message::AppendEntriesReply(AppendEntriesReply {
                    from: self.id,
                    term: self.persistent.current_term,
                    success: false,
                    match_index: self.persistent.log.last().map(|e| e.index).unwrap_or(0),
                }),
            });
            return actions;
        }

        if args.term > self.persistent.current_term {
            actions.push(Action::UpdateTerm(args.term));
        }

        actions.push(Action::BecomeFollower);
        actions.push(Action::ResetElectionTimer);

        if args.prev_log_index > 0 {
            match self.persistent.log.iter().find(|e| e.index == args.prev_log_index) {
                None => {
                    actions.push(Action::SendMessage {
                        to: args.leader_id,
                        msg: Message::AppendEntriesReply(AppendEntriesReply {
                            from: self.id,
                            term: self.persistent.current_term,
                            success: false,
                            match_index: self.persistent.log.last().map(|e| e.index).unwrap_or(0),
                        }),
                    });
                    return actions;
                }
                Some(entry) => {
                    if entry.term != args.prev_log_term {
                        actions.push(Action::SendMessage {
                            to: args.leader_id,
                            msg: Message::AppendEntriesReply(AppendEntriesReply {
                                from: self.id,
                                term: self.persistent.current_term,
                                success: false,
                                match_index: self.persistent.log.last().map(|e| e.index).unwrap_or(0),
                            }),
                        });
                        return actions;
                    }
                }
            }
        }

        for entry in args.entries {
            if let Some(existing_pos) = self.persistent.log.iter().position(|e| e.index == entry.index) {
                if self.persistent.log[existing_pos].term != entry.term {
                    actions.push(Action::TruncateLog(existing_pos));
                } else {
                    continue;
                }
            }
            actions.push(Action::AppendLogEntry(entry.clone()));
        }

        if args.leader_commit > self.volatile.commit_index {
            let last_log_index = self.persistent.log.last().map(|e| e.index).unwrap_or(0);
            let new_commit = std::cmp::min(args.leader_commit, last_log_index);
            if new_commit > self.volatile.commit_index {
                actions.push(Action::UpdateCommitIndex(new_commit));
            }
        }

        actions.push(Action::SendMessage {
            to: args.leader_id,
            msg: Message::AppendEntriesReply(AppendEntriesReply {
                from: self.id,
                term: self.persistent.current_term,
                success: true,
                match_index: self.persistent.log.last().map(|e| e.index).unwrap_or(0),
            }),
        });
        actions
    }

    fn handle_append_entries_reply(&mut self, reply: AppendEntriesReply) -> Vec<Action> {
        let mut actions = vec![];

        let leader_state = match &mut self.role {
            Role::Leader(state) => state,
            _ => return vec![],
        };

        if reply.term > self.persistent.current_term {
            actions.push(Action::UpdateTerm(reply.term));
            actions.push(Action::BecomeFollower);
            return actions;
        }
        if reply.term < self.persistent.current_term {
            return actions;
        }

        if reply.success {
            leader_state.match_index.insert(reply.from, reply.match_index);
            leader_state.next_index.insert(reply.from, reply.match_index + 1);

            let mut match_indices: Vec<u64> =
                leader_state.match_index.values().copied().collect();
            let my_last_index = self.persistent.log.last().map(|e| e.index).unwrap_or(0);
            match_indices.push(my_last_index);
            match_indices.sort_unstable();

            let majority_idx = match_indices.len() / 2;
            let n = match_indices[majority_idx];

            if n > self.volatile.commit_index {
                if let Some(entry) = self.persistent.log.iter().find(|e| e.index == n) {
                    if entry.term == self.persistent.current_term {
                        actions.push(Action::UpdateCommitIndex(n));
                    }
                }
            }
        } else {
            if let Some(next) = leader_state.next_index.get_mut(&reply.from) {
                *next = next.saturating_sub(1).max(1);
            }
        }
        actions
    }

    pub fn execute_action(&mut self, action: Action, now_ms: u64) {
        match action {
            Action::UpdateTerm(term) => {
                self.persistent.current_term = term;
                self.persistent.voted_for = None;
            }
            Action::BecomeFollower => {
                self.role = Role::Follower;
                self.heartbeat_deadline_ms = u64::MAX;
            }
            Action::BecomeCandidate => {
                self.role = Role::Candidate(CandidateState { votes_received: 1 });
                self.heartbeat_deadline_ms = u64::MAX;
            }
            Action::BecomeLeader => {
                let mut next_index = HashMap::new();
                let mut match_index = HashMap::new();
                let last_log_index = self.persistent.log.last().map(|e| e.index).unwrap_or(0);
                for &peer in &self.peers {
                    next_index.insert(peer, last_log_index + 1);
                    match_index.insert(peer, 0);
                }
                self.role = Role::Leader(LeaderState { next_index, match_index });
                self.election_deadline_ms = u64::MAX;
                self.heartbeat_deadline_ms = now_ms + 10;
            }
            Action::IncrementTerm => {
                self.persistent.current_term += 1;
                self.persistent.voted_for = None;
            }
            Action::VoteForSelf => {
                self.persistent.voted_for = Some(self.id);
            }
            Action::VoteFor(node_id) => {
                self.persistent.voted_for = Some(node_id);
            }
            Action::ResetElectionTimer => {
                let timeout_ms = 150 + rand::random::<u64>() % 150;
                self.election_deadline_ms = now_ms + timeout_ms;
            }
            Action::TruncateLog(pos) => {
                self.persistent.log.truncate(pos);
            }
            Action::AppendLogEntry(entry) => {
                self.persistent.log.push(entry);
            }
            Action::UpdateCommitIndex(index) => {
                self.volatile.commit_index = index;
            }
            Action::SendMessage { to, msg } => {
                self.outbox.push((to, msg));
            }
        }
    }
}

#[cfg(target_arch = "wasm32")]
pub mod wasm_bindings;
