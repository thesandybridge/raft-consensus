use std::{collections::HashMap, time::{Duration}};
use tokio::time::{interval, Instant};
use tokio::sync::mpsc;

type NodeId = u64;

#[derive(Clone, Debug)]
struct LogEntry {
    term: u64,
    index: u64,
    command: Vec<u8>,
}

#[derive(Debug, Clone)]
struct RequestVoteArgs {
    term: u64,
    candidate_id: NodeId,
    last_log_index: u64,
    last_log_term: u64,
}

#[derive(Debug, Clone)]
struct RequestVoteReply {
    from: NodeId,
    term: u64,
    vote_granted: bool,
}

#[derive(Debug, Clone)]
struct AppendEntriesArgs {
    term: u64,
    leader_id: NodeId,
    prev_log_index: u64,
    prev_log_term: u64,
    entries: Vec<LogEntry>,
    leader_commit: u64,
}

#[derive(Debug, Clone)]
struct AppendEntriesReply {
    from: NodeId,
    term: u64,
    success: bool,
    match_index: u64,
}

struct LeaderState {
    next_index: HashMap<NodeId, u64>,
    match_index: HashMap<NodeId, u64>,
}

struct CandidateState {
    votes_received: usize,
}

struct PersistentState {
    current_term: u64,
    voted_for: Option<NodeId>,
    log: Vec<LogEntry>
}

struct VolatileState {
    commit_index: u64,
    last_applied: u64
}

#[derive(Debug, Clone)]
enum Message {
    RequestVote(RequestVoteArgs),
    RequestVoteReply(RequestVoteReply),
    AppendEntries(AppendEntriesArgs),
    AppendEntriesReply(AppendEntriesReply),
}

enum Event {
    MessageReceived(Message),
    ElectionTimeout,
    HeartbeatTimeout,
}

enum Action {
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

enum Role {
    Follower,
    Candidate(CandidateState),
    Leader(LeaderState),
}

struct RaftNode {
    id: NodeId,
    role: Role,
    persistent: PersistentState,
    volatile: VolatileState,
    peers: Vec<NodeId>,
    election_deadline: Instant,
    //heartbeat_interval: Duration,
    //last_heartbeat: Instant,
    tx: mpsc::UnboundedSender<(NodeId, Message)>,
    rx: mpsc::UnboundedReceiver<Message>,
}

impl RaftNode {
    fn new(
        id: NodeId,
        peers: Vec<NodeId>,
        tx: mpsc::UnboundedSender<(NodeId, Message)>,
        rx: mpsc::UnboundedReceiver<Message>,
    ) -> Self {
        let timeout_ms = 150 + (rand::random::<u64>() % 150);
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
            election_deadline: Instant::now() + Duration::from_millis(timeout_ms),
            tx,
            rx,
        }
    }
    fn handle_request_vote(&mut self, args: RequestVoteArgs) -> Vec<Action> {
        let mut actions = vec![];

        // Step 1: If their term is greater, update and become follower
        if args.term > self.persistent.current_term {
            actions.push(Action::UpdateTerm(args.term));
            actions.push(Action::BecomeFollower);

            // Check if log is up-to-date (voted_for will be None after UpdateTerm)
            let my_last_term = self.persistent.log.last().map(|e| e.term).unwrap_or(0);
            let my_last_index = self.persistent.log.last().map(|e| e.index).unwrap_or(0);
            let log_is_up_to_date = args.last_log_term > my_last_term
            || (args.last_log_term == my_last_term && args.last_log_index >= my_last_index);

            if log_is_up_to_date {
                actions.push(Action::VoteFor(args.candidate_id));
                actions.push(Action::ResetElectionTimer);
                actions.push(Action::SendMessage {
                    to: args.candidate_id,
                    msg: Message::RequestVoteReply(RequestVoteReply {
                        from: self.id,
                        term: args.term,
                        vote_granted: true,
                    })
                });
            } else {
                actions.push(Action::SendMessage {
                    to: args.candidate_id,
                    msg: Message::RequestVoteReply(RequestVoteReply {
                        from: self.id,
                        term: args.term,
                        vote_granted: false,
                    })
                });
            }
            return actions;
        }

        // Step 2: If their term is less, reject immediately
        if args.term < self.persistent.current_term {
            actions.push(Action::SendMessage {
                to: args.candidate_id,
                msg: Message::RequestVoteReply(RequestVoteReply {
                    from: self.id,
                    term: self.persistent.current_term,
                    vote_granted: false,
                })
            });
            return actions;
        }

        // Step 3: Same term - check both conditions
        let can_vote = self.persistent.voted_for.is_none()
        || self.persistent.voted_for == Some(args.candidate_id);

        let my_last_term = self.persistent.log.last().map(|e| e.term).unwrap_or(0);
        let my_last_index = self.persistent.log.last().map(|e| e.index).unwrap_or(0);

        let log_is_up_to_date = args.last_log_term > my_last_term
        || (args.last_log_term == my_last_term && args.last_log_index >= my_last_index);

        if can_vote && log_is_up_to_date {
            actions.push(Action::VoteFor(args.candidate_id));
            actions.push(Action::ResetElectionTimer);
            actions.push(Action::SendMessage {
                to: args.candidate_id,
                msg: Message::RequestVoteReply(RequestVoteReply {
                    from: self.id,
                    term: self.persistent.current_term,
                    vote_granted: true,
                })
            });
        } else {
            actions.push(Action::SendMessage {
                to: args.candidate_id,
                msg: Message::RequestVoteReply(RequestVoteReply {
                    from: self.id,
                    term: self.persistent.current_term,
                    vote_granted: false,
                })
            });
        }

        actions
    }

    fn handle_request_vote_reply(&mut self, reply: RequestVoteReply) -> Vec<Action> {
        let mut actions = vec![];

        // Step 1: Only candidates care about vote replies
        let candidate_state = match &mut self.role {
            Role::Candidate(state) => state,
            _ => return vec![], // not a candidate, ignore
        };

        // Step 2: Check term
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
            return actions;
        }

        actions
    }

    fn handle_append_entries(&mut self, args: AppendEntriesArgs) -> Vec<Action> {
        let mut actions = vec![];

        // Rule 1: Reject if term is stale
        if args.term < self.persistent.current_term {
            actions.push(Action::SendMessage {
                to: args.leader_id,
                msg: Message::AppendEntriesReply(AppendEntriesReply {
                    from: self.id,
                    term: self.persistent.current_term,
                    success: false,
                    match_index: self.persistent.log.last().map(|e| e.index).unwrap_or(0),
                })
            });
            return actions;
        }

        if args.term > self.persistent.current_term {
            actions.push(Action::UpdateTerm(args.term));
        }

        // Valid leader exists - become/stay follower and reset timer
        actions.push(Action::BecomeFollower);
        actions.push(Action::ResetElectionTimer);

        // Rule 2: Check if log matches at prevLogIndex
        if args.prev_log_index > 0 {
            // find entry where entry.index == args.prev_log_index
            let matching_entry = self.persistent.log.iter().find(|e| e.index == args.prev_log_index);

            match matching_entry {
                None => {
                    actions.push(Action::SendMessage {
                        to: args.leader_id,
                        msg: Message::AppendEntriesReply(AppendEntriesReply {
                            from: self.id,
                            term: self.persistent.current_term,
                            success: false,
                            match_index: self.persistent.log.last().map(|e| e.index).unwrap_or(0),
                        })
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
                            })
                        });
                        return actions;
                    }
                }
            }
        }

        // Rules 3-4: Handle conflicts and append entries
        for entry in args.entries {
            // Check if we have an entry at this index
            if let Some(existing_pos) = self.persistent.log.iter().position(|e| e.index == entry.index) {
                // We have an entry at this index
                if self.persistent.log[existing_pos].term != entry.term {
                    // Conflict! Truncate from this point
                    actions.push(Action::TruncateLog(existing_pos));
                } else {
                    continue;
                }
            }
            // Either no entry at this index, or we truncated - append it
            actions.push(Action::AppendLogEntry(entry.clone()));
        }

        // Rule 5: Update commitIndex
        if args.leader_commit > self.volatile.commit_index {
            let last_log_index = self.persistent.log.last().map(|e| e.index).unwrap_or(0);
            let new_commit_index = std::cmp::min(args.leader_commit, last_log_index);

            if new_commit_index > self.volatile.commit_index {
                actions.push(Action::UpdateCommitIndex(new_commit_index));
            }
        }

        actions.push(Action::SendMessage {
            to: args.leader_id,
            msg: Message::AppendEntriesReply(AppendEntriesReply {
                from: self.id,
                term: self.persistent.current_term,
                success: true,
                match_index: self.persistent.log.last().map(|e| e.index).unwrap_or(0),
            })
        });
        actions
    }

    fn handle_append_entries_reply(&mut self, reply: AppendEntriesReply) -> Vec<Action> {
        let mut actions = vec![];

        // Only leaders care
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
            return actions; // ignore stale reply
        }

        if reply.success {
            leader_state.match_index.insert(reply.from, reply.match_index);
            leader_state.next_index.insert(reply.from, reply.match_index + 1);

            let mut match_indices: Vec<u64> = leader_state.match_index.values().copied().collect();

            // Add your own log's last index (you have all your entries)
            let my_last_index = self.persistent.log.last().map(|e| e.index).unwrap_or(0);
            match_indices.push(my_last_index);

            // Sort to find the median
            match_indices.sort_unstable();

            // The majority threshold is at position cluster_size/2
            let majority_index = match_indices.len() / 2;
            let n = match_indices[majority_index];

            // Can we commit N?
            if n > self.volatile.commit_index {
                // Check if the entry at N is from current term
                if let Some(entry) = self.persistent.log.iter().find(|e| e.index == n) {
                    if entry.term == self.persistent.current_term {
                        actions.push(Action::UpdateCommitIndex(n));
                    }
                }
            }
        } else {
            // Failure - decrement nextIndex and retry
            if let Some(next) = leader_state.next_index.get_mut(&reply.from) {
                *next = next.saturating_sub(1).max(1); // don't go below 1
            }
        }

        actions
    }

    fn handle_message(&mut self, msg: Message) -> Vec<Action> {
        match msg {
            Message::RequestVote(args) => self.handle_request_vote(args),
            Message::RequestVoteReply(reply) => self.handle_request_vote_reply(reply),
            Message::AppendEntries(args) => self.handle_append_entries(args),
            Message::AppendEntriesReply(reply) => self.handle_append_entries_reply(reply),
        }
    }

    fn handle_election_timeout(&mut self) -> Vec<Action> {
        match self.role {
            Role::Leader(_) => vec![],
            Role::Follower | Role::Candidate(..) => {
                let mut actions = vec![
                    Action::BecomeCandidate,
                    Action::IncrementTerm,
                    Action::VoteForSelf,
                    Action::ResetElectionTimer,
                ];

                actions.extend(
                    self.peers.iter().map(|peer_id| {
                        Action::SendMessage {
                            to: *peer_id,
                            msg: Message::RequestVote(RequestVoteArgs {
                                term: self.persistent.current_term + 1,
                                candidate_id: self.id,
                                last_log_index: self.persistent.log.last().map(|e| e.index).unwrap_or(0),
                                last_log_term: self.persistent.log.last().map(|e| e.term).unwrap_or(0),
                            })
                        }
                    })
                );

                actions
            }
        }
    }

    fn handle_event(&mut self, event: Event) -> Vec<Action> {
        match event {
            Event::MessageReceived(msg) => self.handle_message(msg),
            Event::ElectionTimeout => self.handle_election_timeout(),
            Event::HeartbeatTimeout => self.handle_heartbeat_timeout(),
        }
    }

    fn handle_heartbeat_timeout(&mut self) -> Vec<Action> {
        match self.role {
            Role::Leader(_) => {
                // send empty AppendEntries to all peers
                self.peers.iter().map(|peer_id| {
                    Action::SendMessage {
                        to: *peer_id,
                        msg: Message::AppendEntries(AppendEntriesArgs {
                            term: self.persistent.current_term,
                            leader_id: self.id,
                            prev_log_index: self.persistent.log.last().map(|e| e.index).unwrap_or(0),
                            prev_log_term: self.persistent.log.last().map(|e| e.term).unwrap_or(0),
                            entries: Vec::new(),
                            leader_commit: self.volatile.commit_index
                        }),
                    }
                }).collect()
            }
            _ => {
                // followers and candidates don't send heartbeats
                vec![]
            }
        }
    }

    fn execute_action(&mut self, action: Action) {
        match action {
            Action::UpdateTerm(term) => {
                self.persistent.current_term = term;
                self.persistent.voted_for = None;
            }
            Action::BecomeFollower => {
                self.role = Role::Follower;
            }
            Action::BecomeCandidate => {
                println!("Node {} became CANDIDATE in term {}", self.id, self.persistent.current_term);
                self.role = Role::Candidate(CandidateState { votes_received: 1 });
            }
            Action::BecomeLeader => {
                let mut next_index = HashMap::new();
                let mut match_index = HashMap::new();
                let last_log_index = self.persistent.log.last().map(|e| e.index).unwrap_or(0);

                for peer in &self.peers {
                    next_index.insert(*peer, last_log_index + 1);
                    match_index.insert(*peer, 0);
                }

                println!("Node {} became LEADER in term {}", self.id, self.persistent.current_term);  // add this

                self.role = Role::Leader(LeaderState { next_index, match_index });
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
                // Random timeout between 150-300ms (paper's recommendation)
                let timeout_ms = 150 + (rand::random::<u64>() % 150);
                self.election_deadline = Instant::now() + Duration::from_millis(timeout_ms);
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
                let _ = self.tx.send((to, msg));
            }
        }
    }

    async fn run(mut self) {
        let mut heartbeat_timer = interval(Duration::from_millis(50));

        loop {
            // Calculate how long until election deadline
            let sleep_duration = self.election_deadline
                .saturating_duration_since(Instant::now());

            tokio::select! {
                _ = tokio::time::sleep(sleep_duration) => {
                    let actions = self.handle_event(Event::ElectionTimeout);
                    for action in actions {
                        self.execute_action(action);
                    }
                    // election_deadline was updated by ResetElectionTimer action
                }

                _ = heartbeat_timer.tick() => {
                    if matches!(self.role, Role::Leader(_)) {
                        let actions = self.handle_event(Event::HeartbeatTimeout);
                        for action in actions {
                            self.execute_action(action);
                        }
                    }
                }
                msg = self.rx.recv() => {
                    if let Some(msg) = msg {
                        let actions = self.handle_event(Event::MessageReceived(msg));
                        for action in actions {
                            self.execute_action(action);
                        }
                    }
                }
            }
        }
    }

}

struct Network {
    nodes: HashMap<NodeId, mpsc::UnboundedSender<Message>>,
}

impl Network {
    fn new() -> Self {
        Self {
            nodes: HashMap::new(),
        }
    }

    fn register_node(&mut self, id: NodeId, tx: mpsc::UnboundedSender<Message>) {
        self.nodes.insert(id, tx);
    }

    async fn route_messages(
        self,
        mut network_rx: mpsc::UnboundedReceiver<(NodeId, Message)>,
    ) {
        while let Some((to, msg)) = network_rx.recv().await {
            if let Some(tx) = self.nodes.get(&to) {
                let _ = tx.send(msg);
            }
        }
    }
}

#[tokio::main]
async fn main() {
    // Create network router
    let (network_tx, network_rx) = mpsc::unbounded_channel();
    let mut network = Network::new();

    // Create 3 nodes
    let node_ids = vec![1, 2, 3];

    let mut handles = vec![];

    for &id in &node_ids {
        let (node_tx, node_rx) = mpsc::unbounded_channel();
        network.register_node(id, node_tx);

        let peers: Vec<NodeId> = node_ids.iter().filter(|&&nid| nid != id).copied().collect();

        let node = RaftNode::new(id, peers, network_tx.clone(), node_rx);

        let handle = tokio::spawn(async move {
            node.run().await;
        });

        handles.push(handle);
    }

    // Spawn network router
    tokio::spawn(async move {
        network.route_messages(network_rx).await;
    });

    // Run for 5 seconds to see election happen
    tokio::time::sleep(Duration::from_secs(5)).await;

    println!("Shutting down...");
}
