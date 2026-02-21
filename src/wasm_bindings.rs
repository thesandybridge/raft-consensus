use wasm_bindgen::prelude::*;
use serde::Serialize;
use std::collections::VecDeque;

use crate::{LogEntry, Message, NodeId, Role, SimNode};

// ── JSON state shapes ────────────────────────────────────────────────────────

#[derive(Serialize)]
struct NodeState {
    id: u64,
    role: &'static str,
    term: u64,
    voted_for: Option<u64>,
    log_length: usize,
    commit_index: u64,
    election_timeout_pct: f64,
    votes_received: Option<usize>,
    dead: bool,
}

#[derive(Serialize)]
struct InflightState {
    id: u64,
    from: u64,
    to: u64,
    kind: &'static str,
    progress: f64,
    granted: Option<bool>,
    success: Option<bool>,
    entries_count: Option<usize>,
}

#[derive(Serialize)]
struct ClusterState<'a> {
    now_ms: u64,
    nodes: Vec<NodeState>,
    inflight: Vec<InflightState>,
    partitions: &'a Vec<[u64; 2]>,
    events: Vec<&'a str>,
    applied_commands: Vec<&'a str>,
}

// ── Internal types ───────────────────────────────────────────────────────────

struct InFlightMsg {
    id: u64,
    from: NodeId,
    to: NodeId,
    msg: Message,
    sent_at_ms: u64,
    deliver_at_ms: u64,
}

// ── WasmCluster ──────────────────────────────────────────────────────────────

#[wasm_bindgen]
pub struct WasmCluster {
    nodes: Vec<SimNode>,
    partitions: Vec<[u64; 2]>,
    inflight: Vec<InFlightMsg>,
    now_ms: u64,
    next_msg_id: u64,
    events: VecDeque<String>,
    applied_commands: VecDeque<String>,
    applied_up_to: u64,
}

#[wasm_bindgen]
impl WasmCluster {
    #[wasm_bindgen(constructor)]
    pub fn new() -> Self {
        let node_ids: Vec<u64> = (1..=5).collect();
        let nodes: Vec<SimNode> = node_ids
            .iter()
            .map(|&id| {
                let peers: Vec<u64> = node_ids.iter().filter(|&&p| p != id).copied().collect();
                SimNode::new(id, peers, 0)
            })
            .collect();

        Self {
            nodes,
            partitions: vec![],
            inflight: vec![],
            now_ms: 0,
            next_msg_id: 1,
            events: VecDeque::new(),
            applied_commands: VecDeque::new(),
            applied_up_to: 0,
        }
    }

    /// Advance simulation by `elapsed_ms` milliseconds and return JSON state.
    pub fn tick(&mut self, elapsed_ms: u32) -> String {
        self.now_ms += elapsed_ms as u64;
        self.process_step();
        self.state_json()
    }

    pub fn get_state(&self) -> String {
        self.state_json()
    }

    pub fn kill_node(&mut self, id: u32) {
        let id = id as u64;
        if let Some(node) = self.nodes.iter_mut().find(|n| n.id == id) {
            node.dead = true;
            node.outbox.clear();
        }
        self.inflight.retain(|m| m.from != id && m.to != id);
        self.add_event(format!("Node {} killed", id));
    }

    pub fn revive_node(&mut self, id: u32) {
        let id = id as u64;
        if let Some(node) = self.nodes.iter_mut().find(|n| n.id == id) {
            node.dead = false;
            node.role = Role::Follower;
            node.heartbeat_deadline_ms = u64::MAX;
            node.persistent.voted_for = None;
            let timeout_ms = 150 + rand::random::<u64>() % 150;
            node.election_deadline_ms = self.now_ms + timeout_ms;
            self.add_event(format!("Node {} revived", id));
        }
    }

    /// Toggle partition between nodes a and b. If not partitioned, adds one;
    /// if already partitioned, removes it.
    pub fn toggle_partition(&mut self, a: u32, b: u32) {
        let (a, b) = (a as u64, b as u64);
        if a == b {
            return;
        }
        let pair = if a < b { [a, b] } else { [b, a] };
        if let Some(idx) = self.partitions.iter().position(|p| p == &pair) {
            self.partitions.remove(idx);
            // Drain in-flight messages between the two nodes.
            self.inflight.retain(|m| !(m.from == a && m.to == b) && !(m.from == b && m.to == a));
            self.add_event(format!("Partition removed: {} <-> {}", a, b));
        } else {
            self.partitions.push(pair);
            self.inflight.retain(|m| !(m.from == a && m.to == b) && !(m.from == b && m.to == a));
            self.add_event(format!("Partition added: {} <-> {}", a, b));
        }
    }

    /// Submit a command to the current leader. No-op if there is no leader.
    pub fn submit_command(&mut self, cmd: String) {
        let leader_idx = self
            .nodes
            .iter()
            .position(|n| !n.dead && matches!(n.role, Role::Leader(_)));
        if let Some(idx) = leader_idx {
            let node = &mut self.nodes[idx];
            let next_index = node.persistent.log.last().map(|e| e.index).unwrap_or(0) + 1;
            let entry = LogEntry {
                term: node.persistent.current_term,
                index: next_index,
                command: cmd.as_bytes().to_vec(),
            };
            node.persistent.log.push(entry);
            let leader_id = node.id;
            self.add_event(format!("Command \"{}\" sent to leader {}", cmd, leader_id));
        } else {
            self.add_event("No leader — command dropped".to_string());
        }
    }
}

// ── Private helpers ──────────────────────────────────────────────────────────

impl WasmCluster {
    fn add_event(&mut self, msg: String) {
        self.events.push_back(msg);
        if self.events.len() > 40 {
            self.events.pop_front();
        }
    }

    fn is_partitioned(&self, a: NodeId, b: NodeId) -> bool {
        let pair = if a < b { [a, b] } else { [b, a] };
        self.partitions.contains(&pair)
    }

    fn network_delay() -> u64 {
        15 + rand::random::<u64>() % 25 // 15–40 ms
    }

    fn role_name(role: &Role) -> &'static str {
        match role {
            Role::Follower => "follower",
            Role::Candidate(_) => "candidate",
            Role::Leader(_) => "leader",
        }
    }

    fn process_step(&mut self) {
        let now = self.now_ms;

        // 1. Separate due messages from in-transit
        let mut new_inflight: Vec<InFlightMsg> = Vec::new();
        let mut to_deliver: Vec<InFlightMsg> = Vec::new();
        for msg in self.inflight.drain(..) {
            if msg.deliver_at_ms <= now {
                to_deliver.push(msg);
            } else {
                new_inflight.push(msg);
            }
        }
        self.inflight = new_inflight;

        // 2. Deliver messages
        for in_flight in to_deliver {
            let dest = in_flight.to;
            if let Some(idx) = self.nodes.iter().position(|n| n.id == dest && !n.dead) {
                let prev = Self::role_name(&self.nodes[idx].role);
                let prev_term = self.nodes[idx].persistent.current_term;
                let actions = self.nodes[idx].handle_message(in_flight.msg);
                for action in actions {
                    self.nodes[idx].execute_action(action, now);
                }
                let next = Self::role_name(&self.nodes[idx].role);
                let next_term = self.nodes[idx].persistent.current_term;
                if prev != next {
                    let id = self.nodes[idx].id;
                    self.add_event(format!("Node {} -> {} (term {})", id, next, next_term));
                } else if prev_term != next_term && next == "follower" {
                    let id = self.nodes[idx].id;
                    self.add_event(format!("Node {} bumped to term {}", id, next_term));
                }
            }
        }

        // 3. Election timeouts
        for i in 0..self.nodes.len() {
            if self.nodes[i].dead {
                continue;
            }
            if self.nodes[i].election_deadline_ms <= now {
                let prev = Self::role_name(&self.nodes[i].role);
                let actions = self.nodes[i].handle_election_timeout();
                for action in actions {
                    self.nodes[i].execute_action(action, now);
                }
                let next = Self::role_name(&self.nodes[i].role);
                if prev != next {
                    let id = self.nodes[i].id;
                    let term = self.nodes[i].persistent.current_term;
                    self.add_event(format!("Node {} -> {} (term {})", id, next, term));
                }
            }
        }

        // 4. Heartbeat timeouts
        for i in 0..self.nodes.len() {
            if self.nodes[i].dead {
                continue;
            }
            if matches!(self.nodes[i].role, Role::Leader(_))
                && self.nodes[i].heartbeat_deadline_ms <= now
            {
                let prev = Self::role_name(&self.nodes[i].role);
                let actions = self.nodes[i].handle_heartbeat_timeout();
                for action in actions {
                    self.nodes[i].execute_action(action, now);
                }
                let next = Self::role_name(&self.nodes[i].role);
                if prev != next {
                    let id = self.nodes[i].id;
                    let term = self.nodes[i].persistent.current_term;
                    self.add_event(format!("Node {} -> {} (term {})", id, next, term));
                }
                self.nodes[i].heartbeat_deadline_ms = now + 50;
            }
        }

        // 5. Drain outboxes -> inflight
        for i in 0..self.nodes.len() {
            if self.nodes[i].dead {
                continue;
            }
            let from = self.nodes[i].id;
            let outbox = self.nodes[i].drain_outbox();
            for (to, msg) in outbox {
                if !self.is_partitioned(from, to) {
                    let delay = Self::network_delay();
                    let id = self.next_msg_id;
                    self.next_msg_id += 1;
                    self.inflight.push(InFlightMsg {
                        id,
                        from,
                        to,
                        msg,
                        sent_at_ms: now,
                        deliver_at_ms: now + delay,
                    });
                }
            }
        }

        // 6. Record newly committed entries
        self.update_applied();
    }

    fn update_applied(&mut self) {
        let max_commit = self
            .nodes
            .iter()
            .filter(|n| !n.dead)
            .map(|n| n.volatile.commit_index)
            .max()
            .unwrap_or(0);

        if max_commit > self.applied_up_to {
            // Find a node with the committed entries
            let node_idx = self
                .nodes
                .iter()
                .position(|n| !n.dead && n.volatile.commit_index >= max_commit);

            if let Some(idx) = node_idx {
                for log_idx in (self.applied_up_to + 1)..=max_commit {
                    if let Some(entry) = self.nodes[idx]
                        .persistent
                        .log
                        .iter()
                        .find(|e| e.index == log_idx)
                    {
                        let cmd = String::from_utf8_lossy(&entry.command).into_owned();
                        self.add_event(format!("Committed \"{}\" (index {})", cmd, log_idx));
                        self.applied_commands.push_back(cmd);
                        if self.applied_commands.len() > 15 {
                            self.applied_commands.pop_front();
                        }
                    }
                }
                self.applied_up_to = max_commit;
            }
        }
    }

    fn state_json(&self) -> String {
        let nodes: Vec<NodeState> = self
            .nodes
            .iter()
            .map(|n| {
                let (role, votes) = match &n.role {
                    Role::Follower => ("follower", None),
                    Role::Candidate(s) => ("candidate", Some(s.votes_received)),
                    Role::Leader(_) => ("leader", None),
                };
                let election_timeout_pct = if n.dead || matches!(n.role, Role::Leader(_)) {
                    0.0
                } else {
                    let remaining = n.election_deadline_ms.saturating_sub(self.now_ms);
                    (1.0_f64 - remaining as f64 / 300.0_f64).clamp(0.0, 1.0)
                };
                NodeState {
                    id: n.id,
                    role,
                    term: n.persistent.current_term,
                    voted_for: n.persistent.voted_for,
                    log_length: n.persistent.log.len(),
                    commit_index: n.volatile.commit_index,
                    election_timeout_pct,
                    votes_received: votes,
                    dead: n.dead,
                }
            })
            .collect();

        let inflight: Vec<InflightState> = self
            .inflight
            .iter()
            .map(|m| {
                let total = (m.deliver_at_ms - m.sent_at_ms).max(1);
                let elapsed = self.now_ms.saturating_sub(m.sent_at_ms);
                let progress = (elapsed as f64 / total as f64).clamp(0.0, 1.0);
                let (kind, granted, success, entries_count) = match &m.msg {
                    Message::RequestVote(_) => ("RequestVote", None, None, None),
                    Message::RequestVoteReply(r) => {
                        ("RequestVoteReply", Some(r.vote_granted), None, None)
                    }
                    Message::AppendEntries(a) => {
                        ("AppendEntries", None, None, Some(a.entries.len()))
                    }
                    Message::AppendEntriesReply(r) => {
                        ("AppendEntriesReply", None, Some(r.success), None)
                    }
                };
                InflightState {
                    id: m.id,
                    from: m.from,
                    to: m.to,
                    kind,
                    progress,
                    granted,
                    success,
                    entries_count,
                }
            })
            .collect();

        let events: Vec<&str> = self.events.iter().map(|s| s.as_str()).collect();
        let applied_commands: Vec<&str> =
            self.applied_commands.iter().map(|s| s.as_str()).collect();

        let state = ClusterState {
            now_ms: self.now_ms,
            nodes,
            inflight,
            partitions: &self.partitions,
            events,
            applied_commands,
        };

        serde_json::to_string(&state).unwrap_or_default()
    }
}
