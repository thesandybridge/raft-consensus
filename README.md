# Raft Consensus Algorithm

Implementation of the Raft distributed consensus protocol in Rust, built from [the original paper](https://raft.github.io/raft.pdf).

## What's Implemented

- **Leader election** - randomized timeouts, vote counting, term-based ordering
- **Log replication** - AppendEntries RPC with conflict resolution
- **Safety guarantees** - majority voting, log matching, commit rules
- **Async runtime** - tokio-based event loop with proper timer management
- **In-memory network** - message routing for testing cluster behavior

## Architecture

The implementation uses an action/effect pattern: event handlers return `Vec<Action>` describing state changes, which are then executed. This separation makes the protocol logic testable independent of side effects.

Core types map directly to the paper's specification: `PersistentState`, `VolatileState`, `LeaderState`, with RPC messages matching Figure 2.

## What's Missing

This is a learning implementation, not production-ready:
- No persistence (state lives in memory only)
- No client request handling
- No log compaction/snapshotting
- No dynamic membership changes
- Network is simulated via channels, not real I/O

## Running
```bash
cargo run
```

Creates a 3-node cluster. You should see one node win election within ~200-300ms.

## Learning Notes

Built this to understand distributed consensus from first principles. Used Claude as a guide for structure and debugging, but implemented the protocol logic by working through the paper's state machine descriptions and RPC handlers.
