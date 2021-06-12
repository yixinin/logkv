use crate::kv::KvEngine;
use async_std::channel::{Receiver, Sender};
use protobuf::Message as PbMessage;
use raft::storage::MemStorage;
use raft::{prelude::*, StateRole};
use std::collections::HashMap;

use slog::{error, info, o};

pub struct Node {
    raft_group: Option<RawNode<MemStorage>>,
    my_mailbox: Receiver<Message>,
    mailboxes: HashMap<String, Sender<Message>>,
    kv_pairs: KvEngine,
}

fn example_config() -> Config {
    Config {
        election_tick: 10,
        heartbeat_tick: 3,
        ..Default::default()
    }
}

fn is_initial_msg(msg: &Message) -> bool {
    let msg_type = msg.get_msg_type();
    msg_type == MessageType::MsgRequestVote
        || msg_type == MessageType::MsgRequestPreVote
        || (msg_type == MessageType::MsgHeartbeat && msg.commit == 0)
}

impl Node {
    // Create a raft leader only with itself in its configuration.
    pub fn create_raft_leader(
        id: u64,
        my_mailbox: Receiver<Message>,
        mailboxes: HashMap<String, Sender<Message>>,
        logger: &slog::Logger,
        name: String,
    ) -> Self {
        let mut cfg = example_config();
        cfg.id = id;
        let logger = logger.new(o!("tag" => format!("peer_{}", id)));
        let mut s = Snapshot::default();
        // Because we don't use the same configuration to initialize every node, so we use
        // a non-zero index to force new followers catch up logs by snapshot first, which will
        // bring all nodes to the same initial state.
        s.mut_metadata().index = 1;
        s.mut_metadata().term = 1;
        s.mut_metadata().mut_conf_state().voters = vec![1];
        let storage = MemStorage::new();
        storage.wl().apply_snapshot(s).unwrap();
        let raft_group = Some(RawNode::new(&cfg, storage, &logger).unwrap());
        Node {
            raft_group,
            my_mailbox,
            mailboxes, 
            kv_pairs: KvEngine::new(name),
        }
    }

    // Create a raft follower.
    pub fn create_raft_follower(
        my_mailbox: Receiver<Message>,
        mailboxes: HashMap<String, Sender<Message>>,
        name: String,
    ) -> Self {
        Node {
            raft_group: None,
            my_mailbox,
            mailboxes,
            kv_pairs: KvEngine::new(name),
        }
    }

    // Initialize raft for followers.
    fn initialize_raft_from_message(&mut self, msg: &Message, logger: &slog::Logger) {
        if !is_initial_msg(msg) {
            return;
        }
        let mut cfg = example_config();
        cfg.id = msg.to;
        let logger = logger.new(o!("tag" => format!("peer_{}", msg.to)));
        let storage = MemStorage::new();
        self.raft_group = Some(RawNode::new(&cfg, storage, &logger).unwrap());
    }

    // Step a raft message, initialize the raft if need.
    fn step(&mut self, msg: Message, logger: &slog::Logger) {
        if self.raft_group.is_none() {
            if is_initial_msg(&msg) {
                self.initialize_raft_from_message(&msg, &logger);
            } else {
                return;
            }
        }
        let raft_group = self.raft_group.as_mut().unwrap();
        let _ = raft_group.step(msg);
    }
}
