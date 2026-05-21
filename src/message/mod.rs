//! Message types — spec §8.

pub mod command;
pub mod datagram;
pub mod event;
pub mod snapshot;
pub mod state;
pub mod stream;

use serde::{Deserialize, Serialize};

/// Top-level message class (spec §8.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MessageClass {
    Event,
    Command,
    Reply,
    State,
    Datagram,
    Stream,
    Snapshot,
    System,
}

impl MessageClass {
    pub fn as_str(self) -> &'static str {
        match self {
            MessageClass::Event => "event",
            MessageClass::Command => "command",
            MessageClass::Reply => "reply",
            MessageClass::State => "state",
            MessageClass::Datagram => "datagram",
            MessageClass::Stream => "stream",
            MessageClass::Snapshot => "snapshot",
            MessageClass::System => "system",
        }
    }
}

/// Delivery semantics (spec §8.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryMode {
    AtMostOnce,
    AtLeastOnce,
    ExactlyOnceEffect,
    LatestOnly,
    BestEffort,
    DurableOrdered,
}

impl DeliveryMode {
    /// Default delivery mode for a message class (spec §8.2).
    pub fn default_for(class: MessageClass) -> Self {
        match class {
            MessageClass::Event => DeliveryMode::AtLeastOnce,
            MessageClass::Command => DeliveryMode::AtLeastOnce,
            MessageClass::Reply => DeliveryMode::AtLeastOnce,
            MessageClass::State => DeliveryMode::LatestOnly,
            MessageClass::Datagram => DeliveryMode::BestEffort,
            MessageClass::Stream => DeliveryMode::DurableOrdered,
            MessageClass::Snapshot => DeliveryMode::AtLeastOnce,
            MessageClass::System => DeliveryMode::AtLeastOnce,
        }
    }
}

/// Subscription mode (spec §10.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SubscribeMode {
    Live,
    Replay,
    SnapshotThenLive,
    Latest,
    Passive,
    Ephemeral,
}

/// Subscribe acknowledgement result (spec §10.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SubscribeResult {
    Accepted,
    Denied,
    NotFound,
    Gone,
    ReplayRequired,
    SnapshotRequired,
    RateLimited,
    Overloaded,
    InvalidFilter,
}

/// A typed message exchanged with the broker.
///
/// Most variants wrap the on-wire structs from each submodule; the
/// `Raw` variant is the escape hatch used when we don't have a typed
/// representation (e.g. a state message that is purely a JSON value).
#[derive(Debug, Clone)]
pub enum Message {
    Event(event::Event),
    Command(command::Command),
    Reply(command::Reply),
    State(state::State),
    Datagram(datagram::Datagram),
    Stream(stream::StreamSegment),
    Snapshot(snapshot::Snapshot),
    /// Generic system message keyed by an event name.
    System {
        event: String,
        payload: serde_json::Value,
    },
}

impl Message {
    pub fn class(&self) -> MessageClass {
        match self {
            Message::Event(_) => MessageClass::Event,
            Message::Command(_) => MessageClass::Command,
            Message::Reply(_) => MessageClass::Reply,
            Message::State(_) => MessageClass::State,
            Message::Datagram(_) => MessageClass::Datagram,
            Message::Stream(_) => MessageClass::Stream,
            Message::Snapshot(_) => MessageClass::Snapshot,
            Message::System { .. } => MessageClass::System,
        }
    }
}
