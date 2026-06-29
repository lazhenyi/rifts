//! TCP mesh cluster integration test.
//!
//! Spawns two `ClusterBroker` instances with different listen
//! addresses, connects them via seed-node discovery, and verifies
//! that publishes on one node reach subscribers on the other.
//!
//! ```sh
//! cargo test --features cluster -- --ignored --test-threads=1
//! ```

#[cfg(feature = "cluster")]
#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use rifts::broker::InMemoryBroker;
    use rifts::broker::broker::Broker;
    use rifts::broker::fanout::SubscribeIntent;
    use rifts::broker::fanout::test_sink::CountingSink;
    use rifts::cluster::broker::ClusterBroker;
    use rifts::cluster::config::ClusterConfig;
    use rifts::frame::{Codec, Frame, FrameFlags, FrameType};
    use rifts::topic::TopicProfile;

    fn make_frame(topic: &str, msg_id: &str, payload: &[u8]) -> Frame {
        Frame {
            version: 0x0100,
            frame_id: 1,
            frame_type: FrameType::Data,
            flags: FrameFlags::empty(),
            codec: Codec::Json,
            session_id: Some("s-1".into()),
            stream_id: None,
            topic: Some(topic.into()),
            event: Some("test.event".into()),
            message_id: Some(msg_id.into()),
            correlation_id: None,
            trace_id: None,
            timestamp: 0,
            ttl_ms: None,
            priority: None,
            payload: Some(bytes::Bytes::copy_from_slice(payload)),
        }
    }

    fn local_broker() -> Arc<dyn Broker> {
        Arc::new(InMemoryBroker::new(
            TopicProfile::default(),
            Duration::from_secs(60),
            65536,
        ))
    }

    #[tokio::test]
    #[ignore = "requires TCP ports 19101-19102 free"]
    async fn two_node_cluster_basic_publish() {
        // Node A on port 19101, seeds = [B].
        let config_a = ClusterConfig {
            listen_addr: "127.0.0.1:19101".parse().unwrap(),
            seed_nodes: vec!["127.0.0.1:19102".into()],
            gossip_interval: Duration::from_millis(100),
            gossip_fanout: 3,
            ping_timeout: Duration::from_millis(500),
            suspect_timeout: Duration::from_secs(2),
            dead_timeout: Duration::from_secs(10),
            max_reconnect_attempts: 10,
            reconnect_base_ms: 100,
            reconnect_max_ms: 5_000,
        };
        let broker_a = ClusterBroker::start(config_a, local_broker())
            .await
            .unwrap();

        // Node B on port 19102, seeds = [A].
        let config_b = ClusterConfig {
            listen_addr: "127.0.0.1:19102".parse().unwrap(),
            seed_nodes: vec!["127.0.0.1:19101".into()],
            gossip_interval: Duration::from_millis(100),
            gossip_fanout: 3,
            ping_timeout: Duration::from_millis(500),
            suspect_timeout: Duration::from_secs(2),
            dead_timeout: Duration::from_secs(10),
            max_reconnect_attempts: 10,
            reconnect_base_ms: 100,
            reconnect_max_ms: 5_000,
        };
        let broker_b = ClusterBroker::start(config_b, local_broker())
            .await
            .unwrap();

        // Subscribe on B.
        let sink_b = Arc::new(CountingSink::new(1));
        broker_b
            .subscribe("chat", SubscribeIntent::Live, sink_b.clone())
            .await
            .unwrap();

        // Give the cluster time to discover and connect.
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Publish on A.
        broker_a
            .publish(&make_frame("chat", "m1", b"hello"))
            .await
            .unwrap();

        // Wait for cross-node delivery via TCP mesh.
        tokio::time::sleep(Duration::from_millis(500)).await;

        // B's subscriber should have received the message.
        assert_eq!(
            sink_b.count(),
            1,
            "cross-node fanout should deliver to B's subscriber"
        );
    }
}
