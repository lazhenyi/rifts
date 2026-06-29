//! TCP mesh cluster example.
//!
//! Starts a single cluster node that uses `ClusterBroker` for
//! cross-node message routing. Run two instances on different
//! ports to see cross-node publish/subscribe in action.
//!
//! ```sh
//! # Terminal 1:
//! RIFTS_LISTEN=127.0.0.1:19101 cargo run --example cluster_node --features cluster
//! # Terminal 2:
//! RIFTS_LISTEN=127.0.0.1:19102 RIFTS_SEEDS=127.0.0.1:19101 cargo run --example cluster_node --features cluster
//! ```

use std::sync::Arc;
use std::time::Duration;

use rifts::broker::InMemoryBroker;
use rifts::cluster::broker::ClusterBroker;
use rifts::cluster::config::ClusterConfig;
use rifts::topic::TopicProfile;

#[tokio::main]
async fn main() -> rifts::Result<()> {
    let listen: std::net::SocketAddr = std::env::var("RIFTS_LISTEN")
        .unwrap_or_else(|_| "127.0.0.1:19101".into())
        .parse()
        .expect("RIFTS_LISTEN must be a valid socket addr");

    let seeds: Vec<String> = std::env::var("RIFTS_SEEDS")
        .ok()
        .map(|s| s.split(',').map(String::from).collect())
        .unwrap_or_default();

    println!("=== Cluster Node ===");
    println!("Listen: {listen}");
    println!("Seeds:  {seeds:?}");

    let config = ClusterConfig {
        listen_addr: listen,
        seed_nodes: seeds,
        ..ClusterConfig::default()
    };

    let local_broker: Arc<dyn rifts::broker::Broker> = Arc::new(InMemoryBroker::new(
        TopicProfile::default(),
        Duration::from_secs(60),
        65536,
    ));

    let cluster = ClusterBroker::start(config, local_broker).await?;

    println!("Cluster broker started. Press Ctrl-C to shut down.");

    // Keep the node alive.
    tokio::signal::ctrl_c().await.ok();
    cluster.shutdown().await;
    println!("Shutting down.");
    Ok(())
}
