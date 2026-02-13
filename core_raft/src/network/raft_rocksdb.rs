use crate::network::model::{Request, Response};
use crate::network::network::NetworkFactory;
use crate::network::node::{App, CacheCatApp, NodeId, StateMachineStore, create_node};
use crate::server::handler::rpc;
use crate::store::rocks_store::new_storage;
use openraft::{BasicNode, Config};
use std::collections::BTreeMap;

use crate::store::rocks_log_store::RocksLogStore;
use std::path::Path;
use std::sync::Arc;
use rocksdb::DB;
use crate::server::core::config::{ONE, THREE, TWO};
use crate::store::raft_engine::create_raft_engine;

pub async fn start_raft_app<P>(node_id: NodeId, dir: P, addr: String) -> std::io::Result<()>
where
    P: AsRef<Path>,
{
    let config = Arc::new(Config {
        heartbeat_interval: 2500,
        election_timeout_min: 2990,
        election_timeout_max: 5990, // 添加最大选举超时时间
        ..Default::default()
    });
    let raft_engine = dir.as_ref().join("raft-engine");
    let rocksdb_path = dir.as_ref().join("rocksdb");
    let engine = create_raft_engine(raft_engine.clone());
    let db: Arc<DB> = new_storage(rocksdb_path).await;
    let log_store = RocksLogStore::new( 0,engine.clone());
    let sm_store = StateMachineStore::new(db.clone(), 0).await.unwrap();
    let network = NetworkFactory {};

    let raft = openraft::Raft::new(
        node_id,
        config.clone(),
        network,
        log_store,
        sm_store.clone(),
    )
    .await
    .unwrap();

    let app = CacheCatApp {
        id: node_id,
        addr: addr.clone(),
        raft,
        group_id: 0,
        state_machine: sm_store,
    };

    // 正确构建集群成员映射
    let mut nodes = BTreeMap::new();
    if node_id == 3 {
        nodes.insert(
            1,
            BasicNode {
                addr: ONE.to_string(),
            },
        );
        nodes.insert(
            2,
            BasicNode {
                addr: TWO.to_string(),
            },
        );
        nodes.insert(
            3,
            BasicNode {
                addr: THREE.to_string(),
            },
        );
        app.raft.initialize(nodes).await.unwrap();
    }
    // 根据node_id决定完整的集群配置

    rpc::start_server(App::new(vec![Box::new(app)]), addr).await
}
pub async fn start_multi_raft_app<P>(node_id: NodeId, dir: P, addr: String) -> std::io::Result<()>
where
    P: AsRef<Path>,
{
    let node = create_node(&addr, node_id, dir).await;
    let apps: Vec<Box<CacheCatApp>> = node.groups.into_values().map(Box::new).collect();
    let mut nodes = BTreeMap::new();
    if node_id == 3 {
        nodes.insert(
            1,
            BasicNode {
                addr: ONE.to_string(),
            },
        );
        nodes.insert(
            2,
            BasicNode {
                addr: TWO.to_string(),
            },
        );
        nodes.insert(
            3,
            BasicNode {
                addr: THREE.to_string(),
            },
        );
        for app in &apps {
            app.raft.initialize(nodes.clone()).await.unwrap();
        }
    }

    rpc::start_server(App::new(apps), addr).await
}
