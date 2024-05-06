use std::collections::BTreeMap;
use management_interface::Blob;
use tokio::io::AsyncReadExt;

use management_interface::IncomingInterface;
use management_interface::IncomingInterfaceId;
use management_interface::OutgoingInterfaceId;
use management_interface::ServiceId;
use management_interface::{ClusterConfiguration, Service, WasiProcess};

async fn read_blob(from: &std::path::Path) -> Blob {
    let mut file = tokio::fs::File::open(from)
        .await
        .expect(&format!("Tried to open {}", from.display()));
    let mut contents = vec![];
    file.read_to_end(&mut contents).await.unwrap();
    Blob::Direct(contents)
}

pub async fn compile_cluster_configuration(target: &std::path::Path) -> ClusterConfiguration {
    let hello_world_id = ServiceId(0);
    let essrpc_server_id = ServiceId(1);
    let essrpc_client_id = ServiceId(2);
    let provide_api_id = ServiceId(3);
    let call_api_id = ServiceId(4);
    let database_server_id = ServiceId(5);
    let database_client_id = ServiceId(6);
    let idle_service_id = ServiceId(7);

    ClusterConfiguration {
        services: vec![
            Service {
                id: hello_world_id,
                outgoing_interfaces: BTreeMap::new(),
                wasi: WasiProcess {
                    code: read_blob(&target.join("wasm32-wasi/release/hello_rust.wasm")).await,
                    has_threads: false,
                },
                filesystem_dir_unique_id: None,
            },
            Service {
                id: essrpc_server_id,
                outgoing_interfaces: BTreeMap::new(),
                wasi: WasiProcess {
                    code: read_blob(
                        &target.join("wasm32-wasip1-threads/release/essrpc_server.wasm"),
                    )
                    .await,
                    has_threads: true,
                },
                filesystem_dir_unique_id: None,
            },
            Service {
                id: essrpc_client_id,
                outgoing_interfaces: BTreeMap::from([(
                    OutgoingInterfaceId(0),
                    IncomingInterface::new(essrpc_server_id, IncomingInterfaceId(0)),
                )]),
                wasi: WasiProcess {
                    code: read_blob(&target.join("wasm32-wasi/release/essrpc_client.wasm")).await,
                    has_threads: false,
                },
                filesystem_dir_unique_id: None,
            },
            Service {
                id: provide_api_id,
                outgoing_interfaces: BTreeMap::new(),
                wasi: WasiProcess {
                    code: read_blob(&target.join("wasm32-wasi/release/provide_api.wasm")).await,
                    has_threads: false,
                },
                filesystem_dir_unique_id: None,
            },
            Service {
                id: call_api_id,
                outgoing_interfaces: BTreeMap::from([(
                    OutgoingInterfaceId(0),
                    IncomingInterface::new(provide_api_id, IncomingInterfaceId(0)),
                )]),
                wasi: WasiProcess {
                    code: read_blob(&target.join("wasm32-wasi/release/call_api.wasm")).await,
                    has_threads: false,
                },
                filesystem_dir_unique_id: None,
            },
            Service {
                id: database_server_id,
                outgoing_interfaces: BTreeMap::new(),
                wasi: WasiProcess {
                    code: read_blob(
                        &target.join("wasm32-wasip1-threads/release/database_server.wasm"),
                    )
                    .await,
                    has_threads: true,
                },
                filesystem_dir_unique_id: Some("example_database_server".to_string()),
            },
            Service {
                id: database_client_id,
                outgoing_interfaces: BTreeMap::from([(
                    OutgoingInterfaceId(0),
                    IncomingInterface::new(database_server_id, IncomingInterfaceId(0)),
                )]),
                wasi: WasiProcess {
                    code: read_blob(&target.join("wasm32-wasi/release/database_client.wasm")).await,
                    has_threads: false,
                },
                filesystem_dir_unique_id: None,
            },
            Service {
                id: idle_service_id,
                outgoing_interfaces: BTreeMap::new(),
                wasi: WasiProcess {
                    code: read_blob(&target.join("wasm32-wasi/release/idle_service.wasm")).await,
                    has_threads: false,
                },
                filesystem_dir_unique_id: None,
            },
        ],
    }
}