#[deny(warnings)]
use essrpc::essrpc;
use essrpc::RPCError;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, PartialEq, PartialOrd, Ord, Eq, Clone, Copy)]
pub struct BlobDigest(pub ([u8; 32], [u8; 32]));

#[derive(Serialize, Deserialize, Debug, PartialEq, PartialOrd, Ord, Eq, Clone)]
pub enum Blob {
    Digest(BlobDigest),
    Direct(Vec<u8>),
}

#[derive(Serialize, Deserialize, Debug, PartialEq, PartialOrd, Ord, Eq, Clone, Copy)]
pub struct IncomingInterfaceId(pub i32);

#[derive(Serialize, Deserialize, Debug, PartialEq, PartialOrd, Ord, Eq, Clone, Copy)]
pub struct OutgoingInterfaceId(pub i32);

#[derive(Serialize, Deserialize, Debug, PartialEq, PartialOrd, Ord, Eq, Clone, Copy)]
pub struct ServiceId(pub i32);

#[derive(Serialize, Deserialize, Debug, PartialEq, PartialOrd, Ord, Eq, Clone)]
pub struct WasiProcess {
    pub code: Blob,
    pub has_threads: bool,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, PartialOrd, Ord, Eq, Clone, Copy)]
pub struct IncomingInterface {
    pub destination_service: ServiceId,
    pub interface: IncomingInterfaceId,
}

impl IncomingInterface {
    pub fn new(
        destination_service: ServiceId,
        interface: IncomingInterfaceId,
    ) -> IncomingInterface {
        IncomingInterface {
            destination_service,
            interface,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq, PartialOrd, Ord, Eq, Clone)]
pub struct Service {
    pub id: ServiceId,
    pub label: String,
    pub outgoing_interfaces: std::collections::BTreeMap<OutgoingInterfaceId, IncomingInterface>,
    pub wasi: WasiProcess,
    pub filesystem_dir_unique_id: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, PartialOrd, Ord, Eq, Clone)]
pub struct ClusterConfiguration {
    pub services: Vec<Service>,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, PartialOrd, Ord, Eq, Clone, Copy)]
pub enum ConfigurationError {
    InternalError,
}

#[essrpc]
pub trait ManagementInterface {
    fn shutdown(&self) -> Result<bool, RPCError>;
    fn reconfigure(
        &self,
        configuration: ClusterConfiguration,
    ) -> Result<Option<ConfigurationError>, RPCError>;
}
