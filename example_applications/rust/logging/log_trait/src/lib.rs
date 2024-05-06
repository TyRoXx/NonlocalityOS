use essrpc::essrpc;
use essrpc::RPCError;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub enum LogLevel {
    Unknown = 0,
    Panic = 1,
    Error = 2,
    Warning = 3,
    Info = 4,
    Debug = 5,
}

impl std::fmt::Display for LogLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        return write!(
            f,
            "{}",
            match self {
                LogLevel::Unknown => "UNKNOWN",
                LogLevel::Panic => "PANIC",
                LogLevel::Error => "ERROR",
                LogLevel::Warning => "WARNING",
                LogLevel::Info => "INFO",
                LogLevel::Debug => "DEBUG",
            }
        );
    }
}

#[essrpc]
pub trait Logger {
    fn log(&self, level: LogLevel, message: String) -> Result<(), RPCError>;
}
