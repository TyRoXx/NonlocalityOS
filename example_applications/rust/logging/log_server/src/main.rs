use essrpc::transports::BincodeTransport;
use essrpc::RPCError;
use essrpc::RPCServer;
use log_trait::LogLevel;
use log_trait::Logger;
use log_trait::LoggerRPCServer;
use nonlocality_env::accept;

struct ConsoleLogger {}

impl Logger for ConsoleLogger {
    fn log(&self, level: LogLevel, message: String) -> Result<(), RPCError> {
        println!("[{}] {}", level, message);
        return Ok(());
    }
}

fn main() {
    loop {
        let accepted = accept();
        let _ = std::thread::spawn(move || {
            println!("Logger ready on interface {}.", accepted.interface);
            let mut server =
                LoggerRPCServer::new(ConsoleLogger {}, BincodeTransport::new(accepted.stream));
            match server.serve() {
                Ok(()) => {
                    println!("Serve completed successfully.");
                }
                Err(error) => {
                    println!("Serve failed: {}.", error);
                }
            }
        });
    }
}
