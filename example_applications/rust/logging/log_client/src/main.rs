#[deny(warnings)]
use essrpc::transports::BincodeTransport;
use essrpc::RPCClient;
use log_trait::LogLevel;
use log_trait::Logger;
use log_trait::LoggerRPCClient;
use nonlocality_env::connect;

fn main() {
    let connected = connect(0);
    let client = LoggerRPCClient::new(BincodeTransport::new(connected));
    println!("Logger client initialized");

    client
        .log(LogLevel::Info, "I can use the logging service".to_string())
        .expect("Could not log statement.");

    let messages = client
        .show_logs()
        .expect("Failed to get logs from the log server.");

    println!("=== Log {} message(s) ===", messages.len());

    for message in messages {
        println!("{}", message);
    }

    println!("Logger client is done");
}
