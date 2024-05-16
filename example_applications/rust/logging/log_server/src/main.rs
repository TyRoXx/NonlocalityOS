#[deny(warnings)]
use essrpc::transports::BincodeTransport;
use essrpc::RPCError;
use essrpc::RPCServer;
use log_trait::LogLevel;
use log_trait::Logger;
use log_trait::LoggerRPCServer;
use nonlocality_env::accept;
use sqlite::State;
use std::sync::Arc;
use std::sync::Mutex;

fn create_database_connection(path: &std::path::Path) -> sqlite::Connection {
    let connection = sqlite::open(path).expect("Tried to create database connection");
    let query = "CREATE TABLE IF NOT EXISTS log_entries (log_level TEXT, message TEXT)";
    connection.execute(query).expect("Tried to create table");

    return connection;
}

struct DatabaseLogger {
    pub handle: Arc<Mutex<sqlite::Connection>>,
}

impl Logger for DatabaseLogger {
    fn log(&self, level: LogLevel, message: String) -> Result<(), RPCError> {
        println!("Received log message: {} {}", level, message);
        let connection = self
            .handle
            .lock()
            .expect("Unable to get a lock on the database connection.");

        let query = "INSERT INTO log_entries (log_level, message) VALUES (?, ?)";
        let mut statement = connection
            .prepare(query)
            .expect("Tried to prepare statement");
        statement
            .bind((1, &format!("{}", level)[..]))
            .expect("Tried to bind parameter to statement");
        statement
            .bind((2, &message[..]))
            .expect("Tried to bind parameter to statement");

        loop {
            match statement.next().expect("Tried to execute statement") {
                sqlite::State::Row => {
                    println!("INSERT should not return a row")
                }
                sqlite::State::Done => break,
            }
        }
        return Ok(());
    }

    fn show_logs(&self) -> Result<Vec<String>, RPCError> {
        let query = "SELECT message FROM log_entries";
        let connection = self.handle.lock().unwrap();
        let mut statement = connection.prepare(query).unwrap();
        let mut messages: Vec<String> = Vec::new();
        while let Ok(State::Row) = statement.next() {
            messages.push(statement.read::<String, _>("message").unwrap());
        }

        return Ok(messages);
    }
}

#[test]
fn test_log() {
    let connection = create_database_connection(std::path::Path::new(":memory:"));
    let logger = DatabaseLogger {
        handle: Arc::new(Mutex::new(connection)),
    };

    logger.log(LogLevel::Info, "I can log".to_string()).unwrap();
}

fn main() -> std::io::Result<()> {
    println!("Starting logger server");
    let handle = Arc::new(Mutex::new(create_database_connection(
        std::path::Path::new("/database.sqlite"),
    )));

    loop {
        let accepted = accept()?;
        let handle_clone = handle.clone();
        let _ = std::thread::spawn(move || {
            println!("Logger ready on interface {}.", accepted.interface);
            let mut server = LoggerRPCServer::new(
                DatabaseLogger {
                    handle: handle_clone,
                },
                BincodeTransport::new(accepted.stream),
            );
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
