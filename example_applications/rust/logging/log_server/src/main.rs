#[deny(warnings)]
use essrpc::transports::BincodeTransport;
use essrpc::RPCError;
use essrpc::RPCServer;
use log_trait::LogLevel;
use log_trait::Logger;
use log_trait::LoggerRPCServer;
use nonlocality_env::accept;
use std::fs::OpenOptions;
use std::io::Seek;
use std::io::Write;
use std::sync::Arc;
use std::sync::Mutex;

struct FileLogger {
    pub handle: Arc<Mutex<std::fs::File>>,
}

impl Logger for FileLogger {
    fn log(&self, level: LogLevel, message: String) -> Result<(), RPCError> {
        println!("Received log message: {} {}", level, message);
        self.handle
            .lock()
            .unwrap()
            .write_all(format!("[{}] {}\n", level, message).as_bytes())?;
        return Ok(());
    }
}

fn main() -> std::io::Result<()> {
    println!("Starting logger server");
    let path = std::path::Path::new("/foo.txt");
    let mut file_handle = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)
        .expect(format!("Unable to open file at {}", path.display()).as_str());
    file_handle
        .seek(std::io::SeekFrom::End(0))
        .expect("Tried to seek");

    let handle = Arc::new(Mutex::new(file_handle));

    loop {
        let accepted = accept()?;
        let handle_clone = handle.clone();
        let _ = std::thread::spawn(move || {
            println!("Logger ready on interface {}.", accepted.interface);
            let mut server = LoggerRPCServer::new(
                FileLogger {
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
