#![deny(warnings)]
use anyhow::bail;
use display_bytes::display_bytes;
use essrpc::transports::BincodeTransport;
use essrpc::RPCError;
use essrpc::RPCServer;
use management_interface::ManagementInterface;
use management_interface::ManagementInterfaceRPCServer;
use normalize_path::NormalizePath;
use os_pipe::{pipe, PipeReader, PipeWriter};
use promising_future::{future_promise, Promise};
use relative_path::RelativePathBuf;
use std::any::Any;
use std::collections::BTreeMap;
use std::collections::VecDeque;
use std::env;
use std::fmt;
use std::io::Read;
use std::io::Write;
use std::path::Path;
use std::process::ExitCode;
use std::sync::{Arc, Mutex};
use std::thread;
use wasi_common::file::{FileAccessMode, FileType};
use wasi_common::pipe::WritePipe;
use wasi_common::sync::WasiCtxBuilder;
use wasi_common::ErrorExt;
use wasi_common::{WasiCtx, WasiFile};
use wasmtime::Config;
use wasmtime::{Caller, Engine, Linker, Module, Store};
use wasmtime_wasi_threads::WasiThreadsCtx;

#[derive(Debug, PartialEq, PartialOrd, Ord, Eq, Clone, Copy)]
struct IncomingInterfaceId(i32);

#[derive(Debug, PartialEq, PartialOrd, Ord, Eq, Clone, Copy)]
struct OutgoingInterfaceId(i32);

#[derive(Debug, PartialEq, PartialOrd, Ord, Eq, Clone, Copy)]
struct ServiceId(i32);

// Some sources will tell you that #[derive(Display)] exists, but that is a lie.
impl fmt::Display for ServiceId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

struct WasiProcess {
    web_assembly_file: RelativePathBuf,
    has_threads: bool,
    id: ServiceId,
    interfaces: BTreeMap<OutgoingInterfaceId, (ServiceId, IncomingInterfaceId)>,
}

struct Order {
    wasi_processes: Vec<WasiProcess>,
}

struct Logger {
    name: String,
}

impl std::io::Write for Logger {
    fn write(&mut self, buf: &[u8]) -> std::result::Result<usize, std::io::Error> {
        let displayed_part = match buf {
            [head @ .., b'\n'] => head,
            _ => buf,
        };
        println!("{}: {}", self.name, display_bytes(displayed_part));
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::result::Result<(), std::io::Error> {
        Ok(())
    }
}

struct InterServiceApiStream {
    writer: Mutex<PipeWriter>,
    reader: Mutex<PipeReader>,
}

#[wiggle::async_trait]
impl WasiFile for InterServiceApiStream {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn get_filetype(&self) -> Result<FileType, wasi_common::Error> {
        Ok(FileType::Unknown)
    }

    async fn write_vectored<'a>(
        &self,
        _bufs: &[std::io::IoSlice<'a>],
    ) -> Result<u64, wasi_common::Error> {
        let mut writer = match self.writer.lock() {
            Ok(result) => result,
            Err(error) => {
                println!("Could not lock the pipe writer: {}.", error);
                return Err(wasi_common::Error::not_supported());
            }
        };
        match writer.write_vectored(_bufs) {
            Ok(written) => {
                println!("Wrote {} bytes to the pipe.", written);
                Ok(written as u64)
            }
            Err(error) => Err(wasi_common::Error::from(error)),
        }
    }

    async fn read_vectored<'a>(
        &self,
        _bufs: &mut [std::io::IoSliceMut<'a>],
    ) -> Result<u64, wasi_common::Error> {
        let mut reader = match self.reader.lock() {
            Ok(result) => result,
            Err(error) => {
                println!("Could not lock the pipe reader: {}.", error);
                return Err(wasi_common::Error::not_supported());
            }
        };
        match reader.read_vectored(_bufs) {
            Ok(read) => {
                println!("Read {} bytes from the pipe.", read);
                Ok(read as u64)
            }
            Err(error) => Err(wasi_common::Error::from(error)),
        }
    }
}

enum InterServiceApiError {
    OnlyOneAcceptorSupportedAtTheMoment,
    UnknownInternalError,
    CouldNotCreatePipe,
}

impl fmt::Display for InterServiceApiError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}",
            match self {
                InterServiceApiError::OnlyOneAcceptorSupportedAtTheMoment =>
                    "only one acceptor supported at the moment",
                InterServiceApiError::UnknownInternalError => "unknown internal error",
                InterServiceApiError::CouldNotCreatePipe => "could not create an OS pipe",
            }
        )
    }
}

fn create_pair_of_streams(
) -> std::result::Result<(InterServiceApiStream, InterServiceApiStream), InterServiceApiError> {
    let upload = match pipe() {
        Ok(result) => result,
        Err(error) => {
            println!("Creating an OS pipe failed with {}.", error);
            return Err(InterServiceApiError::CouldNotCreatePipe);
        }
    };
    let download = match pipe() {
        Ok(result) => result,
        Err(error) => {
            println!("Creating an OS pipe failed with {}.", error);
            return Err(InterServiceApiError::CouldNotCreatePipe);
        }
    };
    let server_side = InterServiceApiStream {
        writer: Mutex::new(download.1),
        reader: Mutex::new(upload.0),
    };
    let client_side = InterServiceApiStream {
        writer: Mutex::new(upload.1),
        reader: Mutex::new(download.0),
    };
    return Ok((server_side, client_side));
}

struct AcceptResult {
    interface: IncomingInterfaceId,
    stream: InterServiceApiStream,
}

enum HubQueue {
    Accepting(Option<Promise<AcceptResult>>),
    Connecting(VecDeque<(IncomingInterfaceId, Promise<InterServiceApiStream>)>),
}

struct InterServiceApiHub {
    queue: Mutex<std::collections::BTreeMap<ServiceId, HubQueue>>,
}

impl InterServiceApiHub {
    pub fn new() -> InterServiceApiHub {
        InterServiceApiHub {
            queue: Mutex::new(std::collections::BTreeMap::new()),
        }
    }

    pub fn accept(
        &self,
        accepting_service: ServiceId,
    ) -> std::result::Result<AcceptResult, InterServiceApiError> {
        let mut locked = self.queue.lock().unwrap();
        let queue = locked
            .entry(accepting_service)
            .or_insert_with(|| HubQueue::Connecting(VecDeque::new()));
        match *queue {
            HubQueue::Accepting(_) => {
                Err(InterServiceApiError::OnlyOneAcceptorSupportedAtTheMoment)
            }
            HubQueue::Connecting(ref mut waiting) => match waiting.pop_front() {
                Some(next_in_line) => {
                    let (server_side, client_side) = create_pair_of_streams()?;
                    next_in_line.1.set(client_side);
                    Ok(AcceptResult {
                        interface: next_in_line.0,
                        stream: server_side,
                    })
                }
                None => {
                    let (future, promise) = future_promise();
                    *queue = HubQueue::Accepting(Some(promise));
                    drop(locked);
                    match future.value() {
                        Some(accept_result) => Ok(accept_result),
                        None => Err(InterServiceApiError::UnknownInternalError),
                    }
                }
            },
        }
    }

    pub fn connect(
        &self,
        destination_service: ServiceId,
        interface: IncomingInterfaceId,
    ) -> std::result::Result<InterServiceApiStream, InterServiceApiError> {
        let mut locked = self.queue.lock().unwrap();
        let queue = locked
            .entry(destination_service)
            .or_insert_with(|| HubQueue::Connecting(VecDeque::new()));
        match *queue {
            HubQueue::Accepting(ref mut acceptor) => {
                let (server_side, client_side) = create_pair_of_streams()?;
                let acceptor2: Promise<AcceptResult> = match acceptor.take() {
                    Some(content) => content,
                    None => panic!(),
                };
                acceptor2.set(AcceptResult {
                    interface: interface,
                    stream: server_side,
                });
                Ok(client_side)
            }
            HubQueue::Connecting(ref mut waiting) => {
                let (future, promise) = future_promise();
                waiting.push_back((interface, promise));
                drop(locked);
                match future.value() {
                    Some(stream) => Ok(stream),
                    None => Err(InterServiceApiError::UnknownInternalError),
                }
            }
        }
    }
}

#[derive(Clone)]
struct InterServiceFuncContext {
    wasi: WasiCtx,
    wasi_threads: Option<Arc<WasiThreadsCtx<InterServiceFuncContext>>>,
    // Somehow it's impossible to reference local variables from wasmtime host functions, so we have to use reference counting for no real reason.
    api_hub: Arc<InterServiceApiHub>,
    this_service_id: ServiceId,
    outgoing_interfaces:
        Arc<std::collections::BTreeMap<OutgoingInterfaceId, (ServiceId, IncomingInterfaceId)>>,
}

// Absolutely ridiculous hack necessary because it is impossible to return multiple values,
// or return things by reference parameter in wasmtime.
fn encode_i32_pair(first: i32, second: i32) -> u64 {
    (((first as u32) as u64) << 32) | ((second as u32) as u64)
}

fn run_wasi_process(
    engine: Engine,
    module: Module,
    logger: Logger,
    api_hub: Arc<InterServiceApiHub>,
    has_threads: bool,
    this_service_id: ServiceId,
    outgoing_interfaces: Arc<
        std::collections::BTreeMap<OutgoingInterfaceId, (ServiceId, IncomingInterfaceId)>,
    >,
) -> wasmtime::Result<()> {
    let mut linker = Linker::new(&engine);
    wasi_common::sync::add_to_linker(&mut linker, |s: &mut InterServiceFuncContext| &mut s.wasi)?;
    let wasi = WasiCtxBuilder::new().build();

    let stdout = WritePipe::new(logger);
    wasi.set_stdout(Box::new(stdout.clone()));

    println!("Defining nonlocality_accept.");
    linker
        .func_wrap(
            "env",
            "nonlocality_accept",
            |caller: Caller<'_, InterServiceFuncContext>| -> u64 {
                println!("nonlocality_accept was called.");
                let context = caller.data();
                let accept_result = match context.api_hub.accept(context.this_service_id) {
                    Ok(success) => success,
                    Err(error) => {
                        println!("nonlocality_accept failed with {}.", error);
                        return encode_i32_pair(i32::max_value(), i32::max_value());
                    }
                };
                let file_descriptor = context
                    .wasi
                    .push_file(Box::new(accept_result.stream), FileAccessMode::all())
                    .unwrap() as i32;
                println!("nonlocality_accept returns FD {}.", file_descriptor);
                encode_i32_pair(accept_result.interface.0, file_descriptor)
            },
        )
        .expect("Tried to define nonlocality_accept");

    println!("Defining nonlocality_connect.");
    linker
        .func_wrap(
            "env",
            "nonlocality_connect",
            |caller: Caller<'_, InterServiceFuncContext>, interface: i32| -> i32 {
                println!(
                    "nonlocality_connect was called for interface {}.",
                    interface
                );
                let context = caller.data();
                let connecting_interface = match context
                    .outgoing_interfaces
                    .get(&OutgoingInterfaceId(interface))
                {
                    Some(found) => found,
                    None => todo!(),
                };
                let stream = match context
                    .api_hub
                    .connect(connecting_interface.0, connecting_interface.1)
                {
                    Ok(stream) => stream,
                    Err(error) => {
                        println!("nonlocality_connect failed with {}.", error);
                        return i32::max_value();
                    }
                };
                let stream_fd = context
                    .wasi
                    .push_file(Box::new(stream), FileAccessMode::all())
                    .unwrap() as i32;
                println!("nonlocality_connect returns FD {}.", stream_fd);
                stream_fd
            },
        )
        .expect("Tried to define nonlocality_connect");

    let mut func_context_store = Store::new(
        &engine,
        InterServiceFuncContext {
            wasi: wasi,
            wasi_threads: None,
            api_hub: api_hub.clone(),
            this_service_id: this_service_id,
            outgoing_interfaces: outgoing_interfaces,
        },
    );

    if has_threads {
        println!("Threads are enabled.");
        wasmtime_wasi_threads::add_to_linker(
            &mut linker,
            &func_context_store,
            &module,
            |s: &mut InterServiceFuncContext| &mut s.wasi_threads.as_ref().unwrap(),
        )
        .expect("Tried to add threads to the linker");
        func_context_store.data_mut().wasi_threads = Some(Arc::new(
            WasiThreadsCtx::new(module.clone(), Arc::new(linker.clone()))
                .expect("Tried to create a context"),
        ));
    } else {
        println!("Threads are not enabled.");
    }

    println!("Setting up the main module or something.");
    linker
        .module(&mut func_context_store, "", &module)
        .expect("Tried to module the main module, whatever that means");

    println!("Calling main function.");
    let entry_point = linker
        .get_default(&mut func_context_store, "")
        .expect("Tried to find the main entry point of the application");
    let typed_entry_point = entry_point
        .typed::<(), ()>(&func_context_store)
        .expect("Tried to cast the main entry point function type");
    match typed_entry_point.call(&mut func_context_store, ()) {
        Ok(_) => {
            println!("Main function returned.");
            Ok(())
        }
        Err(error) => bail!("Service {} failed: {}", this_service_id, error),
    }
}

struct ManagementInterfaceImpl {
    request_shutdown: tokio::sync::mpsc::Sender<()>,
}

impl ManagementInterface for ManagementInterfaceImpl {
    fn shutdown(&self) -> Result<bool, RPCError> {
        println!("Shutdown requested.");
        let handle = tokio::runtime::Handle::current();
        match handle.block_on(self.request_shutdown.send(())) {
            Ok(_) => Ok(true),
            Err(error) => {
                println!("Requesting shutdown failed: {}.", error);
                Ok(false)
            }
        }
    }
}

fn handle_external_requests(
    stream: tokio::net::TcpStream,
    request_shutdown: tokio::sync::mpsc::Sender<()>,
) {
    let sync_stream = tokio_util::io::SyncIoBridge::new(stream);
    let mut server = ManagementInterfaceRPCServer::new(
        ManagementInterfaceImpl { request_shutdown },
        BincodeTransport::new(sync_stream),
    );
    match server.serve() {
        Ok(_) => {}
        Err(error) => {
            println!("External request server failed with {}.", error);
        }
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> ExitCode {
    let args: Vec<String> = env::args().collect();

    let external_port = "127.0.0.1:6969";
    let external_port_listener = match tokio::net::TcpListener::bind(external_port).await {
        Ok(success) => success,
        Err(error) => {
            println!("Could not bind {}: {}", external_port, error);
            return ExitCode::FAILURE;
        }
    };

    let repository = Path::new(&args[1]).normalize();
    let order = Order {
        wasi_processes: vec![
            WasiProcess {
                web_assembly_file: RelativePathBuf::from_path(
                    "example_applications/rust/target/wasm32-wasi/debug/hello_rust.wasm",
                )
                .unwrap(),
                has_threads: false,
               id:   ServiceId(0),
               interfaces: BTreeMap::new(),
            },
            WasiProcess {
                web_assembly_file: RelativePathBuf::from_path(
                    "example_applications/rust/target/wasm32-wasip1-threads/debug/essrpc_server.wasm",
                )
                .unwrap(),
                has_threads: true,
                id:   ServiceId(1),
                interfaces: BTreeMap::new(),
            },
            WasiProcess {
                web_assembly_file: RelativePathBuf::from_path(
                    "example_applications/rust/target/wasm32-wasi/debug/essrpc_client.wasm",
                )
                .unwrap(),
                has_threads: false,
                id:   ServiceId(2),
                interfaces: BTreeMap::from([( OutgoingInterfaceId(0), (ServiceId(1), IncomingInterfaceId(0)))] ),
            },
            WasiProcess {
                web_assembly_file: RelativePathBuf::from_path(
                    "example_applications/rust/target/wasm32-wasi/debug/provide_api.wasm",
                )
                .unwrap(),
                has_threads: false,
                id:   ServiceId(3),
                interfaces: BTreeMap::new(),
            },
            WasiProcess {
                web_assembly_file: RelativePathBuf::from_path(
                    "example_applications/rust/target/wasm32-wasi/debug/call_api.wasm",
                )
                .unwrap(),
                has_threads: false,
                id:   ServiceId(4),
                interfaces: BTreeMap::from([( OutgoingInterfaceId(0), (ServiceId(3), IncomingInterfaceId(0)))] ),
            },
            WasiProcess {
                web_assembly_file: RelativePathBuf::from_path(
                    "example_applications/rust/target/wasm32-wasip1-threads/debug/database_server.wasm",
                )
                .unwrap(),
                has_threads: true,
                id:   ServiceId(5),
                interfaces: BTreeMap::new(),
            },
            WasiProcess {
                web_assembly_file: RelativePathBuf::from_path(
                    "example_applications/rust/target/wasm32-wasi/debug/database_client.wasm",
                )
                .unwrap(),
                has_threads: false,
                id:   ServiceId(6),
                interfaces: BTreeMap::from([( OutgoingInterfaceId(0), (ServiceId(5), IncomingInterfaceId(0)))] ),
            },
            WasiProcess {
                web_assembly_file: RelativePathBuf::from_path(
                    "example_applications/rust/target/wasm32-wasi/debug/idle_service.wasm",
                )
                .unwrap(),
                has_threads: false,
                id: ServiceId(7),
                interfaces: BTreeMap::new(),
            }
        ],
    };

    let (request_shutdown, mut shutdown_requested) = tokio::sync::mpsc::channel::<()>(1);
    let background_acceptor = tokio::spawn(async move {
        loop {
            tokio::select! {
                maybe_accepted = external_port_listener.accept() => match maybe_accepted{
                    Ok(incoming_connection) => {
                        println!(
                            "Accepted external API connection from {}.",
                            incoming_connection.1
                        );
                        let request_shutdown_clone = request_shutdown.clone();
                        tokio::task::spawn_blocking(move || {
                            handle_external_requests(incoming_connection.0, request_shutdown_clone);
                        });
                    }
                    Err(error) => {
                        println!("Accept failed with {}.", error);
                        break;
                    }
                },
                maybe_received = shutdown_requested.recv() => {
                    match maybe_received {
                        Some(_) => {
                            println!("Not accepting external connections anymore.");
                            break;
                        },
                        None => unreachable!("The sender remains on the stack, so the channel will never be closed."),
                    }
                }
            }
        }
    });

    let api_hub = Arc::new(InterServiceApiHub::new());
    let exit_code = thread::scope(|s| {
        let mut threads = Vec::new();
        for wasi_process in order.wasi_processes {
            let input_program_path = wasi_process.web_assembly_file.to_path(&repository);
            println!("Starting thread for {}.", input_program_path.display());
            let api_hub_2 = api_hub.clone();
            let this_service_id = wasi_process.id;
            let interfaces = Arc::new(wasi_process.interfaces.clone());
            let handler = s.spawn(move || {
                let mut config = Config::new();
                config.wasm_threads(wasi_process.has_threads);
                config.debug_info(true);
                config.wasm_backtrace(true);
                config.wasm_backtrace_details(wasmtime::WasmBacktraceDetails::Enable);
                let engine = match Engine::new(&config) {
                    Ok(success) => success,
                    Err(error) => {
                        panic!("Could not create wasmtime engine: {}.", error)
                    }
                };
                let module = match Module::from_file(&engine, &input_program_path) {
                    Ok(module) => module,
                    Err(error) => {
                        println!(
                            "Could not load {}, error: {}.",
                            input_program_path.display(),
                            error
                        );
                        todo!()
                    }
                };
                run_wasi_process(
                    engine,
                    module,
                    Logger {
                        name: input_program_path.display().to_string(),
                    },
                    api_hub_2,
                    wasi_process.has_threads,
                    this_service_id,
                    interfaces,
                )
            });
            threads.push(handler);
        }

        let mut exit_code = ExitCode::SUCCESS;
        for thread in threads {
            println!("Waiting for a thread to complete.");
            match thread.join().unwrap() {
                Ok(_) => {}
                Err(error) => {
                    println!("One process failed with error: {}.", error);
                    exit_code = ExitCode::FAILURE;
                }
            }
        }

        println!("All threads completed.");
        exit_code
    });
    background_acceptor.await.unwrap();
    exit_code
}
