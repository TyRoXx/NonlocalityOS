use tokio_stream::Stream;
use tonic_rpc::tonic_rpc;
#[cfg(any(unix, target_os = "wasi"))]
use std::os::fd::{FromRawFd, IntoRawFd, RawFd};
use core::pin::Pin;
use tonic::{Request};

extern "C" {
    fn nonlocality_accept() -> i32;
}

/// The `tonic_rpc` attribute says that we want to build an RPC defined by this trait.
/// The `json` option says that we should use the `tokio-serde` Json codec for serialization.
#[tonic_rpc(json)]
trait Increment {
    /// Our service will have a single endpoint.
    fn increment(arg: i32) -> i32;
}

/// Our server doesn't need any state.
struct State;

#[tonic::async_trait]
impl increment_server::Increment for State {
    /// The request type gets wrapped in a `tonic::Request`.
    /// The response type gets wrapped in a `Result<tonic::Response<_>, tonic::Status>`.
    async fn increment(
        &self,
        request: tonic::Request<i32>,
    ) -> Result<tonic::Response<i32>, tonic::Status> {
        let arg = request.into_inner();
        Ok(tonic::Response::new(arg + 1))
    }
}

struct Acceptor {
    
}

impl Stream for Acceptor {
    type Item = Result< tokio::net::UnixStream, std::error::Error>;

    fn poll_next(self: Pin<&mut Self>, _: &mut std::task::Context<'_>) -> std::task::Poll<Option<<Self as Stream>::Item>> {
        println!("Accepting an API client..");
        let api_fd = unsafe { nonlocality_accept() };
        println!("Accepted an API client..");
        let mut file = unsafe { std::os::unix::net::UnixStream::from_raw_fd(api_fd) };
        tokio::net::UnixStream::from_std(file)
    }
}

pub(crate) async fn run_server() {
    tokio::spawn(async move {
        Server::builder()
            .add_service(increment_server::IncrementServer::new(State))
            .serve_with_incoming(Acceptor{})
            .await
    });
    /*let mut client = increment_client::IncrementClient::connect(format!("http://{}", addr))
        .await
        .unwrap();
    let response = client.increment(32).await.unwrap().into_inner();
    println!("Got {}", response);
    assert_eq!(33, response);*/
}
