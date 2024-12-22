async fn run_host() -> std::io::Result<()> {
    return Ok(());
}

#[tokio::test(flavor = "multi_thread")]
async fn test_run_host() -> std::io::Result<()> {
    run_host().await
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> std::io::Result<()> {
    run_host().await
}
