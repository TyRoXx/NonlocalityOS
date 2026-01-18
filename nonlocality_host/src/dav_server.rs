use dogbox_dav_server::run_dav_server;
use std::{net::SocketAddr, sync::Arc};
use tokio::net::TcpListener;
use tracing::info;

pub async fn dav_server_main(
    database_file_name: &std::path::Path,
) -> Result<(), Box<dyn core::error::Error + Send + Sync>> {
    let address = SocketAddr::from(([0, 0, 0, 0], 4918));
    let listener = TcpListener::bind(address).await?;
    info!("Serving DAV on http://{}", address);
    let clock = Arc::new(std::time::SystemTime::now);
    let modified_default = clock();
    let (mut save_status_receiver, server, root_directory) = run_dav_server(
        listener,
        database_file_name,
        modified_default,
        clock,
        std::time::Duration::from_secs(5),
    )
    .await?;
    tokio::try_join!(server, async move {
        let mut last_save_status = None;
        while let Some(status) = save_status_receiver.recv().await {
            if last_save_status.as_ref() != Some(&status) {
                info!("Save status: {:?}", &status);
                last_save_status = Some(status);
            }
        }
        Ok(())
    })?;
    match root_directory.request_save().await {
        Ok(it) => it,
        Err(err) => return Err(Box::from(err)),
    };
    Ok(())
}
