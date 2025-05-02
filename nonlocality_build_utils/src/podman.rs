#![cfg(target_os = "linux")]

use std::path::Path;
use tracing::{error, info, warn};

async fn enable_podman_unix_socket() {
    let program = Path::new("/usr/bin/systemctl");
    let maybe_output = tokio::process::Command::new(program)
        .args(["--user", "enable", "--now", "podman.socket"])
        .current_dir("/")
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true)
        .output()
        .await;
    let output = match maybe_output {
        Ok(output) => output,
        Err(err) => {
            error!("Failed to spawn {} process: {}.", program.display(), err);
            panic!()
        }
    };
    if output.status.success() {
        return;
    }
    info!("Executable: {}", program.display());
    info!("Exit status: {}", output.status);
    info!("Standard output:");
    let stdout = String::from_utf8_lossy(&output.stdout);
    info!("{}", &stdout);
    info!("Standard error:");
    let stderr = String::from_utf8_lossy(&output.stderr);
    info!("{}", &stderr);
    panic!()
}

#[test_log::test(tokio::test)]
async fn test_podman() {
    enable_podman_unix_socket().await;
    let podman = podman_api::Podman::unix_versioned(
        format!("/run/user/{}/podman/podman.sock", uzers::get_current_uid()),
        podman_api::ApiVersion::new(4, Some(3), Some(1)),
    );
    info!("Socket enabled");
    let image_id = 'a: {
        let docker_image_name_bla = "ubuntu:24.04";
        // We only want to pull from the global Docker registry if we really have to due to rate limiting for free users.
        let existing_image = podman.images().get(docker_image_name_bla);
        match existing_image.inspect().await {
            Ok(inspection) => {
                if let Some(existing_id) = inspection.id {
                    break 'a existing_id;
                }
            }
            Err(error) => info!(
                "Could not find local image {}: {:?}",
                docker_image_name_bla, &error
            ),
        }
        let events =
            futures_util::TryStreamExt::try_collect::<Vec<_>>(futures_util::StreamExt::map(
                podman.images().pull(
                    &podman_api::opts::PullOpts::builder()
                        .reference(format!("docker.io/library/{}", docker_image_name_bla))
                        .build(),
                ),
                |report| {
                    report.and_then(|report| match report.error {
                        Some(error) => Err(podman_api::Error::InvalidResponse(error)),
                        None => Ok(report),
                    })
                },
            ))
            .await
            .unwrap();
        let mut image_id_result = None;
        for event in events {
            info!("{:?}", &event);
            match event.id {
                None => {}
                Some(result) => image_id_result = Some(result),
            }
        }
        image_id_result.unwrap()
    };

    let container_created = podman
        .containers()
        .create(
            &podman_api::opts::ContainerCreateOpts::builder()
                .image(&image_id)
                .command(["/usr/bin/sleep", "9"])
                .build(),
        )
        .await
        .unwrap();
    for warning in container_created.warnings {
        warn!("{}", &warning);
    }
    info!("Container ID: {}", container_created.id);

    // there isn't really documentation for the podman API
    let podman_version = podman.version().await.unwrap().version.unwrap();
    info!("Podman version: {}", &podman_version);
    let expected_status = match podman_version.as_str() {
        "3.4.4" => "configured",
        "4.9.3" => "created",
        "5.2.2" => "created",
        _ => todo!(),
    };

    let container = podman.containers().get(container_created.id.clone());
    assert_eq!(
        Some(expected_status),
        container
            .inspect()
            .await
            .unwrap()
            .state
            .unwrap()
            .status
            .as_deref()
    );

    let container2 = podman.containers().get(container_created.id);
    let _logger = tokio::spawn(async move {
        let mut logs = container2.logs(
            &podman_api::opts::ContainerLogsOpts::builder()
                .stdout(true)
                .stderr(true)
                .follow(true)
                .build(),
        );
        while let Some(chunk) = futures_util::StreamExt::next(&mut logs).await {
            match chunk.unwrap() {
                podman_api::conn::TtyChunk::StdOut(data) => {
                    info!("{}", String::from_utf8_lossy(&data));
                }
                podman_api::conn::TtyChunk::StdErr(data) => {
                    error!("{}", String::from_utf8_lossy(&data));
                }
                _ => {}
            }
        }
    });

    container.start(None).await.unwrap();
    assert_eq!(
        Some("running"),
        container
            .inspect()
            .await
            .unwrap()
            .state
            .unwrap()
            .status
            .as_deref()
    );

    info!(
        "{:?}",
        container.inspect().await.unwrap().state.unwrap().status
    );

    let exec = container
        .create_exec(
            &podman_api::opts::ExecCreateOpts::builder()
                .command(["/usr/bin/ls", "/usr/bin"])
                .attach_stdout(true)
                .attach_stderr(true)
                .build(),
        )
        .await
        .unwrap();

    let opts = Default::default();
    let mut stream = exec.start(&opts).await.unwrap().unwrap();
    while let Some(chunk) = futures_util::StreamExt::next(&mut stream).await {
        match chunk.unwrap() {
            podman_api::conn::TtyChunk::StdOut(data) => {
                info!("{}", String::from_utf8_lossy(&data));
            }
            podman_api::conn::TtyChunk::StdErr(data) => {
                error!("{}", String::from_utf8_lossy(&data));
            }
            _ => {}
        }
    }
}
