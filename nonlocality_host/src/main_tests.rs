use crate::{
    fake_operating_system::{FakeDirectoryEntry, FakeOperatingSystem, RunProcessFunction},
    install,
    operating_system::OperatingSystem,
    uninstall, SYSTEMD_SERVICES_DIRECTORY,
};
use pretty_assertions::assert_eq;
use std::{collections::BTreeMap, sync::Arc};

#[test_log::test(tokio::test)]
async fn test_install() {
    let run_process_call_count = Arc::new(tokio::sync::Mutex::new(0));
    let run_process: Box<RunProcessFunction> = Box::new({
        let run_process_call_count = run_process_call_count.clone();
        move |working_directory, executable, arguments| {
            let working_directory = working_directory.to_path_buf();
            let executable = executable.to_path_buf();
            let arguments = arguments
                .iter()
                .map(|arg| arg.to_string())
                .collect::<Vec<_>>();
            let run_process_call_count = run_process_call_count.clone();
            Box::pin(async move {
                let mut locked = run_process_call_count.lock().await;
                *locked += 1;
                match *locked {
                    1 => {
                        assert_eq!(working_directory, std::path::Path::new("/"));
                        assert_eq!(executable, std::path::Path::new("/usr/bin/systemctl"));
                        assert_eq!(arguments, &["daemon-reload"]);
                        Ok(())
                    }
                    2 => {
                        assert_eq!(working_directory, std::path::Path::new("/"));
                        assert_eq!(executable, std::path::Path::new("/usr/bin/systemctl"));
                        assert_eq!(arguments, &["enable", "nonlocalityos_host.service"]);
                        Ok(())
                    }
                    3 => {
                        assert_eq!(working_directory, std::path::Path::new("/"));
                        assert_eq!(executable, std::path::Path::new("/usr/bin/systemctl"));
                        assert_eq!(arguments, &["restart", "nonlocalityos_host.service"]);
                        Ok(())
                    }
                    4 => {
                        assert_eq!(working_directory, std::path::Path::new("/"));
                        assert_eq!(executable, std::path::Path::new("/usr/bin/systemctl"));
                        assert_eq!(arguments, &["status", "nonlocalityos_host.service"]);
                        Ok(())
                    }
                    _ => panic!("Unexpected call to run_process"),
                }
            })
        }
    });
    let fake_operating_system = FakeOperatingSystem::new(run_process).unwrap();
    fake_operating_system
        .open_directory(std::path::Path::new(SYSTEMD_SERVICES_DIRECTORY))
        .await
        .unwrap()
        .create()
        .await
        .unwrap();
    install(
        std::path::Path::new("/home/nonlocality/.nonlocality"),
        std::ffi::OsStr::new("nonlocality_host"),
        &fake_operating_system,
    )
    .await
    .unwrap();
    assert_eq!(4, *run_process_call_count.lock().await);
    let fake_filesystem_status = fake_operating_system.enumerate_filesystem().await.unwrap();
    let expected_filesystem_status = BTreeMap::from([
        (
            std::path::PathBuf::from("etc/systemd/system/nonlocalityos_host.service"),
            FakeDirectoryEntry::File(275),
        ),
        (
            std::path::PathBuf::from("home/nonlocality/.nonlocality/database.sqlite3"),
            FakeDirectoryEntry::File(36864),
        ),
        (
            std::path::PathBuf::from("tmp"),
            FakeDirectoryEntry::Directory,
        ),
    ]);
    assert_eq!(expected_filesystem_status, fake_filesystem_status);
}

#[test_log::test(tokio::test)]
async fn test_uninstall_nothing() {
    let run_process_call_count = Arc::new(tokio::sync::Mutex::new(0));
    let run_process: Box<RunProcessFunction> = Box::new({
        let run_process_call_count = run_process_call_count.clone();
        move |working_directory, executable, arguments| {
            let working_directory = working_directory.to_path_buf();
            let executable = executable.to_path_buf();
            let arguments = arguments
                .iter()
                .map(|arg| arg.to_string())
                .collect::<Vec<_>>();
            let run_process_call_count = run_process_call_count.clone();
            Box::pin(async move {
                let mut locked = run_process_call_count.lock().await;
                *locked += 1;
                match *locked {
                    1 => {
                        assert_eq!(working_directory, std::path::Path::new("/"));
                        assert_eq!(executable, std::path::Path::new("/usr/bin/systemctl"));
                        assert_eq!(arguments, &["daemon-reload"]);
                        Ok(())
                    }
                    _ => panic!("Unexpected call to run_process"),
                }
            })
        }
    });
    let fake_operating_system = FakeOperatingSystem::new(run_process).unwrap();
    fake_operating_system
        .open_directory(std::path::Path::new(SYSTEMD_SERVICES_DIRECTORY))
        .await
        .unwrap()
        .create()
        .await
        .unwrap();
    uninstall(&fake_operating_system).await.unwrap();
    assert_eq!(1, *run_process_call_count.lock().await);
    let fake_filesystem_status = fake_operating_system.enumerate_filesystem().await.unwrap();
    let expected_filesystem_status = BTreeMap::from([(
        std::path::PathBuf::from("etc/systemd/system"),
        FakeDirectoryEntry::Directory,
    )]);
    assert_eq!(expected_filesystem_status, fake_filesystem_status);
}

#[test_log::test(tokio::test)]
async fn test_uninstall() {
    let run_process_call_count = Arc::new(tokio::sync::Mutex::new(0));
    let run_process: Box<RunProcessFunction> = Box::new({
        let run_process_call_count = run_process_call_count.clone();
        move |working_directory, executable, arguments| {
            let working_directory = working_directory.to_path_buf();
            let executable = executable.to_path_buf();
            let arguments = arguments
                .iter()
                .map(|arg| arg.to_string())
                .collect::<Vec<_>>();
            let run_process_call_count = run_process_call_count.clone();
            Box::pin(async move {
                let mut locked = run_process_call_count.lock().await;
                *locked += 1;
                match *locked {
                    1 => {
                        assert_eq!(working_directory, std::path::Path::new("/"));
                        assert_eq!(executable, std::path::Path::new("/usr/bin/systemctl"));
                        assert_eq!(arguments, &["disable", "nonlocalityos_host.service"]);
                        Ok(())
                    }
                    2 => {
                        assert_eq!(working_directory, std::path::Path::new("/"));
                        assert_eq!(executable, std::path::Path::new("/usr/bin/systemctl"));
                        assert_eq!(arguments, &["daemon-reload"]);
                        Ok(())
                    }
                    _ => panic!("Unexpected call to run_process"),
                }
            })
        }
    });
    let fake_operating_system = FakeOperatingSystem::new(run_process).unwrap();
    {
        let systemd_service_directory = fake_operating_system
            .open_directory(std::path::Path::new(SYSTEMD_SERVICES_DIRECTORY))
            .await
            .unwrap();
        systemd_service_directory.create().await.unwrap();
        let systemd_service_directory_path = systemd_service_directory.lock().await.unwrap();
        let service_file_path = systemd_service_directory_path.join("nonlocalityos_host.service");
        std::fs::write(&service_file_path, b"[Unit]").unwrap();
        systemd_service_directory.unlock().await.unwrap();
    }
    uninstall(&fake_operating_system).await.unwrap();
    assert_eq!(2, *run_process_call_count.lock().await);
    let fake_filesystem_status = fake_operating_system.enumerate_filesystem().await.unwrap();
    let expected_filesystem_status = BTreeMap::from([(
        std::path::PathBuf::from("etc/systemd/system"),
        FakeDirectoryEntry::Directory,
    )]);
    assert_eq!(expected_filesystem_status, fake_filesystem_status);
}
