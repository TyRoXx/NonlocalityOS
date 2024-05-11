use crate::run::{run_cargo, run_process_with_error_only_output, NumberOfErrors, ReportProgress};
use std::{collections::HashMap, sync::Arc};

pub async fn install_grcov(
    working_directory: &std::path::Path,
    progress_reporter: &Arc<dyn ReportProgress + Sync + Send>,
) -> NumberOfErrors {
    run_cargo(
        &working_directory,
        &["install", "grcov"],
        &HashMap::new(),
        progress_reporter,
    )
    .await
}

pub async fn generate_coverage_report_with_grcov(
    repository: &std::path::Path,
    coverage_info_directory: &std::path::Path,
    coverage_report_directory: &std::path::Path,
    progress_reporter: &Arc<dyn ReportProgress + Sync + Send>,
) -> NumberOfErrors {
    run_process_with_error_only_output(
        &repository,
        std::path::Path::new("grcov"),
        &[
            coverage_info_directory
                .to_str()
                .expect("Tried to convert path to string"),
            "--log-level",
            "DEBUG",
            "--binary-path",
            repository
                .join("target/debug/deps")
                .to_str()
                .expect("Tried to convert path to string"),
            "--source-dir",
            repository
                .to_str()
                .expect("Tried to convert path to string"),
            // exclude some generated files in the target dir:
            "--ignore",
            "target/debug/build/*",
            "-t",
            "html",
            "--branch",
            "--output-path",
            coverage_report_directory
                .to_str()
                .expect("Tried to convert path to string"),
        ],
        &HashMap::new(),
        progress_reporter,
    )
    .await
}

pub fn delete_directory(root: &std::path::Path) -> NumberOfErrors {
    match std::fs::metadata(&root) {
        Ok(_) => match std::fs::remove_dir_all(&root) {
            Ok(_) => NumberOfErrors(0),
            Err(error) => {
                println!("Could not delete {}: {}", root.display(), error);
                NumberOfErrors(1)
            }
        },
        Err(_) => NumberOfErrors(0),
    }
}
