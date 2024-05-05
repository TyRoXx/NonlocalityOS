set NL_CLUSTER_CONFIG=%CD%/target/example_applications_cluster.config
set NL_FILESYSTEM_ACCESS_PARENT=%CD%/filesystem_access
remove %NL_CLUSTER_CONFIG%
call .\build.bat || exit /B 1
cargo run --verbose --bin management_service --release %NL_CLUSTER_CONFIG% %NL_FILESYSTEM_ACCESS_PARENT% || exit /B 1
