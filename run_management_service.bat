set NL_CLUSTER_CONFIG=%CD%/target/example_applications_cluster.config 
remove %NL_CLUSTER_CONFIG%
call .\build.bat || exit /B 1
cargo run --verbose --bin management_service --release %NL_CLUSTER_CONFIG%  || exit /B 1
