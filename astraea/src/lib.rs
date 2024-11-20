#![feature(test)]

// seems to make the benchmarks go a bit faster than default malloc. https://crates.io/crates/jemallocator
#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

pub mod expressions;
mod expressions_tests;
pub mod storage;
mod storage_benchmarks;
pub mod storage_test;
pub mod tree;
mod tree_benchmarks;
pub mod type_checking;
mod type_checking_tests;
pub mod types;
