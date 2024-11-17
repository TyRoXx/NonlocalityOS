#![feature(test)]

// seems to make the benchmarks go a bit faster than default malloc. https://crates.io/crates/jemallocator
#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

pub mod compiler;
pub mod storage;
mod storage_benchmarks;
pub mod storage_test;
pub mod tree;
mod tree_benchmarks;
