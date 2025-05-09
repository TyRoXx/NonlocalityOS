#![feature(test)]

// seems to make the benchmarks go a bit faster than default malloc. https://crates.io/crates/jemallocator
#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

pub mod storage;

#[cfg(test)]
mod storage_benchmarks;

#[cfg(test)]
pub mod storage_test;

pub mod tree;

#[cfg(test)]
mod tree_benchmarks;

#[cfg(test)]
mod tree_tests;
