#![feature(test)]

// seems to make the benchmarks go a bit faster than default malloc. https://crates.io/crates/jemallocator
#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

pub mod storage;

#[cfg(test)]
pub mod storage_test;

#[cfg(test)]
mod storage_benchmarks;

pub mod deep_tree;

#[cfg(test)]
pub mod deep_tree_tests;

pub mod tree;

#[cfg(test)]
mod tree_tests;

#[cfg(test)]
mod tree_benchmarks;
