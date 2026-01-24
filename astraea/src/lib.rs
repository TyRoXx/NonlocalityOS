#![feature(test)]
#![feature(iterator_try_collect)]

pub mod storage;

#[cfg(test)]
mod storage_benchmarks;

pub mod deep_tree;

#[cfg(test)]
mod deep_tree_tests;

pub mod tree;

#[cfg(test)]
mod tree_tests;

#[cfg(test)]
mod tree_benchmarks;

pub mod sqlite_storage;

#[cfg(test)]
mod sqlite_storage_tests;

pub mod in_memory_storage;

#[cfg(test)]
mod in_memory_storage_tests;

pub mod load_cache_storage;

pub mod delayed_hashed_tree;

#[cfg(test)]
mod delayed_hashed_tree_tests;
