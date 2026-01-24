#![feature(test)]
#![feature(iterator_try_collect)]

pub mod storage;

#[cfg(test)]
mod storage_tests;

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
