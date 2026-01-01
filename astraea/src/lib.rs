#![feature(test)]
#![feature(iterator_try_collect)]

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
