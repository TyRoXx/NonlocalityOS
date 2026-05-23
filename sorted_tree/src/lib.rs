// The compiler warns about "#![feature(test)]" for no reason.
#![allow(unused_features)]
#![feature(test)]
#![feature(async_iterator)]

pub mod sorted_tree;

#[cfg(test)]
pub mod sorted_tree_tests;

pub mod prolly_tree_editable_node;

#[cfg(test)]
pub mod prolly_tree_editable_node_tests;
