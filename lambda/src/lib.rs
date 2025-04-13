#![feature(test)]

pub mod builtins;
mod builtins_test;
pub mod expressions;
mod expressions_tests;
pub mod standard_library;
pub mod type_checking;
mod type_checking_tests;
pub mod types;

#[cfg(test)]
mod complex_expression_tests;

#[cfg(test)]
mod hello_world_tests;

#[cfg(test)]
mod memory_byte_sink_tests;
