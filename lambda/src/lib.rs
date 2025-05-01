#![feature(test)]

pub mod builtins;
mod builtins_test;
pub mod expressions;
mod expressions_tests;
pub mod standard_library;
pub mod name;

#[cfg(test)]
mod hello_world_tests;

#[cfg(test)]
mod effect_tests;
