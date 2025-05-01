#![feature(test)]

pub mod builtins;
mod builtins_test;
pub mod expressions;
pub mod name;
pub mod standard_library;

#[cfg(test)]
mod expressions_tests;

#[cfg(test)]
mod hello_world_tests;

#[cfg(test)]
mod effect_tests;
