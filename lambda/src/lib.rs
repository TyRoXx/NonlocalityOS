#![feature(test)]

pub mod builtins;
pub mod expressions;
pub mod name;
pub mod standard_library;

#[cfg(test)]
mod expressions_tests;

#[cfg(test)]
mod hello_world_tests;

#[cfg(test)]
mod effect_tests;

#[cfg(test)]
mod evaluation_tests;
