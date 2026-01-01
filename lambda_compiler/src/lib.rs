#![feature(test)]

pub mod compilation;

#[cfg(test)]
mod compilation_tests;

pub mod ast;
pub mod type_checking;

#[cfg(test)]
mod type_checking_tests;

pub mod parsing;

#[cfg(test)]
mod parsing_tests;

pub mod tokenization;

#[cfg(test)]
mod tokenization_tests;

#[cfg(test)]
mod examples_tests;

pub mod format;

#[cfg(test)]
mod format_tests;
