#![feature(test)]

pub mod compilation;

#[cfg(test)]
mod compilation_test;

pub mod ast;
pub mod type_checking;

#[cfg(test)]
mod type_checking_test;

pub mod parsing;

#[cfg(test)]
mod parsing_test;

pub mod tokenization;

#[cfg(test)]
mod tokenization_test;

#[cfg(test)]
mod examples_test;

pub mod format;

#[cfg(test)]
mod format_test;
