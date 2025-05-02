#![feature(test)]

pub mod compilation;

#[cfg(test)]
mod compilation_test;

pub mod parsing;

#[cfg(test)]
mod parsing_test;

pub mod tokenization;
