use crate::{
    parsing::{parse_entry_point_lambda, pop_next_non_whitespace_token},
    tokenization::tokenize_default_syntax,
};
use astraea::{expressions::Expression, types::NamespaceId};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Copy, Hash)]
pub struct SourceLocation {
    pub line: u64,
    pub column: u64,
}

impl SourceLocation {
    pub fn new(line: u64, column: u64) -> Self {
        Self { line, column }
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone)]
pub struct CompilerError {
    pub message: String,
    pub location: SourceLocation,
}

impl CompilerError {
    pub fn new(message: String, location: SourceLocation) -> Self {
        Self { message, location }
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone)]
pub struct CompilerOutput {
    pub entry_point: Expression,
    pub errors: Vec<CompilerError>,
}

impl CompilerOutput {
    pub fn new(entry_point: Expression, errors: Vec<CompilerError>) -> CompilerOutput {
        CompilerOutput {
            entry_point: entry_point,
            errors: errors,
        }
    }
}

pub async fn compile(source: &str, local_namespace: &NamespaceId) -> CompilerOutput {
    let tokens = tokenize_default_syntax(source);
    let mut token_iterator = tokens.iter().peekable();
    let mut result = parse_entry_point_lambda(&mut token_iterator, local_namespace).await;
    match pop_next_non_whitespace_token(&mut token_iterator) {
        Some(extra_token) => {
            result.errors.push(CompilerError::new(
                "Unexpected token after the entry point lambda".to_string(),
                extra_token.location,
            ));
        }
        None => {}
    }
    result
}
