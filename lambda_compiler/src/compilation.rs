use crate::{
    parsing::{parse_expression_tolerantly, pop_next_non_whitespace_token},
    tokenization::tokenize_default_syntax,
    type_checking::{check_types, TypedExpression},
};
use astraea::storage::StoreError;
use lambda::name::NamespaceId;
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

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone)]
pub struct CompilerOutput {
    pub entry_point: Option<TypedExpression>,
    pub errors: Vec<CompilerError>,
}

impl CompilerOutput {
    pub fn new(entry_point: Option<TypedExpression>, errors: Vec<CompilerError>) -> CompilerOutput {
        CompilerOutput {
            entry_point,
            errors,
        }
    }
}

pub fn compile(source: &str, source_namespace: &NamespaceId) -> Result<CompilerOutput, StoreError> {
    let tokens = tokenize_default_syntax(source);
    let mut token_iterator = tokens.iter().peekable();
    let mut result = parse_expression_tolerantly(&mut token_iterator, source_namespace);
    let final_token = pop_next_non_whitespace_token(&mut token_iterator)
        .expect("Expected an end of file token after the entry point lambda");
    match &final_token.content {
        crate::tokenization::TokenContent::EndOfFile => {}
        _ => {
            result.errors.push(CompilerError::new(
                "Unexpected token after the entry point lambda".to_string(),
                final_token.location,
            ));
        }
    }
    let mut environment_builder = crate::type_checking::EnvironmentBuilder::new();
    let result = match &result.entry_point {
        Some(entry_point) => {
            let type_check_result = check_types(entry_point, &mut environment_builder)?;
            result.errors.extend(type_check_result.errors);
            Ok(CompilerOutput::new(
                type_check_result.entry_point,
                result.errors,
            ))
        }
        None => Ok(CompilerOutput::new(None, result.errors)),
    };
    assert!(environment_builder.is_empty());
    result
}
