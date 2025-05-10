use crate::{
    parsing::{parse_expression_tolerantly, pop_next_non_whitespace_token},
    tokenization::tokenize_default_syntax,
    type_checking::check_types,
};
use astraea::storage::{StoreError, StoreTree};
use lambda::{expressions::DeepExpression, name::NamespaceId};
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
    pub entry_point: Option<DeepExpression>,
    pub errors: Vec<CompilerError>,
}

impl CompilerOutput {
    pub fn new(entry_point: Option<DeepExpression>, errors: Vec<CompilerError>) -> CompilerOutput {
        CompilerOutput {
            entry_point,
            errors,
        }
    }
}

pub async fn compile(
    source: &str,
    source_namespace: &NamespaceId,
    generated_name_namespace: &NamespaceId,
    storage: &dyn StoreTree,
) -> Result<CompilerOutput, StoreError> {
    let tokens = tokenize_default_syntax(source);
    let mut token_iterator = tokens.iter().peekable();
    let mut result = parse_expression_tolerantly(&mut token_iterator, source_namespace);
    if let Some(extra_token) = pop_next_non_whitespace_token(&mut token_iterator) {
        result.errors.push(CompilerError::new(
            "Unexpected token after the entry point lambda".to_string(),
            extra_token.location,
        ));
    }
    match &result.entry_point {
        Some(entry_point) => {
            let type_check_result =
                check_types(entry_point, generated_name_namespace, storage).await?;
            result.errors.extend(type_check_result.errors);
            Ok(CompilerOutput::new(
                type_check_result.entry_point,
                result.errors,
            ))
        }
        None => Ok(CompilerOutput::new(None, result.errors)),
    }
}
