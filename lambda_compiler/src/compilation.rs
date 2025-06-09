use crate::{
    parsing::{parse_expression_tolerantly, pop_next_non_whitespace_token, ParserOutput},
    tokenization::tokenize_default_syntax,
    type_checking::{check_types_with_default_globals, TypedExpression},
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

pub fn parse_source(source: &str, source_namespace: &NamespaceId) -> ParserOutput {
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
    result
}

pub async fn compile(
    source: &str,
    source_namespace: &NamespaceId,
) -> Result<CompilerOutput, StoreError> {
    let mut parser_output = parse_source(source, source_namespace);
    match &parser_output.entry_point {
        Some(entry_point) => {
            let type_check_result =
                check_types_with_default_globals(entry_point, *source_namespace).await?;
            parser_output.errors.extend(type_check_result.errors);
            Ok(CompilerOutput::new(
                type_check_result.entry_point,
                parser_output.errors,
            ))
        }
        None => Ok(CompilerOutput::new(None, parser_output.errors)),
    }
}
