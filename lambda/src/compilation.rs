use crate::{
    parsing::{parse_entry_point_lambda, pop_next_non_whitespace_token},
    tokenization::tokenize_default_syntax,
};
use astraea::expressions::Expression;
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

pub async fn compile(source: &str) -> CompilerOutput {
    let tokens = tokenize_default_syntax(source);
    let mut token_iterator = tokens.iter();
    let mut result = parse_entry_point_lambda(&mut token_iterator).await;
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

#[cfg(test)]
mod tests2 {
    use super::*;
    use crate::compilation::SourceLocation;
    use astraea::{
        expressions::{Expression, LambdaExpression},
        types::{Name, NamespaceId, Type},
    };

    #[test_log::test(tokio::test)]
    async fn test_compile_empty_source() {
        let output = compile("").await;
        let expected = CompilerOutput::new(
            Expression::Unit,
            vec![CompilerError::new(
                "Expected entry point lambda".to_string(),
                SourceLocation::new(0, 0),
            )],
        );
        assert_eq!(expected, output);
    }

    #[test_log::test(tokio::test)]
    async fn test_compile_simple_program() {
        let output = compile(r#"^x . x"#).await;
        let name = Name::new(NamespaceId([0; 16]), "x".to_string());
        let entry_point =
            LambdaExpression::new(Type::Unit, name.clone(), Expression::ReadVariable(name));
        let expected = CompilerOutput::new(Expression::Lambda(Box::new(entry_point)), Vec::new());
        assert_eq!(expected, output);
    }
}
