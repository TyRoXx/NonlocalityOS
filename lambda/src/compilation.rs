use crate::{
    parsing::{parse_entry_point_lambda, pop_next_non_whitespace_token},
    tokenization::tokenize_default_syntax,
};
use astraea::{
    storage::StoreValue,
    tree::{Reference, Value},
};
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
    pub entry_point: Reference,
    pub errors: Vec<CompilerError>,
}

impl CompilerOutput {
    pub fn new(entry_point: Reference, errors: Vec<CompilerError>) -> CompilerOutput {
        CompilerOutput {
            entry_point: entry_point,
            errors: errors,
        }
    }

    pub fn from_value(input: Value) -> Option<CompilerOutput> {
        if input.references.len() != 1 {
            return None;
        }
        let errors: Vec<CompilerError> = match postcard::from_bytes(input.blob.as_slice()) {
            Ok(parsed) => parsed,
            Err(_) => return None,
        };
        Some(CompilerOutput::new(input.references[0], errors))
    }
}

pub async fn compile(source: &str, storage: &dyn StoreValue) -> CompilerOutput {
    let tokens = tokenize_default_syntax(source);
    let mut token_iterator = tokens.iter();
    let mut result = parse_entry_point_lambda(&mut token_iterator, storage).await;
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
    use std::sync::Arc;

    use super::*;
    use crate::compilation::SourceLocation;
    use crate::parsing::{make_lambda, Lambda};
    use astraea::{
        storage::InMemoryValueStorage,
        tree::{HashedValue, Value},
    };
    use tokio::sync::Mutex;

    #[test_log::test(tokio::test)]
    async fn test_compile_empty_source() {
        let value_storage =
            InMemoryValueStorage::new(Mutex::new(std::collections::BTreeMap::new()));
        let output = compile("", &value_storage).await;
        let expected = CompilerOutput::new(
            value_storage
                .store_value(&HashedValue::from(Arc::new(Value::from_unit())))
                .await
                .unwrap(),
            vec![CompilerError::new(
                "Expected entry point lambda".to_string(),
                SourceLocation::new(0, 0),
            )],
        );
        assert_eq!(expected, output);
        assert_eq!(1, value_storage.len().await);
    }

    #[test_log::test(tokio::test)]
    async fn test_compile_simple_program() {
        let value_storage =
            InMemoryValueStorage::new(Mutex::new(std::collections::BTreeMap::new()));
        let output = compile(r#"^x . x"#, &value_storage).await;
        let parameter = value_storage
            .store_value(&HashedValue::from(Arc::new(
                Value::from_string("x").unwrap(),
            )))
            .await
            .unwrap();
        let entry_point = value_storage
            .store_value(&HashedValue::from(Arc::new(make_lambda(Lambda::new(
                parameter, parameter,
            )))))
            .await
            .unwrap();
        let expected = CompilerOutput::new(entry_point, Vec::new());
        assert_eq!(expected, output);
        assert_eq!(2, value_storage.len().await);
    }
}
