use crate::{
    expressions::{CompilerError, CompilerOutput},
    parsing::{parse_entry_point_lambda, pop_next_non_whitespace_token},
    tokenization::tokenize_default_syntax,
};
use astraea::storage::StoreValue;

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
    use crate::expressions::{make_lambda, Lambda, SourceLocation};
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
