use crate::{
    expressions::{make_lambda, CompilerError, CompilerOutput, Lambda, SourceLocation},
    tokenization::{tokenize_default_syntax, Token, TokenContent},
};
use astraea::{
    storage::StoreValue,
    tree::{HashedValue, Reference, Value},
};
use std::sync::Arc;

fn pop_next_non_whitespace_token<'t>(tokens: &'t mut std::slice::Iter<Token>) -> Option<&'t Token> {
    loop {
        let next = tokens.next();
        match next {
            Some(token) => match token.content {
                TokenContent::Whitespace => continue,
                TokenContent::Identifier(_) => return next,
                TokenContent::Assign => return next,
                TokenContent::Caret => return next,
                TokenContent::LeftParenthesis => return next,
                TokenContent::RightParenthesis => return next,
                TokenContent::Dot => return next,
            },
            None => return None,
        }
    }
}

fn expect_dot(tokens: &mut std::slice::Iter<Token>) {
    match pop_next_non_whitespace_token(tokens) {
        Some(non_whitespace) => match &non_whitespace.content {
            TokenContent::Whitespace => todo!(),
            TokenContent::Identifier(_) => todo!(),
            TokenContent::Assign => todo!(),
            TokenContent::Caret => todo!(),
            TokenContent::LeftParenthesis => todo!(),
            TokenContent::RightParenthesis => todo!(),
            TokenContent::Dot => {}
        },
        None => todo!(),
    }
}

async fn parse_expression<'t>(
    tokens: &mut std::slice::Iter<'t, Token>,
    storage: &dyn StoreValue,
) -> Reference {
    match pop_next_non_whitespace_token(tokens) {
        Some(non_whitespace) => match &non_whitespace.content {
            TokenContent::Whitespace => todo!(),
            TokenContent::Identifier(identifier) => storage
                .store_value(&HashedValue::from(Arc::new(
                    Value::from_string(&identifier).unwrap(/*TODO*/),
                )))
                .await
                .unwrap(),
            TokenContent::Assign => todo!(),
            TokenContent::Caret => todo!(),
            TokenContent::LeftParenthesis => todo!(),
            TokenContent::RightParenthesis => todo!(),
            TokenContent::Dot => todo!(),
        },
        None => todo!(),
    }
}

async fn parse_lambda<'t>(
    tokens: &mut std::slice::Iter<'t, Token>,
    storage: &dyn StoreValue,
) -> Reference {
    let parameter_name = match pop_next_non_whitespace_token(tokens) {
        Some(non_whitespace) => match &non_whitespace.content {
            TokenContent::Whitespace => todo!(),
            TokenContent::Identifier(identifier) => identifier,
            TokenContent::Assign => todo!(),
            TokenContent::Caret => todo!(),
            TokenContent::LeftParenthesis => todo!(),
            TokenContent::RightParenthesis => todo!(),
            TokenContent::Dot => todo!(),
        },
        None => todo!(),
    };
    let parameter = storage
        .store_value(&HashedValue::from(Arc::new(
            Value::from_string(parameter_name).unwrap(/*TODO*/),
        )))
        .await
        .unwrap();
    expect_dot(tokens);
    let body = parse_expression(tokens, storage).await;
    let result = storage
        .store_value(&HashedValue::from(Arc::new(make_lambda(Lambda::new(
            parameter, body,
        )))))
        .await
        .unwrap();
    result
}

pub async fn parse_entry_point_lambda<'t>(
    tokens: &mut std::slice::Iter<'t, Token>,
    storage: &dyn StoreValue,
) -> CompilerOutput {
    let mut errors = Vec::new();
    match pop_next_non_whitespace_token(tokens) {
        Some(non_whitespace) => match non_whitespace.content {
            TokenContent::Whitespace => todo!(),
            TokenContent::Identifier(_) => todo!(),
            TokenContent::Assign => todo!(),
            TokenContent::Caret => {
                let entry_point = parse_lambda(tokens, storage).await;
                CompilerOutput::new(entry_point, errors)
            }
            TokenContent::LeftParenthesis => todo!(),
            TokenContent::RightParenthesis => todo!(),
            TokenContent::Dot => todo!(),
        },
        None => {
            errors.push(CompilerError::new(
                "Expected entry point lambda".to_string(),
                SourceLocation::new(0, 0),
            ));
            let entry_point = storage
                .store_value(&HashedValue::from(Arc::new(Value::from_unit())))
                .await
                .unwrap();
            CompilerOutput::new(entry_point, errors)
        }
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
    use super::*;
    use astraea::storage::InMemoryValueStorage;
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
