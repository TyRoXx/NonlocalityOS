#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::{parsing::parse_expression, tokenization::tokenize_default_syntax};
    use lambda::expressions::{DeepExpression, Expression};
    use lambda::types::{Name, NamespaceId};

    const TEST_NAMESPACE: NamespaceId =
        NamespaceId([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]);

    async fn parse_wellformed_expression(source: &str) -> DeepExpression {
        let tokens = tokenize_default_syntax(source);
        let mut token_iterator = tokens.iter().peekable();
        let output = parse_expression(&mut token_iterator, &TEST_NAMESPACE)
            .await
            .unwrap();
        assert_eq!(None, token_iterator.next());
        output
    }

    async fn test_wellformed_parsing(source: &str, expected: DeepExpression) {
        let output = parse_wellformed_expression(source).await;
        assert_eq!(expected, output);
        assert_eq!(expected.to_string(), output.to_string());
    }

    #[test_log::test(tokio::test)]
    async fn test_parse_lambda() {
        let name = Name::new(TEST_NAMESPACE, "f".to_string());
        let expected = DeepExpression(Expression::make_lambda(
            name.clone(),
            Arc::new(DeepExpression(Expression::ReadVariable(name))),
        ));
        test_wellformed_parsing(r#"(f) => f"#, expected).await;
    }

    #[test_log::test(tokio::test)]
    async fn test_parse_nested_lambda() {
        let f = Name::new(TEST_NAMESPACE, "f".to_string());
        let g = Name::new(TEST_NAMESPACE, "g".to_string());
        let expected = DeepExpression(Expression::make_lambda(
            f.clone(),
            Arc::new(DeepExpression(Expression::make_lambda(
                g,
                Arc::new(DeepExpression(Expression::ReadVariable(f))),
            ))),
        ));
        test_wellformed_parsing(r#"(f) => (g) => f"#, expected).await;
    }

    #[test_log::test(tokio::test)]
    async fn test_parse_function_call() {
        let name = Name::new(TEST_NAMESPACE, "f".to_string());
        let f = Arc::new(DeepExpression(Expression::ReadVariable(name.clone())));
        let expected = DeepExpression(Expression::make_lambda(
            name,
            Arc::new(DeepExpression(Expression::make_apply(f.clone(), f))),
        ));
        test_wellformed_parsing(r#"(f) => f(f)"#, expected).await;
    }
}
