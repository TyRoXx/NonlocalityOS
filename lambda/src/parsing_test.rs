#[cfg(test)]
mod tests {
    use crate::{
        builtins::builtins_namespace, parsing::parse_expression,
        tokenization::tokenize_default_syntax,
    };
    use astraea::{
        expressions::{Application, Expression, LambdaExpression},
        tree::BlobDigest,
        types::{Name, Type},
    };

    async fn parse_wellformed_expression(source: &str) -> Expression {
        let tokens = tokenize_default_syntax(source);
        let mut token_iterator = tokens.iter().peekable();
        let output = parse_expression(&mut token_iterator).await.unwrap();
        assert_eq!(None, token_iterator.next());
        output
    }

    async fn test_wellformed_parsing(source: &str, expected: Expression) {
        let output = parse_wellformed_expression(source).await;
        assert_eq!(expected, output);
        assert_eq!(expected.to_string(), output.to_string());
    }

    #[test_log::test(tokio::test)]
    async fn test_parse_lambda() {
        let name = Name::new(builtins_namespace(), "f".to_string());
        let expected = Expression::Lambda(Box::new(LambdaExpression::new(
            Type::Unit,
            name.clone(),
            Expression::ReadVariable(name),
        )));
        test_wellformed_parsing(r#"(f) => f"#, expected).await;
    }

    #[test_log::test(tokio::test)]
    async fn test_parse_nested_lambda() {
        let f = Name::new(builtins_namespace(), "f".to_string());
        let g = Name::new(builtins_namespace(), "g".to_string());
        let expected = Expression::Lambda(Box::new(LambdaExpression::new(
            Type::Unit,
            f.clone(),
            Expression::Lambda(Box::new(LambdaExpression::new(
                Type::Unit,
                g,
                Expression::ReadVariable(f),
            ))),
        )));
        test_wellformed_parsing(r#"(f) => (g) => f"#, expected).await;
    }

    #[test_log::test(tokio::test)]
    async fn test_parse_function_call() {
        let name = Name::new(builtins_namespace(), "f".to_string());
        let f = Expression::ReadVariable(name.clone());
        let expected = Expression::Lambda(Box::new(LambdaExpression::new(
            Type::Unit,
            name,
            Expression::Apply(Box::new(Application::new(
                f.clone(),
                BlobDigest::hash(b"todo"),
                Name::new(builtins_namespace(), "apply".to_string()),
                f,
            ))),
        )));
        test_wellformed_parsing(r#"(f) => f(f)"#, expected).await;
    }
}
