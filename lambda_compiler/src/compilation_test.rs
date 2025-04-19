#[cfg(test)]
mod tests2 {
    use crate::compilation::{compile, CompilerError, CompilerOutput, SourceLocation};
    use astraea::tree::{HashedValue, Value};
    use lambda::expressions::{DeepExpression, Expression};
    use lambda::types::{Name, NamespaceId};
    use std::sync::Arc;

    const TEST_NAMESPACE: NamespaceId =
        NamespaceId([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]);

    #[test_log::test(tokio::test)]
    async fn test_compile_empty_source() {
        let output = compile("", &TEST_NAMESPACE).await;
        let expected = CompilerOutput::new(
            DeepExpression(Expression::Unit),
            vec![CompilerError::new(
                "Parser error: Expected expression, got EOF.".to_string(),
                SourceLocation::new(0, 0),
            )],
        );
        assert_eq!(expected, output);
    }

    #[test_log::test(tokio::test)]
    async fn test_compile_lambda() {
        let output = compile(r#"(x) => x"#, &TEST_NAMESPACE).await;
        let name = Name::new(TEST_NAMESPACE, "x".to_string());
        let entry_point = DeepExpression(Expression::make_lambda(
            name.clone(),
            Arc::new(DeepExpression(Expression::ReadVariable(name))),
        ));
        let expected = CompilerOutput::new(entry_point, Vec::new());
        assert_eq!(expected, output);
    }

    #[test_log::test(tokio::test)]
    async fn test_compile_function_call() {
        let output = compile(r#"(f) => f(f)"#, &TEST_NAMESPACE).await;
        let name = Name::new(TEST_NAMESPACE, "f".to_string());
        let f = Arc::new(DeepExpression(Expression::ReadVariable(name.clone())));
        let entry_point = DeepExpression(Expression::make_lambda(
            name,
            Arc::new(DeepExpression(Expression::make_apply(f.clone(), f))),
        ));
        let expected = CompilerOutput::new(entry_point, Vec::new());
        assert_eq!(expected, output);
    }

    #[test_log::test(tokio::test)]
    async fn test_compile_quotes() {
        let output = compile(r#"(print) => print("Hello, world!")"#, &TEST_NAMESPACE).await;
        let print_name = Name::new(TEST_NAMESPACE, "print".to_string());
        let print = Arc::new(DeepExpression(Expression::ReadVariable(print_name.clone())));
        let entry_point = DeepExpression(Expression::make_lambda(
            print_name,
            Arc::new(DeepExpression(Expression::make_apply(
                print.clone(),
                Arc::new(DeepExpression(Expression::make_literal(
                    HashedValue::from(Arc::new(Value::from_string("Hello, world!").unwrap()))
                        .digest()
                        .clone(),
                ))),
            ))),
        ));
        let expected = CompilerOutput::new(entry_point, Vec::new());
        assert_eq!(expected, output);
    }
}
