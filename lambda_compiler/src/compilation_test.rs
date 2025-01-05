#[cfg(test)]
mod tests2 {
    use crate::compilation::{compile, CompilerError, CompilerOutput, SourceLocation};
    use astraea::{
        builtins::{BUILTINS_NAMESPACE, LAMBDA_APPLY_METHOD_NAME, UTF8_STRING_TYPE_NAME},
        expressions::{Application, Expression, LambdaExpression},
        tree::{BlobDigest, HashedValue, Value},
        types::{Name, NamespaceId, Type},
    };
    use std::sync::Arc;

    const TEST_NAMESPACE: NamespaceId =
        NamespaceId([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]);

    #[test_log::test(tokio::test)]
    async fn test_compile_empty_source() {
        let output = compile("", &TEST_NAMESPACE).await;
        let expected = CompilerOutput::new(
            Expression::Unit,
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
        let entry_point =
            LambdaExpression::new(Type::Unit, name.clone(), Expression::ReadVariable(name));
        let expected = CompilerOutput::new(Expression::Lambda(Box::new(entry_point)), Vec::new());
        assert_eq!(expected, output);
    }

    #[test_log::test(tokio::test)]
    async fn test_compile_function_call() {
        let output = compile(r#"(f) => f(f)"#, &TEST_NAMESPACE).await;
        let name = Name::new(TEST_NAMESPACE, "f".to_string());
        let f = Expression::ReadVariable(name.clone());
        let entry_point = LambdaExpression::new(
            Type::Unit,
            name,
            Expression::Apply(Box::new(Application::new(
                f.clone(),
                BlobDigest::hash(b"todo"),
                Name::new(BUILTINS_NAMESPACE, LAMBDA_APPLY_METHOD_NAME.to_string()),
                f,
            ))),
        );
        let expected = CompilerOutput::new(Expression::Lambda(Box::new(entry_point)), Vec::new());
        assert_eq!(expected, output);
    }

    #[test_log::test(tokio::test)]
    async fn test_compile_quotes() {
        let output = compile(r#"(print) => print("Hello, world!")"#, &TEST_NAMESPACE).await;
        let print_name = Name::new(TEST_NAMESPACE, "print".to_string());
        let print = Expression::ReadVariable(print_name.clone());
        let entry_point = LambdaExpression::new(
            Type::Unit,
            print_name,
            Expression::Apply(Box::new(Application::new(
                print.clone(),
                BlobDigest::hash(b"todo"),
                Name::new(BUILTINS_NAMESPACE, LAMBDA_APPLY_METHOD_NAME.to_string()),
                Expression::Literal(
                    Type::Named(Name::new(
                        BUILTINS_NAMESPACE,
                        UTF8_STRING_TYPE_NAME.to_string(),
                    )),
                    HashedValue::from(Arc::new(Value::from_string("Hello, world!").unwrap())),
                ),
            ))),
        );
        let expected = CompilerOutput::new(Expression::Lambda(Box::new(entry_point)), Vec::new());
        assert_eq!(expected, output);
    }
}
