#[cfg(test)]
mod tests2 {
    use crate::compilation::{compile, CompilerError, CompilerOutput, SourceLocation};
    use astraea::{
        expressions::{Application, Expression, LambdaExpression},
        tree::{BlobDigest, HashedValue, Value},
        types::{Name, NamespaceId, Type},
    };
    use std::sync::Arc;

    #[test_log::test(tokio::test)]
    async fn test_compile_empty_source() {
        let output = compile("").await;
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
        let output = compile(r#"(x) => x"#).await;
        let name = Name::new(NamespaceId([0; 16]), "x".to_string());
        let entry_point =
            LambdaExpression::new(Type::Unit, name.clone(), Expression::ReadVariable(name));
        let expected = CompilerOutput::new(Expression::Lambda(Box::new(entry_point)), Vec::new());
        assert_eq!(expected, output);
    }

    #[test_log::test(tokio::test)]
    async fn test_compile_function_call() {
        let output = compile(r#"(f) => f(f)"#).await;
        let name = Name::new(NamespaceId::builtins(), "f".to_string());
        let f = Expression::ReadVariable(name.clone());
        let entry_point = LambdaExpression::new(
            Type::Unit,
            name,
            Expression::Apply(Box::new(Application::new(
                f.clone(),
                BlobDigest::hash(b"todo"),
                Name::new(NamespaceId::builtins(), "apply".to_string()),
                f,
            ))),
        );
        let expected = CompilerOutput::new(Expression::Lambda(Box::new(entry_point)), Vec::new());
        assert_eq!(expected, output);
    }

    #[test_log::test(tokio::test)]
    async fn test_compile_quotes() {
        let output = compile(r#"(print) => print("Hello, world!")"#).await;
        let print_name = Name::new(NamespaceId::builtins(), "print".to_string());
        let print = Expression::ReadVariable(print_name.clone());
        let entry_point = LambdaExpression::new(
            Type::Unit,
            print_name,
            Expression::Apply(Box::new(Application::new(
                print.clone(),
                BlobDigest::hash(b"todo"),
                Name::new(NamespaceId::builtins(), "apply".to_string()),
                Expression::Literal(
                    Type::Named(Name::new(
                        NamespaceId::builtins(),
                        "utf8-string".to_string(),
                    )),
                    HashedValue::from(Arc::new(Value::from_string("Hello, world!").unwrap())),
                ),
            ))),
        );
        let expected = CompilerOutput::new(Expression::Lambda(Box::new(entry_point)), Vec::new());
        assert_eq!(expected, output);
    }
}
