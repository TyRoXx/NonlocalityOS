#[cfg(test)]
mod tests2 {
    use crate::compilation::{compile, CompilerError, CompilerOutput, SourceLocation};
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
