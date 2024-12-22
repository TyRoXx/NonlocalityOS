use crate::{parsing::parse_expression, tokenization::tokenize_default_syntax};
use astraea::{
    expressions::{Application, Expression, LambdaExpression},
    tree::BlobDigest,
    types::{Name, NamespaceId, Type},
};

async fn parse_wellformed_expression(source: &str) -> Expression {
    let tokens = tokenize_default_syntax(source);
    let mut token_iterator = tokens.iter().peekable();
    let output = parse_expression(&mut token_iterator).await;
    assert_eq!(None, token_iterator.next());
    output
}

#[test_log::test(tokio::test)]
async fn test_parse_lambda() {
    let output = parse_wellformed_expression(r#"^f . f"#).await;
    let name = Name::new(NamespaceId::builtins(), "f".to_string());
    let expected = Expression::Lambda(Box::new(LambdaExpression::new(
        Type::Unit,
        name.clone(),
        Expression::ReadVariable(name),
    )));
    assert_eq!(expected, output);
}

#[test_log::test(tokio::test)]
async fn test_parse_function_call() {
    let output = parse_wellformed_expression(r#"^f . f(f)"#).await;
    let name = Name::new(NamespaceId::builtins(), "f".to_string());
    let f = Expression::ReadVariable(name.clone());
    let expected = Expression::Lambda(Box::new(LambdaExpression::new(
        Type::Unit,
        name,
        Expression::Apply(Box::new(Application::new(
            f.clone(),
            BlobDigest::hash(b"todo"),
            Name::new(NamespaceId::builtins(), "apply".to_string()),
            f,
        ))),
    )));
    assert_eq!(expected, output);
}
