use crate::{ast, compilation::CompilerOutput, type_checking::check_types};
use astraea::storage::InMemoryTreeStorage;
use lambda::{
    expressions::DeepExpression,
    name::{Name, NamespaceId},
};
use std::sync::Arc;

const TEST_NAMESPACE: NamespaceId =
    NamespaceId([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]);

#[test_log::test(tokio::test)]
async fn test_check_types_lambda() {
    let x = Name::new(TEST_NAMESPACE, "x".to_string());
    let input = ast::Expression::Lambda {
        parameter_name: x.clone(),
        body: Box::new(ast::Expression::Identifier(x.clone())),
    };
    let storage = Arc::new(InMemoryTreeStorage::empty());
    let output = check_types(&input, &*storage).await;
    let expected = CompilerOutput::new(
        Some(DeepExpression(lambda::expressions::Expression::Lambda {
            parameter_name: x.clone(),
            body: Arc::new(DeepExpression(
                lambda::expressions::Expression::ReadVariable(x),
            )),
        })),
        Vec::new(),
    );
    assert_eq!(output, Ok(expected));
}
