use crate::expressions::{evaluate, DeepExpression, Expression};
use astraea::{
    storage::{InMemoryTreeStorage, LoadTree},
    tree::Tree,
};
use std::sync::Arc;

async fn expect_evaluate_result(
    expression: &DeepExpression,
    storage: &InMemoryTreeStorage,
    expected_result: &Tree,
) {
    let evaluated_digest = evaluate(expression, storage, storage, &None, &None)
        .await
        .unwrap();
    let evaluated = storage
        .load_tree(&evaluated_digest)
        .await
        .unwrap()
        .hash()
        .unwrap();
    assert_eq!(expected_result, evaluated.tree().as_ref());
}

#[test_log::test(tokio::test)]
async fn test_lambda_parameter() {
    let storage = InMemoryTreeStorage::empty();
    let empty_tree = Arc::new(DeepExpression(Expression::make_literal(Tree::empty())));
    let expected_result = Tree::from_string("Hello, world!").unwrap();
    let lambda = DeepExpression(Expression::make_lambda(
        empty_tree,
        Arc::new(DeepExpression(Expression::Argument)),
    ));
    let apply = DeepExpression(Expression::make_apply(
        Arc::new(lambda),
        Arc::new(DeepExpression(Expression::make_literal(
            expected_result.clone(),
        ))),
    ));
    expect_evaluate_result(&apply, &storage, &expected_result).await;
}
