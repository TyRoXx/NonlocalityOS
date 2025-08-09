use crate::expressions::{evaluate, DeepExpression, Expression};
use astraea::{deep_tree::DeepTree, storage::InMemoryTreeStorage};
use pretty_assertions::assert_eq;
use std::sync::Arc;

async fn expect_evaluate_result(
    expression: &DeepExpression,
    storage: &InMemoryTreeStorage,
    expected_result: &DeepTree,
) {
    let evaluated_digest = evaluate(expression, storage, storage, &None, &None)
        .await
        .unwrap();
    let evaluated = DeepTree::deserialize(&evaluated_digest, storage)
        .await
        .unwrap();
    assert_eq!(expected_result, &evaluated);
}

#[test_log::test(tokio::test)]
async fn test_lambda_parameter() {
    let storage = InMemoryTreeStorage::empty();
    let empty_tree = Arc::new(DeepExpression(Expression::make_literal(DeepTree::empty())));
    let expected_result = DeepTree::try_from_string("Hello, world!").unwrap();
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
