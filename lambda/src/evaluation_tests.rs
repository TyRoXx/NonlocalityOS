use crate::{
    expressions::{evaluate, DeepExpression, Expression, ReadVariable},
    name::{Name, NamespaceId},
};
use astraea::{
    storage::{InMemoryTreeStorage, StoreTree},
    tree::{BlobDigest, HashedTree, Tree},
};
use std::sync::Arc;

const TEST_NAMESPACE: NamespaceId = NamespaceId([
    17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32,
]);

async fn expect_evaluate_result(
    expression: &DeepExpression,
    storage: &InMemoryTreeStorage,
    expected_result: &BlobDigest,
) {
    let read_variable: Arc<ReadVariable> = Arc::new(|_name| todo!());
    let evaluated = evaluate(expression, &*storage, &*storage, &read_variable).await;
    assert_eq!(Ok(*expected_result), evaluated);
}

#[test_log::test(tokio::test)]
async fn test_lambda_parameter() {
    let storage = InMemoryTreeStorage::empty();
    let expected_result = storage
        .store_tree(&HashedTree::from(Arc::new(
            Tree::from_string("Hello, world!").unwrap(),
        )))
        .await
        .unwrap();
    let lambda = DeepExpression(Expression::make_lambda(
        Name::new(TEST_NAMESPACE, "x".to_string()),
        Arc::new(DeepExpression(Expression::ReadVariable(Name::new(
            TEST_NAMESPACE,
            "x".to_string(),
        )))),
    ));
    let apply = DeepExpression(Expression::make_apply(
        Arc::new(lambda),
        Arc::new(DeepExpression(Expression::make_literal(
            expected_result.clone(),
        ))),
    ));
    expect_evaluate_result(&apply, &storage, &expected_result).await;
}
