use crate::compilation::{compile, CompilerOutput};
use astraea::{
    storage::{InMemoryTreeStorage, StoreTree},
    tree::{HashedTree, Tree, TreeBlob},
};
use lambda::{
    expressions::{apply_evaluated_argument, DeepExpression, Expression, ReadVariable},
    name::{Name, NamespaceId},
};
use std::sync::Arc;

const TEST_SOURCE_NAMESPACE: NamespaceId =
    NamespaceId([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]);

const TEST_GENERATED_NAME_NAMESPACE: NamespaceId = NamespaceId([
    17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32,
]);

#[test_log::test(tokio::test)]
async fn test_hello_world() {
    let source = include_str!("../examples/hello_world.tl");
    let storage = Arc::new(InMemoryTreeStorage::empty());
    let output = compile(
        source,
        &TEST_SOURCE_NAMESPACE,
        &TEST_GENERATED_NAME_NAMESPACE,
        &*storage,
    )
    .await;
    let parameter_name = Name::new(TEST_GENERATED_NAME_NAMESPACE, "".to_string());
    let entry_point = DeepExpression(Expression::make_lambda(
        parameter_name,
        Arc::new(DeepExpression(Expression::make_construct_tree(vec![
            Arc::new(DeepExpression(Expression::make_literal(
                *HashedTree::from(Arc::new(Tree::from_string("Hello, world!").unwrap())).digest(),
            ))),
        ]))),
    ));
    let expected = CompilerOutput::new(Some(entry_point), Vec::new());
    assert_eq!(Ok(expected), output);

    let read_variable: Arc<ReadVariable> = Arc::new(|_name| todo!());
    let argument = storage
        .store_tree(&HashedTree::from(Arc::new(Tree::empty())))
        .await
        .unwrap();
    let evaluated = apply_evaluated_argument(
        &output.unwrap().entry_point.unwrap(),
        &argument,
        &*storage,
        &*storage,
        &read_variable,
    )
    .await;
    let expected_result = storage
        .store_tree(&HashedTree::from(Arc::new(Tree::new(
            TreeBlob::empty(),
            vec![storage
                .store_tree(&HashedTree::from(Arc::new(
                    Tree::from_string("Hello, world!").unwrap(),
                )))
                .await
                .unwrap()],
        ))))
        .await
        .unwrap();
    assert_eq!(Ok(expected_result), evaluated);
}
