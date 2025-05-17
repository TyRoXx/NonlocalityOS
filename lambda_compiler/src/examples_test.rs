use crate::compilation::{compile, CompilerError};
use astraea::{
    storage::{InMemoryTreeStorage, StoreTree},
    tree::{BlobDigest, HashedTree, Tree, TreeBlob},
};
use lambda::{
    expressions::{apply_evaluated_argument, ReadVariable},
    name::NamespaceId,
};
use std::sync::Arc;

const TEST_SOURCE_NAMESPACE: NamespaceId =
    NamespaceId([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]);

const TEST_GENERATED_NAME_NAMESPACE: NamespaceId = NamespaceId([
    17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32,
]);

async fn test_example(source: &str, storage: &InMemoryTreeStorage, expected_result: &BlobDigest) {
    let output = compile(
        source,
        &TEST_SOURCE_NAMESPACE,
        &TEST_GENERATED_NAME_NAMESPACE,
        &*storage,
    )
    .await
    .unwrap();
    assert_eq!(Vec::<CompilerError>::new(), output.errors);
    let read_variable: Arc<ReadVariable> = Arc::new(|_name| todo!());
    let argument = storage
        .store_tree(&HashedTree::from(Arc::new(Tree::empty())))
        .await
        .unwrap();
    let evaluated = apply_evaluated_argument(
        &output.entry_point.unwrap(),
        &argument,
        &*storage,
        &*storage,
        &read_variable,
        &None,
    )
    .await;
    assert_eq!(Ok(*expected_result), evaluated);
}

#[test_log::test(tokio::test)]
async fn test_hello_world() {
    let source = include_str!("../examples/hello_world.tl");
    let storage = InMemoryTreeStorage::empty();
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
    test_example(source, &storage, &expected_result).await;
}

#[test_log::test(tokio::test)]
async fn test_lambda_parameters() {
    let source = include_str!("../examples/lambda_parameters.tl");
    let storage = InMemoryTreeStorage::empty();
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
    test_example(source, &storage, &expected_result).await;
}
