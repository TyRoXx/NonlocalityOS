use crate::compilation::{compile, CompilerError};
use astraea::{
    deep_tree::DeepTree,
    storage::{InMemoryTreeStorage, StoreTree},
    tree::{BlobDigest, HashedTree, Tree, TreeBlob},
};
use lambda::{expressions::apply_evaluated_argument, name::NamespaceId};
use std::sync::Arc;

const TEST_SOURCE_NAMESPACE: NamespaceId =
    NamespaceId([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]);

async fn test_example(source: &str, storage: &InMemoryTreeStorage, expected_result: &BlobDigest) {
    let output = compile(source, &TEST_SOURCE_NAMESPACE).unwrap();
    assert_eq!(Vec::<CompilerError>::new(), output.errors);
    let argument = storage
        .store_tree(&HashedTree::from(Arc::new(Tree::empty())))
        .await
        .unwrap();
    let evaluated = apply_evaluated_argument(
        &output.entry_point.unwrap().expression,
        &argument,
        storage,
        storage,
        &None,
        &None,
    )
    .await
    .unwrap();
    assert_eq!(
        DeepTree::deserialize(expected_result, storage)
            .await
            .unwrap(),
        DeepTree::deserialize(&evaluated, storage).await.unwrap()
    );
    assert_eq!(*expected_result, evaluated);
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
async fn test_lambda_captures() {
    let source = include_str!("../examples/lambda_captures.tl");
    let storage = InMemoryTreeStorage::empty();
    let expected_result = storage
        .store_tree(&HashedTree::from(Arc::new(Tree::new(
            TreeBlob::empty(),
            vec![
                storage
                    .store_tree(&HashedTree::from(Arc::new(
                        Tree::from_string("lam").unwrap(),
                    )))
                    .await
                    .unwrap(),
                storage
                    .store_tree(&HashedTree::from(Arc::new(
                        Tree::from_string("bda").unwrap(),
                    )))
                    .await
                    .unwrap(),
            ],
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
            vec![
                storage
                    .store_tree(&HashedTree::from(Arc::new(
                        Tree::from_string("lam").unwrap(),
                    )))
                    .await
                    .unwrap(),
                storage
                    .store_tree(&HashedTree::from(Arc::new(
                        Tree::from_string("bda").unwrap(),
                    )))
                    .await
                    .unwrap(),
            ],
        ))))
        .await
        .unwrap();
    test_example(source, &storage, &expected_result).await;
}

#[test_log::test(tokio::test)]
async fn test_local_variables() {
    let source = include_str!("../examples/local_variables.tl");
    let storage = InMemoryTreeStorage::empty();
    let expected_result = storage
        .store_tree(&HashedTree::from(Arc::new(Tree::new(
            TreeBlob::empty(),
            vec![
                storage
                    .store_tree(&HashedTree::from(Arc::new(
                        Tree::from_string("lam").unwrap(),
                    )))
                    .await
                    .unwrap(),
                storage
                    .store_tree(&HashedTree::from(Arc::new(
                        Tree::from_string("bda").unwrap(),
                    )))
                    .await
                    .unwrap(),
            ],
        ))))
        .await
        .unwrap();
    test_example(source, &storage, &expected_result).await;
}
