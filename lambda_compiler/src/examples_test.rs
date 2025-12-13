use crate::{
    compilation::{compile, parse_source, CompilerError},
    format::format_file,
};
use astraea::{
    deep_tree::DeepTree,
    storage::{InMemoryTreeStorage, StoreTree},
    tree::{BlobDigest, HashedTree, Tree, TreeBlob, TreeChildren},
};
use lambda::{expressions::apply_evaluated_argument, name::NamespaceId};
use pretty_assertions::assert_eq;
use std::sync::Arc;

const TEST_SOURCE_NAMESPACE: NamespaceId =
    NamespaceId([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]);

async fn test_example_evaluation(
    source: &str,
    storage: &InMemoryTreeStorage,
    expected_result: &BlobDigest,
) {
    let output = compile(source, &TEST_SOURCE_NAMESPACE).await.unwrap();
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

fn test_example_formatting(source: &str) {
    let parser_output = parse_source(source, &TEST_SOURCE_NAMESPACE);
    assert!(
        parser_output.errors.is_empty(),
        "Unexpected parser errors: {:?}",
        parser_output.errors
    );
    let entry_point = parser_output
        .entry_point
        .expect("Expected an entry point in the parser output");
    let mut formatted = String::new();
    format_file(&entry_point, &mut formatted).expect("Failed to format expression");
    assert_eq!(formatted.as_str(), source);
}

async fn test_example(source: &str, storage: &InMemoryTreeStorage, expected_result: &BlobDigest) {
    test_example_evaluation(source, storage, expected_result).await;
    test_example_formatting(source);
}

fn normalize_line_endings(source: &str) -> String {
    source.replace("\r\n", "\n")
}

#[test_log::test(tokio::test)]
async fn test_hello_world() {
    let source = normalize_line_endings(include_str!("../examples/hello_world.tl"));
    let storage = InMemoryTreeStorage::empty();
    let expected_result = storage
        .store_tree(&HashedTree::from(Arc::new(Tree::new(
            TreeBlob::empty(),
            TreeChildren::try_from(vec![storage
                .store_tree(&HashedTree::from(Arc::new(
                    Tree::from_string("Hello, world!").unwrap(),
                )))
                .await
                .unwrap()])
            .unwrap(),
        ))))
        .await
        .unwrap();
    test_example(&source, &storage, &expected_result).await;
}

#[test_log::test(tokio::test)]
async fn test_integers() {
    let source = normalize_line_endings(include_str!("../examples/integers.tl"));
    let storage = InMemoryTreeStorage::empty();
    let expected_result = storage
        .store_tree(&HashedTree::from(Arc::new(Tree::new(
            TreeBlob::empty(),
            TreeChildren::try_from(vec![
                storage
                    .store_tree(&HashedTree::from(Arc::new(Tree::from_postcard_integer(1))))
                    .await
                    .unwrap(),
                storage
                    .store_tree(&HashedTree::from(Arc::new(Tree::from_postcard_integer(
                        123456789,
                    ))))
                    .await
                    .unwrap(),
                storage
                    .store_tree(&HashedTree::from(Arc::new(Tree::from_postcard_integer(0))))
                    .await
                    .unwrap(),
            ])
            .unwrap(),
        ))))
        .await
        .unwrap();
    test_example(&source, &storage, &expected_result).await;
}

#[test_log::test(tokio::test)]
async fn test_lambda_captures() {
    let source = normalize_line_endings(include_str!("../examples/lambda_captures.tl"));
    let storage = InMemoryTreeStorage::empty();
    let expected_result = storage
        .store_tree(&HashedTree::from(Arc::new(Tree::new(
            TreeBlob::empty(),
            TreeChildren::try_from(vec![
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
            ])
            .unwrap(),
        ))))
        .await
        .unwrap();
    test_example(&source, &storage, &expected_result).await;
}

#[test_log::test(tokio::test)]
async fn test_lambda_parameters() {
    let source = normalize_line_endings(include_str!("../examples/lambda_parameters.tl"));
    let storage = InMemoryTreeStorage::empty();
    let expected_result = storage
        .store_tree(&HashedTree::from(Arc::new(Tree::new(
            TreeBlob::empty(),
            TreeChildren::try_from(vec![
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
            ])
            .unwrap(),
        ))))
        .await
        .unwrap();
    test_example(&source, &storage, &expected_result).await;
}

#[test_log::test(tokio::test)]
async fn test_local_variables() {
    let source = normalize_line_endings(include_str!("../examples/local_variables.tl"));
    let storage = InMemoryTreeStorage::empty();
    let expected_result = storage
        .store_tree(&HashedTree::from(Arc::new(Tree::new(
            TreeBlob::empty(),
            TreeChildren::try_from(vec![
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
            ])
            .unwrap(),
        ))))
        .await
        .unwrap();
    test_example(&source, &storage, &expected_result).await;
}

#[test_log::test(tokio::test)]
async fn test_type_of() {
    let source = normalize_line_endings(include_str!("../examples/type_of.tl"));
    let storage = InMemoryTreeStorage::empty();
    let expected_result = storage
        .store_tree(&HashedTree::from(Arc::new(Tree::new(
            TreeBlob::empty(),
            TreeChildren::try_from(vec![
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
            ])
            .unwrap(),
        ))))
        .await
        .unwrap();
    test_example(&source, &storage, &expected_result).await;
}
