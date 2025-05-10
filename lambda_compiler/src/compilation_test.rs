use crate::compilation::{compile, CompilerError, CompilerOutput, SourceLocation};
use astraea::storage::InMemoryTreeStorage;
use astraea::tree::{HashedTree, Tree};
use lambda::expressions::{DeepExpression, Expression};
use lambda::name::{Name, NamespaceId};
use std::sync::Arc;

const TEST_SOURCE_NAMESPACE: NamespaceId =
    NamespaceId([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]);

const TEST_GENERATED_NAME_NAMESPACE: NamespaceId = NamespaceId([
    17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32,
]);

#[test_log::test(tokio::test)]
async fn test_compile_empty_source() {
    let storage = Arc::new(InMemoryTreeStorage::empty());
    let output = compile(
        "",
        &TEST_SOURCE_NAMESPACE,
        &TEST_GENERATED_NAME_NAMESPACE,
        &*storage,
    )
    .await;
    let expected = CompilerOutput::new(
        None,
        vec![CompilerError::new(
            "Parser error: Expected expression, got EOF.".to_string(),
            SourceLocation::new(0, 0),
        )],
    );
    assert_eq!(Ok(expected), output);
}

#[test_log::test(tokio::test)]
async fn test_compile_lambda() {
    let storage = Arc::new(InMemoryTreeStorage::empty());
    let output = compile(
        r#"(x) => x"#,
        &TEST_SOURCE_NAMESPACE,
        &TEST_GENERATED_NAME_NAMESPACE,
        &*storage,
    )
    .await;
    let name_in_source = Name::new(TEST_SOURCE_NAMESPACE, "x".to_string());
    let name_in_output = Name::new(TEST_GENERATED_NAME_NAMESPACE, "x".to_string());
    let entry_point = DeepExpression(Expression::make_lambda(
        name_in_output,
        Arc::new(DeepExpression(Expression::ReadVariable(name_in_source))),
    ));
    let expected = CompilerOutput::new(Some(entry_point), Vec::new());
    assert_eq!(Ok(expected), output);
}

#[test_log::test(tokio::test)]
async fn test_compile_function_call() {
    let storage = Arc::new(InMemoryTreeStorage::empty());
    let output = compile(
        r#"(f) => f(f)"#,
        &TEST_SOURCE_NAMESPACE,
        &TEST_GENERATED_NAME_NAMESPACE,
        &*storage,
    )
    .await;
    let name_in_source = Name::new(TEST_SOURCE_NAMESPACE, "f".to_string());
    let name_in_output = Name::new(TEST_GENERATED_NAME_NAMESPACE, "f".to_string());
    let f = Arc::new(DeepExpression(Expression::ReadVariable(
        name_in_source.clone(),
    )));
    let entry_point = DeepExpression(Expression::make_lambda(
        name_in_output,
        Arc::new(DeepExpression(Expression::make_apply(f.clone(), f))),
    ));
    let expected = CompilerOutput::new(Some(entry_point), Vec::new());
    assert_eq!(Ok(expected), output);
}

#[test_log::test(tokio::test)]
async fn test_compile_quotes() {
    let storage = Arc::new(InMemoryTreeStorage::empty());
    let output = compile(
        r#"(print) => print("Hello, world!")"#,
        &TEST_SOURCE_NAMESPACE,
        &TEST_GENERATED_NAME_NAMESPACE,
        &*storage,
    )
    .await;
    let print_source_name = Name::new(TEST_SOURCE_NAMESPACE, "print".to_string());
    let print_generated_name = Name::new(TEST_GENERATED_NAME_NAMESPACE, "print".to_string());
    let print = Arc::new(DeepExpression(Expression::ReadVariable(
        print_source_name.clone(),
    )));
    let entry_point = DeepExpression(Expression::make_lambda(
        print_generated_name,
        Arc::new(DeepExpression(Expression::make_apply(
            print.clone(),
            Arc::new(DeepExpression(Expression::make_literal(
                HashedTree::from(Arc::new(Tree::from_string("Hello, world!").unwrap()))
                    .digest()
                    .clone(),
            ))),
        ))),
    ));
    let expected = CompilerOutput::new(Some(entry_point), Vec::new());
    assert_eq!(Ok(expected), output);
}

#[test_log::test(tokio::test)]
async fn test_compile_tree_construction_0_children() {
    let storage = Arc::new(InMemoryTreeStorage::empty());
    let output = compile(
        r#"() => []"#,
        &TEST_SOURCE_NAMESPACE,
        &TEST_GENERATED_NAME_NAMESPACE,
        &*storage,
    )
    .await;
    let unused_name = Name::new(TEST_GENERATED_NAME_NAMESPACE, "".to_string());
    let entry_point = DeepExpression(Expression::make_lambda(
        unused_name,
        Arc::new(DeepExpression(Expression::make_construct_tree(vec![]))),
    ));
    let expected = CompilerOutput::new(Some(entry_point), Vec::new());
    assert_eq!(Ok(expected), output);
}

#[test_log::test(tokio::test)]
async fn test_compile_tree_construction_1_child() {
    let storage = Arc::new(InMemoryTreeStorage::empty());
    let output = compile(
        r#"() => ["Hello, world!"]"#,
        &TEST_SOURCE_NAMESPACE,
        &TEST_GENERATED_NAME_NAMESPACE,
        &*storage,
    )
    .await;
    let unused_name = Name::new(TEST_GENERATED_NAME_NAMESPACE, "".to_string());
    let entry_point = DeepExpression(Expression::make_lambda(
        unused_name,
        Arc::new(DeepExpression(Expression::make_construct_tree(vec![
            Arc::new(DeepExpression(Expression::make_literal(
                HashedTree::from(Arc::new(Tree::from_string("Hello, world!").unwrap()))
                    .digest()
                    .clone(),
            ))),
        ]))),
    ));
    let expected = CompilerOutput::new(Some(entry_point), Vec::new());
    assert_eq!(Ok(expected), output);
}

#[test_log::test(tokio::test)]
async fn test_compile_tree_construction_2_children() {
    let storage = Arc::new(InMemoryTreeStorage::empty());
    let output = compile(
        r#"() => ["Hello, ", "world!"]"#,
        &TEST_SOURCE_NAMESPACE,
        &TEST_GENERATED_NAME_NAMESPACE,
        &*storage,
    )
    .await;
    let unused_name = Name::new(TEST_GENERATED_NAME_NAMESPACE, "".to_string());
    let entry_point = DeepExpression(Expression::make_lambda(
        unused_name,
        Arc::new(DeepExpression(Expression::make_construct_tree(vec![
            Arc::new(DeepExpression(Expression::make_literal(
                HashedTree::from(Arc::new(Tree::from_string("Hello, ").unwrap()))
                    .digest()
                    .clone(),
            ))),
            Arc::new(DeepExpression(Expression::make_literal(
                HashedTree::from(Arc::new(Tree::from_string("world!").unwrap()))
                    .digest()
                    .clone(),
            ))),
        ]))),
    ));
    let expected = CompilerOutput::new(Some(entry_point), Vec::new());
    assert_eq!(Ok(expected), output);
}

#[test_log::test(tokio::test)]
async fn test_compile_tree_construction_nested() {
    let storage = Arc::new(InMemoryTreeStorage::empty());
    let output = compile(
        r#"() => [["Hello, world!"]]"#,
        &TEST_SOURCE_NAMESPACE,
        &TEST_GENERATED_NAME_NAMESPACE,
        &*storage,
    )
    .await;
    let unused_name = Name::new(TEST_GENERATED_NAME_NAMESPACE, "".to_string());
    let entry_point = DeepExpression(Expression::make_lambda(
        unused_name,
        Arc::new(DeepExpression(Expression::make_construct_tree(vec![
            Arc::new(DeepExpression(Expression::make_construct_tree(vec![
                Arc::new(DeepExpression(Expression::make_literal(
                    HashedTree::from(Arc::new(Tree::from_string("Hello, world!").unwrap()))
                        .digest()
                        .clone(),
                ))),
            ]))),
        ]))),
    ));
    let expected = CompilerOutput::new(Some(entry_point), Vec::new());
    assert_eq!(Ok(expected), output);
}

#[test_log::test(tokio::test)]
async fn test_compile_extra_token() {
    let storage = Arc::new(InMemoryTreeStorage::empty());
    let output = compile(
        r#"(x) => x)"#,
        &TEST_SOURCE_NAMESPACE,
        &TEST_GENERATED_NAME_NAMESPACE,
        &*storage,
    )
    .await;
    let x_in_source = Name::new(TEST_SOURCE_NAMESPACE, "x".to_string());
    let x_in_output = Name::new(TEST_GENERATED_NAME_NAMESPACE, "x".to_string());
    let entry_point = DeepExpression(Expression::make_lambda(
        x_in_output,
        Arc::new(DeepExpression(Expression::ReadVariable(x_in_source))),
    ));
    let expected = CompilerOutput::new(
        Some(entry_point),
        vec![CompilerError::new(
            "Unexpected token after the entry point lambda".to_string(),
            SourceLocation::new(0, 8),
        )],
    );
    assert_eq!(Ok(expected), output);
}
