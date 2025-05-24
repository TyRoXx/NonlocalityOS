use crate::compilation::{compile, CompilerError, CompilerOutput, SourceLocation};
use astraea::tree::Tree;
use lambda::expressions::{DeepExpression, Expression};
use lambda::name::NamespaceId;
use std::sync::Arc;

const TEST_SOURCE_NAMESPACE: NamespaceId =
    NamespaceId([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]);

#[test_log::test(tokio::test)]
async fn test_compile_empty_source() {
    let output = compile("", &TEST_SOURCE_NAMESPACE);
    let expected = CompilerOutput::new(
        None,
        vec![CompilerError::new(
            "Parser error: Expected expression, got end of file.".to_string(),
            SourceLocation::new(0, 0),
        )],
    );
    assert_eq!(Ok(expected), output);
}

#[test_log::test(tokio::test)]
async fn test_compile_lambda() {
    let empty_tree = Arc::new(DeepExpression(Expression::make_construct_tree(vec![])));
    let output = compile(r#"(x) => x"#, &TEST_SOURCE_NAMESPACE);
    let entry_point = DeepExpression(Expression::make_lambda(
        empty_tree,
        Arc::new(DeepExpression(Expression::make_get_child(
            Arc::new(DeepExpression(Expression::make_argument())),
            0,
        ))),
    ));
    let expected = CompilerOutput::new(Some(entry_point), Vec::new());
    assert_eq!(Ok(expected), output);
}

#[test_log::test(tokio::test)]
async fn test_compile_multiple_parameters() {
    let empty_tree = Arc::new(DeepExpression(Expression::make_construct_tree(vec![])));
    let output = compile(r#"(x, y) => y"#, &TEST_SOURCE_NAMESPACE);
    let entry_point = DeepExpression(Expression::make_lambda(
        empty_tree,
        Arc::new(DeepExpression(Expression::make_get_child(
            Arc::new(DeepExpression(Expression::make_argument())),
            1,
        ))),
    ));
    let expected = CompilerOutput::new(Some(entry_point), Vec::new());
    assert_eq!(Ok(expected), output);
}

#[test_log::test(tokio::test)]
async fn test_compile_function_call() {
    let empty_tree = Arc::new(DeepExpression(Expression::make_construct_tree(vec![])));
    let output = compile(r#"(f) => f(f)"#, &TEST_SOURCE_NAMESPACE);
    let f = Arc::new(DeepExpression(Expression::make_get_child(
        Arc::new(DeepExpression(Expression::make_argument())),
        0,
    )));
    let entry_point = DeepExpression(Expression::make_lambda(
        empty_tree,
        Arc::new(DeepExpression(Expression::make_apply(f.clone(), f))),
    ));
    let expected = CompilerOutput::new(Some(entry_point), Vec::new());
    assert_eq!(Ok(expected), output);
}

#[test_log::test(tokio::test)]
async fn test_compile_quotes() {
    let empty_tree = Arc::new(DeepExpression(Expression::make_construct_tree(vec![])));
    let output = compile(
        r#"(print) => print("Hello, world!")"#,
        &TEST_SOURCE_NAMESPACE,
    );
    let print = Arc::new(DeepExpression(Expression::make_get_child(
        Arc::new(DeepExpression(Expression::make_argument())),
        0,
    )));
    let entry_point = DeepExpression(Expression::make_lambda(
        empty_tree,
        Arc::new(DeepExpression(Expression::make_apply(
            print.clone(),
            Arc::new(DeepExpression(Expression::make_literal(
                Tree::from_string("Hello, world!").unwrap(),
            ))),
        ))),
    ));
    let expected = CompilerOutput::new(Some(entry_point), Vec::new());
    assert_eq!(Ok(expected), output);
}

#[test_log::test(tokio::test)]
async fn test_compile_tree_construction_0_children() {
    let empty_tree = Arc::new(DeepExpression(Expression::make_construct_tree(vec![])));
    let output = compile(r#"() => []"#, &TEST_SOURCE_NAMESPACE);
    let entry_point = DeepExpression(Expression::make_lambda(
        empty_tree,
        Arc::new(DeepExpression(Expression::make_construct_tree(vec![]))),
    ));
    let expected = CompilerOutput::new(Some(entry_point), Vec::new());
    assert_eq!(Ok(expected), output);
}

#[test_log::test(tokio::test)]
async fn test_compile_tree_construction_1_child() {
    let empty_tree = Arc::new(DeepExpression(Expression::make_construct_tree(vec![])));
    let output = compile(r#"() => ["Hello, world!"]"#, &TEST_SOURCE_NAMESPACE);
    let entry_point = DeepExpression(Expression::make_lambda(
        empty_tree,
        Arc::new(DeepExpression(Expression::make_construct_tree(vec![
            Arc::new(DeepExpression(Expression::make_literal(
                Tree::from_string("Hello, world!").unwrap(),
            ))),
        ]))),
    ));
    let expected = CompilerOutput::new(Some(entry_point), Vec::new());
    assert_eq!(Ok(expected), output);
}

#[test_log::test(tokio::test)]
async fn test_compile_tree_construction_2_children() {
    let empty_tree = Arc::new(DeepExpression(Expression::make_construct_tree(vec![])));
    let output = compile(r#"() => ["Hello, ", "world!"]"#, &TEST_SOURCE_NAMESPACE);
    let entry_point = DeepExpression(Expression::make_lambda(
        empty_tree,
        Arc::new(DeepExpression(Expression::make_construct_tree(vec![
            Arc::new(DeepExpression(Expression::make_literal(
                Tree::from_string("Hello, ").unwrap(),
            ))),
            Arc::new(DeepExpression(Expression::make_literal(
                Tree::from_string("world!").unwrap(),
            ))),
        ]))),
    ));
    let expected = CompilerOutput::new(Some(entry_point), Vec::new());
    assert_eq!(Ok(expected), output);
}

#[test_log::test(tokio::test)]
async fn test_compile_tree_construction_nested() {
    let empty_tree = Arc::new(DeepExpression(Expression::make_construct_tree(vec![])));
    let output = compile(r#"() => [["Hello, world!"]]"#, &TEST_SOURCE_NAMESPACE);
    let entry_point = DeepExpression(Expression::make_lambda(
        empty_tree,
        Arc::new(DeepExpression(Expression::make_construct_tree(vec![
            Arc::new(DeepExpression(Expression::make_construct_tree(vec![
                Arc::new(DeepExpression(Expression::make_literal(
                    Tree::from_string("Hello, world!").unwrap(),
                ))),
            ]))),
        ]))),
    ));
    let expected = CompilerOutput::new(Some(entry_point), Vec::new());
    assert_eq!(Ok(expected), output);
}

#[test_log::test(tokio::test)]
async fn test_compile_extra_token() {
    let empty_tree = Arc::new(DeepExpression(Expression::make_construct_tree(vec![])));
    let output = compile(r#"(x) => x)"#, &TEST_SOURCE_NAMESPACE);
    let entry_point = DeepExpression(Expression::make_lambda(
        empty_tree,
        Arc::new(DeepExpression(Expression::make_get_child(
            Arc::new(DeepExpression(Expression::make_argument())),
            0,
        ))),
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

#[test_log::test(tokio::test)]
async fn test_compile_braces() {
    let empty_tree = Arc::new(DeepExpression(Expression::make_construct_tree(vec![])));
    let output = compile(r#"() => {[]}"#, &TEST_SOURCE_NAMESPACE);
    let entry_point = DeepExpression(Expression::make_lambda(
        empty_tree,
        Arc::new(DeepExpression(Expression::make_construct_tree(vec![]))),
    ));
    let expected = CompilerOutput::new(Some(entry_point), Vec::new());
    assert_eq!(Ok(expected), output);
}
