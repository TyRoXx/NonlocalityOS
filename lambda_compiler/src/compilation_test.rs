use crate::compilation::{compile, CompilerError, CompilerOutput, SourceLocation};
use crate::type_checking::{Type, TypedExpression};
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
    let entry_point = TypedExpression::new(
        DeepExpression(Expression::make_lambda(
            empty_tree,
            Arc::new(DeepExpression(Expression::make_argument())),
        )),
        Type::Function {
            parameters: vec![Type::Any],
            return_type: Box::new(Type::Any),
        },
    );
    let expected = CompilerOutput::new(Some(entry_point), Vec::new());
    assert_eq!(Ok(expected), output);
}

#[test_log::test(tokio::test)]
async fn test_compile_multiple_parameters() {
    let empty_tree = Arc::new(DeepExpression(Expression::make_construct_tree(vec![])));
    let output = compile(r#"(x, y) => y"#, &TEST_SOURCE_NAMESPACE);
    let entry_point = TypedExpression::new(
        DeepExpression(Expression::make_lambda(
            empty_tree,
            Arc::new(DeepExpression(Expression::make_get_child(
                Arc::new(DeepExpression(Expression::make_argument())),
                1,
            ))),
        )),
        Type::Function {
            parameters: vec![Type::Any, Type::Any],
            return_type: Box::new(Type::Any),
        },
    );
    let expected = CompilerOutput::new(Some(entry_point), Vec::new());
    assert_eq!(Ok(expected), output);
}

#[test_log::test(tokio::test)]
async fn test_compile_function_call() {
    let empty_tree = Arc::new(DeepExpression(Expression::make_construct_tree(vec![])));
    let output = compile(r#"(f) => {(g) => g}(f)"#, &TEST_SOURCE_NAMESPACE);
    let f = Arc::new(DeepExpression(Expression::make_argument()));
    let g = Arc::new(DeepExpression(Expression::make_argument()));
    let entry_point = TypedExpression::new(
        DeepExpression(Expression::make_lambda(
            empty_tree.clone(),
            Arc::new(DeepExpression(Expression::make_apply(
                Arc::new(DeepExpression(Expression::make_lambda(empty_tree, g))),
                f,
            ))),
        )),
        Type::Function {
            parameters: vec![Type::Any],
            return_type: Box::new(Type::Any),
        },
    );
    let expected = CompilerOutput::new(Some(entry_point), Vec::new());
    assert_eq!(Ok(expected), output);
}

#[test_log::test(tokio::test)]
async fn test_compile_quotes() {
    let empty_tree = Arc::new(DeepExpression(Expression::make_construct_tree(vec![])));
    let output = compile(r#"() => "Hello, world!""#, &TEST_SOURCE_NAMESPACE);
    let entry_point = TypedExpression::new(
        DeepExpression(Expression::make_lambda(
            empty_tree,
            Arc::new(DeepExpression(Expression::make_literal(
                Tree::from_string("Hello, world!").unwrap(),
            ))),
        )),
        Type::Function {
            parameters: vec![],
            return_type: Box::new(Type::String),
        },
    );
    let expected = CompilerOutput::new(Some(entry_point), Vec::new());
    assert_eq!(Ok(expected), output);
}

#[test_log::test(tokio::test)]
async fn test_compile_callee_is_not_a_function() {
    let output = compile(
        r#"(print) => print("Hello, world!")"#,
        &TEST_SOURCE_NAMESPACE,
    );
    let expected = CompilerOutput::new(
        None,
        vec![CompilerError::new(
            "Callee is not a function".to_string(),
            SourceLocation::new(0, 11),
        )],
    );
    assert_eq!(Ok(expected), output);
}

#[test_log::test(tokio::test)]
async fn test_compile_tree_construction_0_children() {
    let empty_tree = Arc::new(DeepExpression(Expression::make_construct_tree(vec![])));
    let output = compile(r#"() => []"#, &TEST_SOURCE_NAMESPACE);
    let entry_point = TypedExpression::new(
        DeepExpression(Expression::make_lambda(
            empty_tree,
            Arc::new(DeepExpression(Expression::make_construct_tree(vec![]))),
        )),
        Type::Function {
            parameters: vec![],
            return_type: Box::new(Type::TreeWithKnownChildTypes(vec![])),
        },
    );
    let expected = CompilerOutput::new(Some(entry_point), Vec::new());
    assert_eq!(Ok(expected), output);
}

#[test_log::test(tokio::test)]
async fn test_compile_tree_construction_1_child() {
    let empty_tree = Arc::new(DeepExpression(Expression::make_construct_tree(vec![])));
    let output = compile(r#"() => ["Hello, world!"]"#, &TEST_SOURCE_NAMESPACE);
    let entry_point = TypedExpression::new(
        DeepExpression(Expression::make_lambda(
            empty_tree,
            Arc::new(DeepExpression(Expression::make_construct_tree(vec![
                Arc::new(DeepExpression(Expression::make_literal(
                    Tree::from_string("Hello, world!").unwrap(),
                ))),
            ]))),
        )),
        Type::Function {
            parameters: vec![],
            return_type: Box::new(Type::TreeWithKnownChildTypes(vec![Type::String])),
        },
    );
    let expected = CompilerOutput::new(Some(entry_point), Vec::new());
    assert_eq!(Ok(expected), output);
}

#[test_log::test(tokio::test)]
async fn test_compile_tree_construction_2_children() {
    let empty_tree = Arc::new(DeepExpression(Expression::make_construct_tree(vec![])));
    let output = compile(r#"() => ["Hello, ", "world!"]"#, &TEST_SOURCE_NAMESPACE);
    let entry_point = TypedExpression::new(
        DeepExpression(Expression::make_lambda(
            empty_tree,
            Arc::new(DeepExpression(Expression::make_construct_tree(vec![
                Arc::new(DeepExpression(Expression::make_literal(
                    Tree::from_string("Hello, ").unwrap(),
                ))),
                Arc::new(DeepExpression(Expression::make_literal(
                    Tree::from_string("world!").unwrap(),
                ))),
            ]))),
        )),
        Type::Function {
            parameters: vec![],
            return_type: Box::new(Type::TreeWithKnownChildTypes(vec![
                Type::String,
                Type::String,
            ])),
        },
    );
    let expected = CompilerOutput::new(Some(entry_point), Vec::new());
    assert_eq!(Ok(expected), output);
}

#[test_log::test(tokio::test)]
async fn test_compile_tree_construction_nested() {
    let empty_tree = Arc::new(DeepExpression(Expression::make_construct_tree(vec![])));
    let output = compile(r#"() => [["Hello, world!"]]"#, &TEST_SOURCE_NAMESPACE);
    let entry_point = TypedExpression::new(
        DeepExpression(Expression::make_lambda(
            empty_tree,
            Arc::new(DeepExpression(Expression::make_construct_tree(vec![
                Arc::new(DeepExpression(Expression::make_construct_tree(vec![
                    Arc::new(DeepExpression(Expression::make_literal(
                        Tree::from_string("Hello, world!").unwrap(),
                    ))),
                ]))),
            ]))),
        )),
        Type::Function {
            parameters: vec![],
            return_type: Box::new(Type::TreeWithKnownChildTypes(vec![
                Type::TreeWithKnownChildTypes(vec![Type::String]),
            ])),
        },
    );
    let expected = CompilerOutput::new(Some(entry_point), Vec::new());
    assert_eq!(Ok(expected), output);
}

#[test_log::test(tokio::test)]
async fn test_compile_extra_token() {
    let empty_tree = Arc::new(DeepExpression(Expression::make_construct_tree(vec![])));
    let output = compile(r#"(x) => x)"#, &TEST_SOURCE_NAMESPACE);
    let entry_point = TypedExpression::new(
        DeepExpression(Expression::make_lambda(
            empty_tree,
            Arc::new(DeepExpression(Expression::make_argument())),
        )),
        Type::Function {
            parameters: vec![Type::Any],
            return_type: Box::new(Type::Any),
        },
    );
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
    let entry_point = TypedExpression::new(
        DeepExpression(Expression::make_lambda(
            empty_tree,
            Arc::new(DeepExpression(Expression::make_construct_tree(vec![]))),
        )),
        Type::Function {
            parameters: vec![],
            return_type: Box::new(Type::TreeWithKnownChildTypes(vec![])),
        },
    );
    let expected = CompilerOutput::new(Some(entry_point), Vec::new());
    assert_eq!(Ok(expected), output);
}

#[test_log::test(tokio::test)]
async fn test_redundant_captures_minimal() {
    // We reference the same outer variable multiple times in a lambda. The environment should only capture the variable once.
    let empty_tree = Arc::new(DeepExpression(Expression::make_construct_tree(vec![])));
    let output = compile("(a) => () => [a, a]", &TEST_SOURCE_NAMESPACE);
    let entry_point = TypedExpression::new(
        DeepExpression(Expression::make_lambda(
            empty_tree,
            Arc::new(DeepExpression(Expression::make_lambda(
                Arc::new(DeepExpression(Expression::make_construct_tree(vec![
                    Arc::new(DeepExpression(Expression::make_argument())),
                ]))),
                Arc::new(DeepExpression(Expression::make_construct_tree(vec![
                    Arc::new(DeepExpression(Expression::make_get_child(
                        Arc::new(DeepExpression(Expression::make_environment())),
                        0,
                    ))),
                    Arc::new(DeepExpression(Expression::make_get_child(
                        Arc::new(DeepExpression(Expression::make_environment())),
                        0,
                    ))),
                ]))),
            ))),
        )),
        Type::Function {
            parameters: vec![Type::Any],
            return_type: Box::new(Type::Function {
                parameters: vec![],
                return_type: Box::new(Type::TreeWithKnownChildTypes(vec![Type::Any, Type::Any])),
            }),
        },
    );
    let expected = CompilerOutput::new(Some(entry_point), Vec::new());
    assert_eq!(Ok(expected), output);
}

#[test_log::test(tokio::test)]
async fn test_redundant_captures_complex() {
    // We reference the same outer variable multiple times in a lambda. The environment should only capture the variable once.
    let empty_tree = Arc::new(DeepExpression(Expression::make_construct_tree(vec![])));
    let output = compile(
        "(a) => (b) => () => [a, a, a, a, b, b, b, b]",
        &TEST_SOURCE_NAMESPACE,
    );
    let entry_point = TypedExpression::new(
        DeepExpression(Expression::make_lambda(
            empty_tree,
            Arc::new(DeepExpression(Expression::make_lambda(
                Arc::new(DeepExpression(Expression::make_construct_tree(vec![
                    Arc::new(DeepExpression(Expression::make_argument())),
                ]))),
                Arc::new(DeepExpression(Expression::make_lambda(
                    Arc::new(DeepExpression(Expression::make_construct_tree(vec![
                        Arc::new(DeepExpression(Expression::make_get_child(
                            Arc::new(DeepExpression(Expression::make_environment())),
                            0,
                        ))),
                        Arc::new(DeepExpression(Expression::make_argument())),
                    ]))),
                    Arc::new(DeepExpression(Expression::make_construct_tree(vec![
                        Arc::new(DeepExpression(Expression::make_get_child(
                            Arc::new(DeepExpression(Expression::make_environment())),
                            0,
                        ))),
                        Arc::new(DeepExpression(Expression::make_get_child(
                            Arc::new(DeepExpression(Expression::make_environment())),
                            0,
                        ))),
                        Arc::new(DeepExpression(Expression::make_get_child(
                            Arc::new(DeepExpression(Expression::make_environment())),
                            0,
                        ))),
                        Arc::new(DeepExpression(Expression::make_get_child(
                            Arc::new(DeepExpression(Expression::make_environment())),
                            0,
                        ))),
                        Arc::new(DeepExpression(Expression::make_get_child(
                            Arc::new(DeepExpression(Expression::make_environment())),
                            1,
                        ))),
                        Arc::new(DeepExpression(Expression::make_get_child(
                            Arc::new(DeepExpression(Expression::make_environment())),
                            1,
                        ))),
                        Arc::new(DeepExpression(Expression::make_get_child(
                            Arc::new(DeepExpression(Expression::make_environment())),
                            1,
                        ))),
                        Arc::new(DeepExpression(Expression::make_get_child(
                            Arc::new(DeepExpression(Expression::make_environment())),
                            1,
                        ))),
                    ]))),
                ))),
            ))),
        )),
        Type::Function {
            parameters: vec![Type::Any],
            return_type: Box::new(Type::Function {
                parameters: vec![Type::Any],
                return_type: Box::new(Type::Function {
                    parameters: vec![],
                    return_type: Box::new(Type::TreeWithKnownChildTypes(vec![
                        Type::Any,
                        Type::Any,
                        Type::Any,
                        Type::Any,
                        Type::Any,
                        Type::Any,
                        Type::Any,
                        Type::Any,
                    ])),
                }),
            }),
        },
    );
    let expected = CompilerOutput::new(Some(entry_point), Vec::new());
    assert_eq!(Ok(expected), output);
}
