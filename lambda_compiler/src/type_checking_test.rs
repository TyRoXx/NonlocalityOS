use crate::{
    ast,
    compilation::{CompilerOutput, SourceLocation},
    type_checking::{check_types, EnvironmentBuilder},
};
use lambda::{
    expressions::{DeepExpression, Expression},
    name::{Name, NamespaceId},
};
use std::sync::Arc;

const TEST_SOURCE_NAMESPACE: NamespaceId =
    NamespaceId([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]);

#[test_log::test(tokio::test)]
async fn test_check_types_lambda_0_parameters() {
    let input = ast::Expression::Lambda {
        parameter_names: vec![],
        body: Box::new(ast::Expression::ConstructTree(vec![])),
    };
    let empty_tree = Arc::new(DeepExpression(Expression::make_construct_tree(vec![])));
    let mut environment_builder = EnvironmentBuilder::new();
    let output = check_types(&input, &mut environment_builder);
    let expected = CompilerOutput::new(
        Some(DeepExpression(lambda::expressions::Expression::Lambda {
            environment: empty_tree,
            body: Arc::new(DeepExpression(
                lambda::expressions::Expression::make_construct_tree(vec![]),
            )),
        })),
        Vec::new(),
    );
    assert_eq!(output, Ok(expected));
}

#[test_log::test(tokio::test)]
async fn test_check_types_lambda_1_parameter() {
    let x_in_source = Name::new(TEST_SOURCE_NAMESPACE, "x".to_string());
    let input = ast::Expression::Lambda {
        parameter_names: vec![x_in_source.clone()],
        body: Box::new(ast::Expression::Identifier(
            x_in_source.clone(),
            SourceLocation { line: 0, column: 1 },
        )),
    };
    let empty_tree = Arc::new(DeepExpression(Expression::make_construct_tree(vec![])));
    let mut environment_builder = EnvironmentBuilder::new();
    let output = check_types(&input, &mut environment_builder);
    let expected = CompilerOutput::new(
        Some(DeepExpression(lambda::expressions::Expression::Lambda {
            environment: empty_tree,
            body: Arc::new(DeepExpression(
                lambda::expressions::Expression::make_get_child(
                    Arc::new(DeepExpression(
                        lambda::expressions::Expression::make_argument(),
                    )),
                    0,
                ),
            )),
        })),
        Vec::new(),
    );
    assert_eq!(output, Ok(expected));
}

#[test_log::test(tokio::test)]
async fn test_check_types_lambda_2_parameters() {
    let x_in_source = Name::new(TEST_SOURCE_NAMESPACE, "x".to_string());
    let y_in_source = Name::new(TEST_SOURCE_NAMESPACE, "y".to_string());
    let input = ast::Expression::Lambda {
        parameter_names: vec![x_in_source.clone(), y_in_source.clone()],
        body: Box::new(ast::Expression::Identifier(
            x_in_source.clone(),
            SourceLocation {
                line: 0,
                column: 10,
            },
        )),
    };
    let empty_tree = Arc::new(DeepExpression(Expression::make_construct_tree(vec![])));
    let mut environment_builder = EnvironmentBuilder::new();
    let output = check_types(&input, &mut environment_builder);
    let expected = CompilerOutput::new(
        Some(DeepExpression(lambda::expressions::Expression::Lambda {
            environment: empty_tree,
            body: Arc::new(DeepExpression(
                lambda::expressions::Expression::make_get_child(
                    Arc::new(DeepExpression(
                        lambda::expressions::Expression::make_argument(),
                    )),
                    0,
                ),
            )),
        })),
        Vec::new(),
    );
    assert_eq!(output, Ok(expected));
}

#[test_log::test(tokio::test)]
async fn test_check_types_lambda_capture_outer_argument() {
    let x_in_source = Name::new(TEST_SOURCE_NAMESPACE, "x".to_string());
    let input = ast::Expression::Lambda {
        parameter_names: vec![x_in_source.clone()],
        body: Box::new(ast::Expression::Lambda {
            parameter_names: vec![],
            body: Box::new(ast::Expression::Identifier(
                x_in_source.clone(),
                SourceLocation {
                    line: 0,
                    column: 10,
                },
            )),
        }),
    };
    let empty_tree = Arc::new(DeepExpression(Expression::make_construct_tree(vec![])));
    let mut environment_builder = EnvironmentBuilder::new();
    let output = check_types(&input, &mut environment_builder);
    let expected = CompilerOutput::new(
        Some(DeepExpression(lambda::expressions::Expression::Lambda {
            environment: empty_tree,
            body: Arc::new(DeepExpression(lambda::expressions::Expression::Lambda {
                environment: Arc::new(DeepExpression(
                    lambda::expressions::Expression::make_construct_tree(vec![Arc::new(
                        DeepExpression(lambda::expressions::Expression::make_get_child(
                            Arc::new(DeepExpression(
                                lambda::expressions::Expression::make_argument(),
                            )),
                            0,
                        )),
                    )]),
                )),
                body: Arc::new(DeepExpression(
                    lambda::expressions::Expression::make_get_child(
                        Arc::new(DeepExpression(
                            lambda::expressions::Expression::make_environment(),
                        )),
                        0,
                    ),
                )),
            })),
        })),
        Vec::new(),
    );
    assert_eq!(output, Ok(expected));
}

#[test_log::test(tokio::test)]
async fn test_check_types_lambda_capture_multiple_variables() {
    let x_in_source = Name::new(TEST_SOURCE_NAMESPACE, "x".to_string());
    let y_in_source = Name::new(TEST_SOURCE_NAMESPACE, "y".to_string());
    let input = ast::Expression::Lambda {
        parameter_names: vec![x_in_source.clone(), y_in_source.clone()],
        body: Box::new(ast::Expression::Lambda {
            parameter_names: vec![],
            body: Box::new(ast::Expression::ConstructTree(vec![
                ast::Expression::Identifier(
                    x_in_source.clone(),
                    SourceLocation {
                        line: 0,
                        column: 10,
                    },
                ),
                ast::Expression::Identifier(
                    y_in_source.clone(),
                    SourceLocation {
                        line: 0,
                        column: 13,
                    },
                ),
            ])),
        }),
    };
    let empty_tree = Arc::new(DeepExpression(Expression::make_construct_tree(vec![])));
    let mut environment_builder = EnvironmentBuilder::new();
    let output = check_types(&input, &mut environment_builder);
    let expected = CompilerOutput::new(
        Some(DeepExpression(lambda::expressions::Expression::Lambda {
            environment: empty_tree,
            body: Arc::new(DeepExpression(lambda::expressions::Expression::Lambda {
                environment: Arc::new(DeepExpression(
                    lambda::expressions::Expression::make_construct_tree(vec![
                        Arc::new(DeepExpression(
                            lambda::expressions::Expression::make_get_child(
                                Arc::new(DeepExpression(
                                    lambda::expressions::Expression::make_argument(),
                                )),
                                0,
                            ),
                        )),
                        Arc::new(DeepExpression(
                            lambda::expressions::Expression::make_get_child(
                                Arc::new(DeepExpression(
                                    lambda::expressions::Expression::make_argument(),
                                )),
                                1,
                            ),
                        )),
                    ]),
                )),
                body: Arc::new(DeepExpression(
                    lambda::expressions::Expression::make_construct_tree(vec![
                        Arc::new(DeepExpression(
                            lambda::expressions::Expression::make_get_child(
                                Arc::new(DeepExpression(
                                    lambda::expressions::Expression::make_environment(),
                                )),
                                0,
                            ),
                        )),
                        Arc::new(DeepExpression(
                            lambda::expressions::Expression::make_get_child(
                                Arc::new(DeepExpression(
                                    lambda::expressions::Expression::make_environment(),
                                )),
                                1,
                            ),
                        )),
                    ]),
                )),
            })),
        })),
        Vec::new(),
    );
    assert_eq!(output, Ok(expected));
}

#[test_log::test(tokio::test)]
async fn test_check_types_lambda_capture_multiple_layers() {
    let x_in_source = Name::new(TEST_SOURCE_NAMESPACE, "x".to_string());
    let y_in_source = Name::new(TEST_SOURCE_NAMESPACE, "y".to_string());
    let input = ast::Expression::Lambda {
        parameter_names: vec![x_in_source.clone()],
        body: Box::new(ast::Expression::Lambda {
            parameter_names: vec![y_in_source.clone()],
            body: Box::new(ast::Expression::Lambda {
                parameter_names: vec![],
                body: Box::new(ast::Expression::ConstructTree(vec![
                    ast::Expression::Identifier(
                        x_in_source.clone(),
                        SourceLocation {
                            line: 0,
                            column: 10,
                        },
                    ),
                    ast::Expression::Identifier(
                        y_in_source.clone(),
                        SourceLocation {
                            line: 0,
                            column: 13,
                        },
                    ),
                ])),
            }),
        }),
    };
    let empty_tree = Arc::new(DeepExpression(Expression::make_construct_tree(vec![])));
    let mut environment_builder = EnvironmentBuilder::new();
    let output = check_types(&input, &mut environment_builder);
    let expected = CompilerOutput::new(
        Some(DeepExpression(lambda::expressions::Expression::Lambda {
            environment: empty_tree,
            body: Arc::new(DeepExpression(lambda::expressions::Expression::Lambda {
                environment: Arc::new(DeepExpression(
                    lambda::expressions::Expression::make_construct_tree(vec![
                        // x
                        Arc::new(DeepExpression(
                            lambda::expressions::Expression::make_get_child(
                                Arc::new(DeepExpression(
                                    lambda::expressions::Expression::make_argument(),
                                )),
                                0,
                            ),
                        )),
                    ]),
                )),
                body: Arc::new(DeepExpression(lambda::expressions::Expression::Lambda {
                    environment: Arc::new(DeepExpression(
                        lambda::expressions::Expression::make_construct_tree(vec![
                            // x
                            Arc::new(DeepExpression(
                                lambda::expressions::Expression::make_get_child(
                                    Arc::new(DeepExpression(
                                        lambda::expressions::Expression::make_environment(),
                                    )),
                                    0,
                                ),
                            )),
                            // y
                            Arc::new(DeepExpression(
                                lambda::expressions::Expression::make_get_child(
                                    Arc::new(DeepExpression(
                                        lambda::expressions::Expression::make_argument(),
                                    )),
                                    0,
                                ),
                            )),
                        ]),
                    )),
                    body: Arc::new(DeepExpression(
                        lambda::expressions::Expression::make_construct_tree(vec![
                            // x
                            Arc::new(DeepExpression(
                                lambda::expressions::Expression::make_get_child(
                                    Arc::new(DeepExpression(
                                        lambda::expressions::Expression::make_environment(),
                                    )),
                                    0,
                                ),
                            )),
                            // y
                            Arc::new(DeepExpression(
                                lambda::expressions::Expression::make_get_child(
                                    Arc::new(DeepExpression(
                                        lambda::expressions::Expression::make_environment(),
                                    )),
                                    1,
                                ),
                            )),
                        ]),
                    )),
                })),
            })),
        })),
        Vec::new(),
    );
    assert_eq!(output, Ok(expected));
}

#[test_log::test(tokio::test)]
async fn test_unknown_identifier() {
    let x_in_source = Name::new(TEST_SOURCE_NAMESPACE, "x".to_string());
    let input = ast::Expression::Identifier(
        x_in_source.clone(),
        SourceLocation {
            line: 2,
            column: 10,
        },
    );
    let mut environment_builder = EnvironmentBuilder::new();
    let output = check_types(&input, &mut environment_builder);
    let expected = CompilerOutput::new(
        None,
        vec![crate::compilation::CompilerError::new(
            format!("Identifier {x_in_source} not found"),
            SourceLocation {
                line: 2,
                column: 10,
            },
        )],
    );
    assert_eq!(output, Ok(expected));
}

#[test_log::test(tokio::test)]
async fn test_lambda_parameter_scoping() {
    let x_in_source = Name::new(TEST_SOURCE_NAMESPACE, "x".to_string());
    let input = ast::Expression::Apply {
        callee: Box::new(ast::Expression::Lambda {
            parameter_names: vec![x_in_source.clone()],
            body: Box::new(ast::Expression::Identifier(
                x_in_source.clone(),
                SourceLocation { line: 0, column: 1 },
            )),
        }),
        arguments: vec![ast::Expression::Identifier(
            // variable doesn't exist here anymore
            x_in_source.clone(),
            SourceLocation {
                line: 2,
                column: 10,
            },
        )],
    };
    let mut environment_builder = EnvironmentBuilder::new();
    let output = check_types(&input, &mut environment_builder);
    let expected = CompilerOutput::new(
        None,
        vec![crate::compilation::CompilerError::new(
            format!("Identifier {x_in_source} not found"),
            SourceLocation {
                line: 2,
                column: 10,
            },
        )],
    );
    assert_eq!(output, Ok(expected));
}
