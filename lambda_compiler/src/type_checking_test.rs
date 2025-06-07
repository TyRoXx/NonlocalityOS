use crate::{
    ast::{self, LambdaParameter},
    compilation::{CompilerOutput, SourceLocation},
    type_checking::{check_types_with_default_globals, DeepType, GenericType, TypedExpression},
};
use astraea::deep_tree::DeepTree;
use lambda::{
    expressions::{evaluate, DeepExpression, Expression},
    name::{Name, NamespaceId},
};
use std::sync::Arc;

const TEST_SOURCE_NAMESPACE: NamespaceId =
    NamespaceId([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]);

async fn expect_evaluate_result(entry_point: &DeepExpression, expected_result: &DeepTree) {
    let storage = astraea::storage::InMemoryTreeStorage::empty();
    let evaluate_result = DeepTree::deserialize(
        &evaluate(entry_point, &storage, &storage, &None, &None)
            .await
            .unwrap(),
        &storage,
    )
    .await
    .unwrap();
    assert_eq!(expected_result, &evaluate_result);
}

#[test_log::test(tokio::test)]
async fn test_check_types_lambda_0_parameters() {
    let input = ast::Expression::Lambda {
        parameters: vec![],
        body: Box::new(ast::Expression::ConstructTree(vec![])),
    };
    let empty_tree = Arc::new(DeepExpression(Expression::make_construct_tree(vec![])));
    let output = check_types_with_default_globals(&input, TEST_SOURCE_NAMESPACE).await;
    let expected = CompilerOutput::new(
        Some(TypedExpression::new(
            DeepExpression(lambda::expressions::Expression::Lambda {
                environment: empty_tree,
                body: Arc::new(DeepExpression(
                    lambda::expressions::Expression::make_construct_tree(vec![]),
                )),
            }),
            DeepType(GenericType::Function {
                parameters: vec![],
                return_type: Box::new(DeepType(GenericType::TreeWithKnownChildTypes(vec![]))),
            }),
        )),
        Vec::new(),
    );
    assert_eq!(output, Ok(expected));
}

#[test_log::test(tokio::test)]
async fn test_check_types_lambda_1_parameter() {
    let x_in_source = Name::new(TEST_SOURCE_NAMESPACE, "x".to_string());
    let input = ast::Expression::Lambda {
        parameters: vec![LambdaParameter::new(
            x_in_source.clone(),
            SourceLocation { line: 0, column: 1 },
            None,
        )],
        body: Box::new(ast::Expression::Identifier(
            x_in_source.clone(),
            SourceLocation {
                line: 0,
                column: 10,
            },
        )),
    };
    let empty_tree = Arc::new(DeepExpression(Expression::make_construct_tree(vec![])));
    let output = check_types_with_default_globals(&input, TEST_SOURCE_NAMESPACE).await;
    let expected = CompilerOutput::new(
        Some(TypedExpression::new(
            DeepExpression(lambda::expressions::Expression::Lambda {
                environment: empty_tree,
                body: Arc::new(DeepExpression(
                    lambda::expressions::Expression::make_argument(),
                )),
            }),
            DeepType(GenericType::Function {
                parameters: vec![DeepType(GenericType::Any)],
                return_type: Box::new(DeepType(GenericType::Any)),
            }),
        )),
        Vec::new(),
    );
    assert_eq!(output, Ok(expected));
}

#[test_log::test(tokio::test)]
async fn test_check_types_lambda_2_parameters() {
    let x_in_source = Name::new(TEST_SOURCE_NAMESPACE, "x".to_string());
    let y_in_source = Name::new(TEST_SOURCE_NAMESPACE, "y".to_string());
    let input = ast::Expression::Lambda {
        parameters: vec![
            LambdaParameter::new(
                x_in_source.clone(),
                SourceLocation { line: 0, column: 1 },
                None,
            ),
            LambdaParameter::new(
                y_in_source.clone(),
                SourceLocation { line: 0, column: 5 },
                None,
            ),
        ],
        body: Box::new(ast::Expression::Identifier(
            x_in_source.clone(),
            SourceLocation {
                line: 0,
                column: 10,
            },
        )),
    };
    let empty_tree = Arc::new(DeepExpression(Expression::make_construct_tree(vec![])));
    let output = check_types_with_default_globals(&input, TEST_SOURCE_NAMESPACE).await;
    let expected = CompilerOutput::new(
        Some(TypedExpression::new(
            DeepExpression(lambda::expressions::Expression::Lambda {
                environment: empty_tree,
                body: Arc::new(DeepExpression(
                    lambda::expressions::Expression::make_get_child(
                        Arc::new(DeepExpression(
                            lambda::expressions::Expression::make_argument(),
                        )),
                        0,
                    ),
                )),
            }),
            DeepType(GenericType::Function {
                parameters: vec![DeepType(GenericType::Any), DeepType(GenericType::Any)],
                return_type: Box::new(DeepType(GenericType::Any)),
            }),
        )),
        Vec::new(),
    );
    assert_eq!(output, Ok(expected));
}

#[test_log::test(tokio::test)]
async fn test_check_types_lambda_capture_outer_argument() {
    let x_in_source = Name::new(TEST_SOURCE_NAMESPACE, "x".to_string());
    let input = ast::Expression::Lambda {
        parameters: vec![LambdaParameter::new(
            x_in_source.clone(),
            SourceLocation { line: 0, column: 1 },
            None,
        )],
        body: Box::new(ast::Expression::Lambda {
            parameters: vec![],
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
    let output = check_types_with_default_globals(&input, TEST_SOURCE_NAMESPACE).await;
    let expected = CompilerOutput::new(
        Some(TypedExpression::new(
            DeepExpression(lambda::expressions::Expression::Lambda {
                environment: empty_tree,
                body: Arc::new(DeepExpression(lambda::expressions::Expression::Lambda {
                    environment: Arc::new(DeepExpression(
                        lambda::expressions::Expression::make_construct_tree(vec![Arc::new(
                            DeepExpression(lambda::expressions::Expression::make_argument()),
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
            }),
            DeepType(GenericType::Function {
                parameters: vec![DeepType(GenericType::Any)],
                return_type: Box::new(DeepType(GenericType::Function {
                    parameters: vec![],
                    return_type: Box::new(DeepType(GenericType::Any)),
                })),
            }),
        )),
        Vec::new(),
    );
    assert_eq!(output, Ok(expected));
}

#[test_log::test(tokio::test)]
async fn test_check_types_lambda_capture_multiple_variables() {
    let x_in_source = Name::new(TEST_SOURCE_NAMESPACE, "x".to_string());
    let y_in_source = Name::new(TEST_SOURCE_NAMESPACE, "y".to_string());
    let input = ast::Expression::Lambda {
        parameters: vec![
            LambdaParameter::new(
                x_in_source.clone(),
                SourceLocation { line: 0, column: 1 },
                None,
            ),
            LambdaParameter::new(
                y_in_source.clone(),
                SourceLocation { line: 0, column: 5 },
                None,
            ),
        ],
        body: Box::new(ast::Expression::Lambda {
            parameters: vec![],
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
    let output = check_types_with_default_globals(&input, TEST_SOURCE_NAMESPACE).await;
    let expected = CompilerOutput::new(
        Some(TypedExpression::new(
            DeepExpression(lambda::expressions::Expression::Lambda {
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
            }),
            DeepType(GenericType::Function {
                parameters: vec![DeepType(GenericType::Any), DeepType(GenericType::Any)],
                return_type: Box::new(DeepType(GenericType::Function {
                    parameters: vec![],
                    return_type: Box::new(DeepType(GenericType::TreeWithKnownChildTypes(vec![
                        DeepType(GenericType::Any),
                        DeepType(GenericType::Any),
                    ]))),
                })),
            }),
        )),
        Vec::new(),
    );
    assert_eq!(output, Ok(expected));
}

#[test_log::test(tokio::test)]
async fn test_check_types_lambda_capture_multiple_layers() {
    let x_in_source = Name::new(TEST_SOURCE_NAMESPACE, "x".to_string());
    let y_in_source = Name::new(TEST_SOURCE_NAMESPACE, "y".to_string());
    let input = ast::Expression::Lambda {
        parameters: vec![LambdaParameter::new(
            x_in_source.clone(),
            SourceLocation { line: 0, column: 1 },
            None,
        )],
        body: Box::new(ast::Expression::Lambda {
            parameters: vec![LambdaParameter::new(
                y_in_source.clone(),
                SourceLocation { line: 0, column: 5 },
                None,
            )],
            body: Box::new(ast::Expression::Lambda {
                parameters: vec![],
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
    let output = check_types_with_default_globals(&input, TEST_SOURCE_NAMESPACE).await;
    let expected = CompilerOutput::new(
        Some(TypedExpression::new(
            DeepExpression(lambda::expressions::Expression::Lambda {
                environment: empty_tree,
                body: Arc::new(DeepExpression(lambda::expressions::Expression::Lambda {
                    environment: Arc::new(DeepExpression(
                        lambda::expressions::Expression::make_construct_tree(vec![
                            // x
                            Arc::new(DeepExpression(
                                lambda::expressions::Expression::make_argument(),
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
                                    lambda::expressions::Expression::make_argument(),
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
            }),
            DeepType(GenericType::Function {
                parameters: vec![DeepType(GenericType::Any)],
                return_type: Box::new(DeepType(GenericType::Function {
                    parameters: vec![DeepType(GenericType::Any)],
                    return_type: Box::new(DeepType(GenericType::Function {
                        parameters: vec![],
                        return_type: Box::new(DeepType(GenericType::TreeWithKnownChildTypes(
                            vec![DeepType(GenericType::Any), DeepType(GenericType::Any)],
                        ))),
                    })),
                })),
            }),
        )),
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
    let output = check_types_with_default_globals(&input, TEST_SOURCE_NAMESPACE).await;
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
            parameters: vec![LambdaParameter::new(
                x_in_source.clone(),
                SourceLocation { line: 0, column: 1 },
                None,
            )],
            body: Box::new(ast::Expression::Identifier(
                x_in_source.clone(),
                SourceLocation { line: 0, column: 8 },
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
    let output = check_types_with_default_globals(&input, TEST_SOURCE_NAMESPACE).await;
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
async fn test_lambda_parameter_type() {
    let x_in_source = Name::new(TEST_SOURCE_NAMESPACE, "x".to_string());
    let string_in_source = Name::new(TEST_SOURCE_NAMESPACE, "String".to_string());
    let input = ast::Expression::Lambda {
        parameters: vec![LambdaParameter::new(
            x_in_source.clone(),
            SourceLocation { line: 0, column: 1 },
            Some(ast::Expression::Identifier(
                string_in_source.clone(),
                SourceLocation { line: 0, column: 3 },
            )),
        )],
        body: Box::new(ast::Expression::Identifier(
            x_in_source.clone(),
            SourceLocation { line: 1, column: 8 },
        )),
    };
    let output = check_types_with_default_globals(&input, TEST_SOURCE_NAMESPACE).await;
    let empty_tree = Arc::new(DeepExpression(Expression::make_construct_tree(vec![])));
    let expected = CompilerOutput::new(
        Some(TypedExpression::new(
            DeepExpression(lambda::expressions::Expression::Lambda {
                environment: empty_tree,
                body: Arc::new(DeepExpression(
                    lambda::expressions::Expression::make_argument(),
                )),
            }),
            DeepType(GenericType::Function {
                parameters: vec![DeepType(GenericType::String)],
                return_type: Box::new(DeepType(GenericType::String)),
            }),
        )),
        vec![],
    );
    assert_eq!(output, Ok(expected));
}

#[test_log::test(tokio::test)]
async fn test_lambda_parameter_type_has_error() {
    let x_in_source = Name::new(TEST_SOURCE_NAMESPACE, "x".to_string());
    let input = ast::Expression::Lambda {
        parameters: vec![LambdaParameter::new(
            x_in_source.clone(),
            SourceLocation { line: 1, column: 1 },
            Some(ast::Expression::Identifier(
                // the identifier doesn't exist yet
                x_in_source.clone(),
                SourceLocation { line: 2, column: 2 },
            )),
        )],
        body: Box::new(ast::Expression::Identifier(
            x_in_source.clone(),
            SourceLocation { line: 3, column: 3 },
        )),
    };
    let output = check_types_with_default_globals(&input, TEST_SOURCE_NAMESPACE).await;
    let empty_tree = Arc::new(DeepExpression(Expression::make_construct_tree(vec![])));
    let expected = CompilerOutput::new(
        Some(TypedExpression::new(
            DeepExpression(lambda::expressions::Expression::Lambda {
                environment: empty_tree,
                body: Arc::new(DeepExpression(
                    lambda::expressions::Expression::make_argument(),
                )),
            }),
            DeepType(GenericType::Function {
                parameters: vec![DeepType(GenericType::Any)],
                return_type: Box::new(DeepType(GenericType::Any)),
            }),
        )),
        vec![crate::compilation::CompilerError::new(
            format!("Identifier {x_in_source} not found"),
            SourceLocation { line: 2, column: 2 },
        )],
    );
    assert_eq!(output, Ok(expected));
}

#[test_log::test(tokio::test)]
async fn test_lambda_parameter_type_is_not_a_type() {
    let x_in_source = Name::new(TEST_SOURCE_NAMESPACE, "x".to_string());
    let input = ast::Expression::Lambda {
        parameters: vec![LambdaParameter::new(
            x_in_source.clone(),
            SourceLocation { line: 0, column: 1 },
            Some(ast::Expression::ConstructTree(vec![])),
        )],
        body: Box::new(ast::Expression::Identifier(
            x_in_source.clone(),
            SourceLocation { line: 1, column: 8 },
        )),
    };
    let output = check_types_with_default_globals(&input, TEST_SOURCE_NAMESPACE).await;
    let empty_tree = Arc::new(DeepExpression(Expression::make_construct_tree(vec![])));
    let expected = CompilerOutput::new(
        Some(TypedExpression::new(
            DeepExpression(lambda::expressions::Expression::Lambda {
                environment: empty_tree,
                body: Arc::new(DeepExpression(
                    lambda::expressions::Expression::make_argument(),
                )),
            }),
            DeepType(GenericType::Function {
                parameters: vec![DeepType(GenericType::Any)],
                return_type: Box::new(DeepType(GenericType::Any)),
            }),
        )),
        vec![crate::compilation::CompilerError::new(
            "Type annotation must be a type".to_string(),
            SourceLocation { line: 0, column: 1 },
        )],
    );
    assert_eq!(output, Ok(expected));
}

#[test_log::test(tokio::test)]
async fn test_lambda_parameter_type_is_not_a_compile_time_constant() {
    let x_in_source = Name::new(TEST_SOURCE_NAMESPACE, "x".to_string());
    let y_in_source = Name::new(TEST_SOURCE_NAMESPACE, "y".to_string());
    let type_in_source = Name::new(TEST_SOURCE_NAMESPACE, "Type".to_string());
    let input = ast::Expression::Lambda {
        parameters: vec![LambdaParameter::new(
            x_in_source.clone(),
            SourceLocation { line: 1, column: 1 },
            Some(ast::Expression::Identifier(
                type_in_source.clone(),
                SourceLocation { line: 2, column: 2 },
            )),
        )],
        body: Box::new(ast::Expression::Lambda {
            parameters: vec![LambdaParameter::new(
                y_in_source.clone(),
                SourceLocation { line: 3, column: 3 },
                Some(ast::Expression::Identifier(
                    x_in_source.clone(),
                    SourceLocation { line: 4, column: 4 },
                )),
            )],
            body: Box::new(ast::Expression::Identifier(
                y_in_source.clone(),
                SourceLocation { line: 5, column: 5 },
            )),
        }),
    };
    let output = check_types_with_default_globals(&input, TEST_SOURCE_NAMESPACE).await;
    let empty_tree = Arc::new(DeepExpression(Expression::make_construct_tree(vec![])));
    let expected = CompilerOutput::new(
        Some(TypedExpression::new(
            DeepExpression(lambda::expressions::Expression::Lambda {
                environment: empty_tree.clone(),
                body: Arc::new(DeepExpression(lambda::expressions::Expression::Lambda {
                    environment: empty_tree,
                    body: Arc::new(DeepExpression(
                        lambda::expressions::Expression::make_argument(),
                    )),
                })),
            }),
            DeepType(GenericType::Function {
                parameters: vec![DeepType(GenericType::Type)],
                return_type: Box::new(DeepType(GenericType::Function {
                    parameters: vec![DeepType(GenericType::Any)],
                    return_type: Box::new(DeepType(GenericType::Any)),
                })),
            }),
        )),
        vec![crate::compilation::CompilerError::new(
            "Type annotation must be a compile time constant".to_string(),
            SourceLocation { line: 3, column: 3 },
        )],
    );
    assert_eq!(output, Ok(expected));
}

#[test_log::test(tokio::test)]
async fn test_let_local_variable() {
    let x_in_source = Name::new(TEST_SOURCE_NAMESPACE, "x".to_string());
    let input = ast::Expression::Let {
        name: x_in_source.clone(),
        location: SourceLocation { line: 2, column: 1 },
        value: Box::new(ast::Expression::StringLiteral("Hello".to_string())),
        body: Box::new(ast::Expression::Identifier(
            x_in_source.clone(),
            SourceLocation {
                line: 2,
                column: 10,
            },
        )),
    };
    let output = check_types_with_default_globals(&input, TEST_SOURCE_NAMESPACE).await;
    let expected = CompilerOutput::new(
        Some(TypedExpression::new(
            DeepExpression(lambda::expressions::Expression::Apply {
                callee: Arc::new(DeepExpression(
                    lambda::expressions::Expression::make_lambda(
                        Arc::new(DeepExpression(
                            lambda::expressions::Expression::make_construct_tree(vec![]),
                        )),
                        Arc::new(DeepExpression(
                            // The argument is not being used due to constant folding by the type checker.
                            lambda::expressions::Expression::make_literal(
                                DeepTree::try_from_string("Hello").unwrap(),
                            ),
                        )),
                    ),
                )),
                argument: Arc::new(DeepExpression(
                    lambda::expressions::Expression::make_literal(
                        DeepTree::try_from_string("Hello").unwrap(),
                    ),
                )),
            }),
            DeepType(GenericType::String),
        )),
        vec![],
    );
    assert_eq!(output, Ok(expected));
    let expected_result = DeepTree::try_from_string("Hello").unwrap();
    expect_evaluate_result(
        &output.unwrap().entry_point.unwrap().expression,
        &expected_result,
    )
    .await;
}

#[test_log::test(tokio::test)]
async fn test_let_local_variable_with_error_in_value() {
    let x_in_source = Name::new(TEST_SOURCE_NAMESPACE, "x".to_string());
    let input = ast::Expression::Let {
        name: x_in_source.clone(),
        location: SourceLocation { line: 2, column: 1 },
        value: Box::new(ast::Expression::Identifier(
            // the variable doesn't exist yet
            x_in_source.clone(),
            SourceLocation {
                line: 2,
                column: 13,
            },
        )),
        body: Box::new(ast::Expression::Identifier(
            x_in_source.clone(),
            SourceLocation { line: 3, column: 0 },
        )),
    };
    let output = check_types_with_default_globals(&input, TEST_SOURCE_NAMESPACE).await;
    let expected = CompilerOutput::new(
        None,
        vec![crate::compilation::CompilerError::new(
            format!("Identifier {x_in_source} not found"),
            SourceLocation {
                line: 2,
                column: 13,
            },
        )],
    );
    assert_eq!(output, Ok(expected));
}
