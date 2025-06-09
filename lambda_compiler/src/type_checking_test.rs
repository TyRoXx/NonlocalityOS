use crate::{
    ast::{self, LambdaParameter},
    compilation::{CompilerError, CompilerOutput, SourceLocation},
    type_checking::{
        check_types_with_default_globals, type_to_deep_tree, DeepType, GenericType, TypedExpression,
    },
};
use astraea::{
    deep_tree::DeepTree,
    tree::{TreeBlob, TREE_BLOB_MAX_LENGTH},
};
use lambda::{
    expressions::{evaluate, DeepExpression, Expression},
    name::{Name, NamespaceId},
};
use std::sync::Arc;

const TEST_SOURCE_NAMESPACE: NamespaceId =
    NamespaceId([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]);
const IRRELEVANT_SOURCE_LOCATION: SourceLocation = SourceLocation { line: 2, column: 3 };

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
    let a_in_source = Name::new(TEST_SOURCE_NAMESPACE, "a".to_string());
    for non_type_ast in &[
        // testing type TreeWithKnownChildTypes
        ast::Expression::ConstructTree(vec![]),
        // testing type String
        ast::Expression::StringLiteral("test".to_string(), IRRELEVANT_SOURCE_LOCATION),
        // testing type Function
        ast::Expression::Lambda {
            parameters: vec![],
            body: Box::new(ast::Expression::ConstructTree(vec![])),
        },
        // testing type Any
        ast::Expression::Apply {
            callee: Box::new(ast::Expression::Lambda {
                parameters: vec![LambdaParameter::new(
                    a_in_source.clone(),
                    IRRELEVANT_SOURCE_LOCATION,
                    None,
                )],
                body: Box::new(ast::Expression::Identifier(
                    a_in_source,
                    IRRELEVANT_SOURCE_LOCATION,
                )),
            }),
            arguments: vec![ast::Expression::StringLiteral(
                "test".to_string(),
                IRRELEVANT_SOURCE_LOCATION,
            )],
        },
        // testing type Named
        ast::Expression::Identifier(
            Name::new(TEST_SOURCE_NAMESPACE, "true".to_string()),
            IRRELEVANT_SOURCE_LOCATION,
        ),
    ] {
        let x_in_source = Name::new(TEST_SOURCE_NAMESPACE, "x".to_string());
        let input = ast::Expression::Lambda {
            parameters: vec![LambdaParameter::new(
                x_in_source.clone(),
                SourceLocation { line: 0, column: 1 },
                Some(non_type_ast.clone()),
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
async fn test_argument_has_type_error() {
    let x_in_source = Name::new(TEST_SOURCE_NAMESPACE, "x".to_string());
    let y_in_source = Name::new(TEST_SOURCE_NAMESPACE, "y".to_string());
    let input = ast::Expression::Apply {
        callee: Box::new(ast::Expression::Lambda {
            parameters: vec![LambdaParameter::new(
                x_in_source.clone(),
                SourceLocation { line: 1, column: 1 },
                None,
            )],
            body: Box::new(ast::Expression::Identifier(
                x_in_source,
                SourceLocation { line: 2, column: 2 },
            )),
        }),
        arguments: vec![
            // this identifier doesn't exist
            ast::Expression::Identifier(y_in_source.clone(), SourceLocation { line: 3, column: 3 }),
        ],
    };
    let output = check_types_with_default_globals(&input, TEST_SOURCE_NAMESPACE).await;
    let expected = CompilerOutput::new(
        None,
        vec![crate::compilation::CompilerError::new(
            format!("Identifier {y_in_source} not found"),
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
        value: Box::new(ast::Expression::StringLiteral(
            "Hello".to_string(),
            SourceLocation::new(2, 10),
        )),
        body: Box::new(ast::Expression::Identifier(
            x_in_source.clone(),
            SourceLocation { line: 3, column: 0 },
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

#[test_log::test(tokio::test)]
async fn test_let_local_variable_with_error_in_body() {
    let x_in_source = Name::new(TEST_SOURCE_NAMESPACE, "x".to_string());
    let y_in_source = Name::new(TEST_SOURCE_NAMESPACE, "y".to_string());
    let input = ast::Expression::Let {
        name: x_in_source,
        location: SourceLocation { line: 1, column: 1 },
        value: Box::new(ast::Expression::StringLiteral(
            "test".to_string(),
            SourceLocation { line: 2, column: 2 },
        )),
        body: Box::new(ast::Expression::Identifier(
            y_in_source.clone(),
            SourceLocation { line: 3, column: 3 },
        )),
    };
    let output = check_types_with_default_globals(&input, TEST_SOURCE_NAMESPACE).await;
    let expected = CompilerOutput::new(
        None,
        vec![crate::compilation::CompilerError::new(
            format!("Identifier {y_in_source} not found"),
            SourceLocation { line: 3, column: 3 },
        )],
    );
    assert_eq!(output, Ok(expected));
}

#[test_log::test(tokio::test)]
async fn test_string_literal_too_long() {
    // Strings are currently represented as a single tree without children.
    let current_string_size_limit = TREE_BLOB_MAX_LENGTH;
    let x_in_source = Name::new(TEST_SOURCE_NAMESPACE, "x".to_string());
    let input = ast::Expression::Let {
        name: x_in_source.clone(),
        location: SourceLocation { line: 2, column: 2 },
        value: Box::new(ast::Expression::StringLiteral(
            "a".repeat(current_string_size_limit + 1),
            SourceLocation { line: 3, column: 3 },
        )),
        body: Box::new(ast::Expression::Identifier(
            x_in_source.clone(),
            SourceLocation { line: 4, column: 4 },
        )),
    };
    let output = check_types_with_default_globals(&input, TEST_SOURCE_NAMESPACE).await;
    let expected = CompilerOutput::new(
        None,
        vec![crate::compilation::CompilerError::new(
            "String literal is too long".to_string(),
            SourceLocation { line: 3, column: 3 },
        )],
    );
    assert_eq!(output, Ok(expected));
}

#[test_log::test(tokio::test)]
async fn test_bool() {
    let x_in_source = Name::new(TEST_SOURCE_NAMESPACE, "x".to_string());
    let y_in_source = Name::new(TEST_SOURCE_NAMESPACE, "y".to_string());
    let true_in_source = Name::new(TEST_SOURCE_NAMESPACE, "true".to_string());
    let false_in_source = Name::new(TEST_SOURCE_NAMESPACE, "false".to_string());
    let bool_in_source = Name::new(TEST_SOURCE_NAMESPACE, "Bool".to_string());
    let input = ast::Expression::Apply {
        callee: Box::new(ast::Expression::Lambda {
            parameters: vec![
                LambdaParameter::new(
                    x_in_source.clone(),
                    IRRELEVANT_SOURCE_LOCATION,
                    Some(ast::Expression::Identifier(
                        bool_in_source.clone(),
                        IRRELEVANT_SOURCE_LOCATION,
                    )),
                ),
                LambdaParameter::new(
                    y_in_source.clone(),
                    IRRELEVANT_SOURCE_LOCATION,
                    Some(ast::Expression::Identifier(
                        bool_in_source.clone(),
                        IRRELEVANT_SOURCE_LOCATION,
                    )),
                ),
            ],
            body: Box::new(ast::Expression::Identifier(
                x_in_source.clone(),
                IRRELEVANT_SOURCE_LOCATION,
            )),
        }),
        arguments: vec![
            ast::Expression::Identifier(true_in_source, IRRELEVANT_SOURCE_LOCATION),
            ast::Expression::Identifier(false_in_source, IRRELEVANT_SOURCE_LOCATION),
        ],
    };
    let output = check_types_with_default_globals(&input, TEST_SOURCE_NAMESPACE).await;
    let true_deep_tree = DeepTree::new(
        TreeBlob::try_from(bytes::Bytes::from_static(&[1u8])).expect("one byte will always fit"),
        Vec::new(),
    );
    let expected = CompilerOutput::new(
        Some(TypedExpression::new(
            DeepExpression(lambda::expressions::Expression::Apply {
                callee: Arc::new(DeepExpression(
                    lambda::expressions::Expression::make_lambda(
                        Arc::new(DeepExpression(
                            lambda::expressions::Expression::make_construct_tree(vec![]),
                        )),
                        Arc::new(DeepExpression(
                            lambda::expressions::Expression::make_get_child(
                                Arc::new(DeepExpression(
                                    lambda::expressions::Expression::make_argument(),
                                )),
                                0,
                            ),
                        )),
                    ),
                )),
                argument: Arc::new(DeepExpression(
                    lambda::expressions::Expression::make_construct_tree(vec![
                        Arc::new(DeepExpression(
                            lambda::expressions::Expression::make_literal(true_deep_tree.clone()),
                        )),
                        Arc::new(DeepExpression(
                            lambda::expressions::Expression::make_literal(DeepTree::new(
                                TreeBlob::try_from(bytes::Bytes::from_static(&[0u8]))
                                    .expect("one byte will always fit"),
                                Vec::new(),
                            )),
                        )),
                    ]),
                )),
            }),
            DeepType(GenericType::Named(bool_in_source)),
        )),
        vec![],
    );
    assert_eq!(output, Ok(expected));
    expect_evaluate_result(
        &output.unwrap().entry_point.unwrap().expression,
        &true_deep_tree,
    )
    .await;
}

#[test_log::test(tokio::test)]
async fn test_type_of() {
    let input = ast::Expression::TypeOf(Box::new(ast::Expression::StringLiteral(
        "a".to_string(),
        SourceLocation { line: 3, column: 3 },
    )));
    let output = check_types_with_default_globals(&input, TEST_SOURCE_NAMESPACE).await;
    let expected = CompilerOutput::new(
        Some(TypedExpression::new(
            DeepExpression(lambda::expressions::Expression::make_literal(
                type_to_deep_tree(&DeepType(GenericType::String)),
            )),
            DeepType(GenericType::Type),
        )),
        vec![],
    );
    assert_eq!(output, Ok(expected));
}

#[test_log::test(tokio::test)]
async fn test_type_of_with_error() {
    let a = Name::new(TEST_SOURCE_NAMESPACE, "a".to_string());
    let input = ast::Expression::TypeOf(Box::new(ast::Expression::Identifier(
        // unknown identifier
        a.clone(),
        SourceLocation { line: 3, column: 3 },
    )));
    let output = check_types_with_default_globals(&input, TEST_SOURCE_NAMESPACE).await;
    let expected = CompilerOutput::new(
        None,
        vec![CompilerError::new(
            format!("Identifier {a} not found"),
            SourceLocation { line: 3, column: 3 },
        )],
    );
    assert_eq!(output, Ok(expected));
}

#[test_log::test(tokio::test)]
async fn test_type_of_does_not_capture() {
    let a = Name::new(TEST_SOURCE_NAMESPACE, "a".to_string());
    let input = ast::Expression::Lambda {
        parameters: vec![LambdaParameter::new(
            a.clone(),
            IRRELEVANT_SOURCE_LOCATION,
            None,
        )],
        body: Box::new(ast::Expression::Lambda {
            parameters: vec![],
            body: Box::new(ast::Expression::TypeOf(Box::new(
                ast::Expression::Identifier(a, IRRELEVANT_SOURCE_LOCATION),
            ))),
        }),
    };
    let output = check_types_with_default_globals(&input, TEST_SOURCE_NAMESPACE).await;
    let expected = CompilerOutput::new(
        Some(TypedExpression::new(
            DeepExpression(lambda::expressions::Expression::make_lambda(
                Arc::new(DeepExpression(Expression::ConstructTree(vec![]))),
                Arc::new(DeepExpression(
                    lambda::expressions::Expression::make_lambda(
                        Arc::new(DeepExpression(Expression::ConstructTree(
                            // The environment is empty because 'a' is not captured. type_of does not evaluate the expression, so it doesn't need to capture any variables.
                            vec![],
                        ))),
                        Arc::new(DeepExpression(
                            lambda::expressions::Expression::make_literal(type_to_deep_tree(
                                &DeepType(GenericType::Any),
                            )),
                        )),
                    ),
                )),
            )),
            DeepType(GenericType::Function {
                parameters: vec![DeepType(GenericType::Any)],
                return_type: Box::new(DeepType(GenericType::Function {
                    parameters: vec![],
                    return_type: Box::new(DeepType(GenericType::Type)),
                })),
            }),
        )),
        vec![],
    );
    assert_eq!(output, Ok(expected));
}
