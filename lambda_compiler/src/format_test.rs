use crate::{
    ast::{Expression, LambdaParameter},
    compilation::SourceLocation,
    format::format_expression,
};
use lambda::name::{Name, NamespaceId};

const TEST_NAMESPACE: NamespaceId =
    NamespaceId([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]);
const IRRELEVANT_INDENTATION_LEVEL: usize = 23;

#[test]
fn test_format_identifier() {
    let mut formatted = String::new();
    format_expression(
        &Expression::Identifier(
            Name::new(TEST_NAMESPACE, "id123".to_string()),
            // location doesn't matter for this test
            SourceLocation { line: 1, column: 2 },
        ),
        IRRELEVANT_INDENTATION_LEVEL,
        &mut formatted,
    )
    .unwrap();
    assert_eq!("id123", formatted.as_str());
}

#[test]
fn test_format_string_literal_alphanumeric() {
    let mut formatted = String::new();
    format_expression(
        &Expression::StringLiteral(r#"abc123"#.to_string()),
        IRRELEVANT_INDENTATION_LEVEL,
        &mut formatted,
    )
    .unwrap();
    assert_eq!(r#""abc123""#, formatted.as_str());
}

#[test]
fn test_format_string_literal_quotes_and_backslash() {
    let mut formatted = String::new();
    format_expression(
        &Expression::StringLiteral(r#""'\"#.to_string()),
        IRRELEVANT_INDENTATION_LEVEL,
        &mut formatted,
    )
    .unwrap();
    assert_eq!(r#""\"\'\\""#, formatted.as_str());
}

#[test]
fn test_format_string_literal_whitespace() {
    let mut formatted = String::new();
    format_expression(
        &Expression::StringLiteral("\n\r\t".to_string()),
        IRRELEVANT_INDENTATION_LEVEL,
        &mut formatted,
    )
    .unwrap();
    assert_eq!(r#""\n\r\t""#, formatted.as_str());
}

#[test]
fn test_format_apply_0_arguments() {
    let mut formatted = String::new();
    format_expression(
        &Expression::Apply {
            callee: Box::new(Expression::Identifier(
                Name::new(TEST_NAMESPACE, "f".to_string()),
                // location doesn't matter for this test
                SourceLocation { line: 1, column: 2 },
            )),
            arguments: Vec::new(),
        },
        IRRELEVANT_INDENTATION_LEVEL,
        &mut formatted,
    )
    .unwrap();
    assert_eq!("f()", formatted.as_str());
}

#[test]
fn test_format_apply_1_argument() {
    let mut formatted = String::new();
    format_expression(
        &Expression::Apply {
            callee: Box::new(Expression::Identifier(
                Name::new(TEST_NAMESPACE, "f".to_string()),
                // location doesn't matter for this test
                SourceLocation { line: 1, column: 2 },
            )),
            arguments: vec![Expression::StringLiteral("test".to_string())],
        },
        IRRELEVANT_INDENTATION_LEVEL,
        &mut formatted,
    )
    .unwrap();
    assert_eq!(r#"f("test", )"#, formatted.as_str());
}

#[test]
fn test_format_apply_2_arguments() {
    let mut formatted = String::new();
    format_expression(
        &Expression::Apply {
            callee: Box::new(Expression::Identifier(
                Name::new(TEST_NAMESPACE, "f".to_string()),
                // location doesn't matter for this test
                SourceLocation { line: 1, column: 2 },
            )),
            arguments: vec![
                Expression::StringLiteral("test".to_string()),
                Expression::StringLiteral("123".to_string()),
            ],
        },
        IRRELEVANT_INDENTATION_LEVEL,
        &mut formatted,
    )
    .unwrap();
    assert_eq!(r#"f("test", "123", )"#, formatted.as_str());
}

#[test]
fn test_format_lambda_0_parameters() {
    let mut formatted = String::new();
    format_expression(
        &Expression::Lambda {
            parameters: Vec::new(),
            body: Box::new(Expression::Identifier(
                Name::new(TEST_NAMESPACE, "f".to_string()),
                // location doesn't matter for this test
                SourceLocation { line: 1, column: 2 },
            )),
        },
        IRRELEVANT_INDENTATION_LEVEL,
        &mut formatted,
    )
    .unwrap();
    assert_eq!("() => f", formatted.as_str());
}

#[test]
fn test_format_lambda_1_parameter() {
    let mut formatted = String::new();
    format_expression(
        &Expression::Lambda {
            parameters: vec![LambdaParameter::new(
                Name::new(TEST_NAMESPACE, "a".to_string()),
                // location doesn't matter for this test
                SourceLocation { line: 0, column: 0 },
                None,
            )],
            body: Box::new(Expression::Identifier(
                Name::new(TEST_NAMESPACE, "f".to_string()),
                // location doesn't matter for this test
                SourceLocation { line: 0, column: 0 },
            )),
        },
        IRRELEVANT_INDENTATION_LEVEL,
        &mut formatted,
    )
    .unwrap();
    assert_eq!("(a, ) => f", formatted.as_str());
}

#[test]
fn test_format_lambda_2_parameters() {
    let mut formatted = String::new();
    format_expression(
        &Expression::Lambda {
            parameters: vec![
                LambdaParameter::new(
                    Name::new(TEST_NAMESPACE, "a".to_string()),
                    // location doesn't matter for this test
                    SourceLocation { line: 0, column: 0 },
                    None,
                ),
                LambdaParameter::new(
                    Name::new(TEST_NAMESPACE, "b".to_string()),
                    // location doesn't matter for this test
                    SourceLocation { line: 0, column: 0 },
                    None,
                ),
            ],
            body: Box::new(Expression::Identifier(
                Name::new(TEST_NAMESPACE, "f".to_string()),
                // location doesn't matter for this test
                SourceLocation { line: 0, column: 0 },
            )),
        },
        IRRELEVANT_INDENTATION_LEVEL,
        &mut formatted,
    )
    .unwrap();
    assert_eq!("(a, b, ) => f", formatted.as_str());
}

#[test]
fn test_format_lambda_type_annotation() {
    let mut formatted = String::new();
    format_expression(
        &Expression::Lambda {
            parameters: vec![LambdaParameter::new(
                Name::new(TEST_NAMESPACE, "a".to_string()),
                // location doesn't matter for this test
                SourceLocation { line: 0, column: 0 },
                Some(Expression::Identifier(
                    Name::new(TEST_NAMESPACE, "String".to_string()),
                    // location doesn't matter for this test
                    SourceLocation { line: 0, column: 0 },
                )),
            )],
            body: Box::new(Expression::Identifier(
                Name::new(TEST_NAMESPACE, "f".to_string()),
                // location doesn't matter for this test
                SourceLocation { line: 0, column: 0 },
            )),
        },
        IRRELEVANT_INDENTATION_LEVEL,
        &mut formatted,
    )
    .unwrap();
    assert_eq!("(a: String, ) => f", formatted.as_str());
}

#[test]
fn test_format_construct_tree_0_children() {
    let mut formatted = String::new();
    format_expression(
        &Expression::ConstructTree(vec![]),
        IRRELEVANT_INDENTATION_LEVEL,
        &mut formatted,
    )
    .unwrap();
    assert_eq!("[]", formatted.as_str());
}

#[test]
fn test_format_construct_tree_1_child() {
    let mut formatted = String::new();
    format_expression(
        &Expression::ConstructTree(vec![Expression::Identifier(
            Name::new(TEST_NAMESPACE, "a".to_string()),
            // location doesn't matter for this test
            SourceLocation { line: 0, column: 0 },
        )]),
        IRRELEVANT_INDENTATION_LEVEL,
        &mut formatted,
    )
    .unwrap();
    assert_eq!("[a, ]", formatted.as_str());
}

#[test]
fn test_format_construct_tree_2_children() {
    let mut formatted = String::new();
    format_expression(
        &Expression::ConstructTree(vec![
            Expression::Identifier(
                Name::new(TEST_NAMESPACE, "a".to_string()),
                // location doesn't matter for this test
                SourceLocation { line: 0, column: 0 },
            ),
            Expression::Identifier(
                Name::new(TEST_NAMESPACE, "b".to_string()),
                // location doesn't matter for this test
                SourceLocation { line: 0, column: 0 },
            ),
        ]),
        IRRELEVANT_INDENTATION_LEVEL,
        &mut formatted,
    )
    .unwrap();
    assert_eq!("[a, b, ]", formatted.as_str());
}

#[test]
fn test_format_braces() {
    let mut formatted = String::new();
    format_expression(
        &Expression::Braces(Box::new(Expression::Identifier(
            Name::new(TEST_NAMESPACE, "a".to_string()),
            // location doesn't matter for this test
            SourceLocation { line: 0, column: 0 },
        ))),
        IRRELEVANT_INDENTATION_LEVEL,
        &mut formatted,
    )
    .unwrap();
    assert_eq!("{a}", formatted.as_str());
}

#[test]
fn test_format_let() {
    let mut formatted = String::new();
    format_expression(
        &Expression::Let {
            name: Name::new(TEST_NAMESPACE, "a".to_string()),
            // location doesn't matter for this test
            location: SourceLocation { line: 0, column: 0 },
            value: Box::new(Expression::Identifier(
                Name::new(TEST_NAMESPACE, "b".to_string()),
                // location doesn't matter for this test
                SourceLocation { line: 0, column: 0 },
            )),
            body: Box::new(Expression::Identifier(
                Name::new(TEST_NAMESPACE, "c".to_string()),
                // location doesn't matter for this test
                SourceLocation { line: 0, column: 0 },
            )),
        },
        0,
        &mut formatted,
    )
    .unwrap();
    assert_eq!("let a = b\nc", formatted.as_str());
}

#[test]
fn test_format_lambda_let_indentation() {
    let mut formatted = String::new();
    format_expression(
        &Expression::Lambda {
            parameters: Vec::new(),
            body: Box::new(Expression::Let {
                name: Name::new(TEST_NAMESPACE, "a".to_string()),
                // location doesn't matter for this test
                location: SourceLocation { line: 0, column: 0 },
                value: Box::new(Expression::Identifier(
                    Name::new(TEST_NAMESPACE, "b".to_string()),
                    // location doesn't matter for this test
                    SourceLocation { line: 0, column: 0 },
                )),
                body: Box::new(Expression::Identifier(
                    Name::new(TEST_NAMESPACE, "c".to_string()),
                    // location doesn't matter for this test
                    SourceLocation { line: 0, column: 0 },
                )),
            }),
        },
        0,
        &mut formatted,
    )
    .unwrap();
    assert_eq!(concat!("() => let a = b\n", "    c"), formatted.as_str());
}
