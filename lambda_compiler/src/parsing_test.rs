use crate::ast::{self, LambdaParameter};
use crate::compilation::{CompilerError, SourceLocation};
use crate::parsing::{parse_expression_tolerantly, ParserOutput};
use crate::tokenization::{Token, TokenContent};
use crate::{parsing::parse_expression, tokenization::tokenize_default_syntax};
use lambda::name::{Name, NamespaceId};

const TEST_NAMESPACE: NamespaceId =
    NamespaceId([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]);

fn parse_wellformed_expression(source: &str) -> ast::Expression {
    let tokens = tokenize_default_syntax(source);
    let mut token_iterator = tokens.iter().peekable();
    let output = parse_expression(&mut token_iterator, &TEST_NAMESPACE).unwrap();
    assert_eq!(
        Some(&Token::new(
            TokenContent::EndOfFile,
            SourceLocation {
                line: 0,
                column: source.len() as u64
            }
        )),
        token_iterator.next()
    );
    output
}

fn test_wellformed_parsing(source: &str, expected: ast::Expression) {
    let output = parse_wellformed_expression(source);
    assert_eq!(expected, output);
}

#[test_log::test]
fn test_parse_lambda_0_parameters() {
    let name = Name::new(TEST_NAMESPACE, "f".to_string());
    let expected = ast::Expression::Lambda {
        parameters: vec![],
        body: Box::new(ast::Expression::Identifier(
            name,
            SourceLocation { line: 0, column: 6 },
        )),
    };
    test_wellformed_parsing(r#"() => f"#, expected);
}

#[test_log::test]
fn test_parse_lambda_1_parameter() {
    let name = Name::new(TEST_NAMESPACE, "f".to_string());
    let expected = ast::Expression::Lambda {
        parameters: vec![LambdaParameter::new(
            name.clone(),
            SourceLocation { line: 0, column: 1 },
            None,
        )],
        body: Box::new(ast::Expression::Identifier(
            name,
            SourceLocation { line: 0, column: 7 },
        )),
    };
    test_wellformed_parsing(r#"(f) => f"#, expected);
}

#[test_log::test]
fn test_parse_lambda_2_parameters() {
    for (source, column_f, column_g) in &[
        ("(f, g) => f", 1, 4),
        ("(f, g,) => f", 1, 4),
        ("(f, g, ) => f", 1, 4),
        ("( f , g ) => f", 2, 6),
        ("( f , g , ) => f", 2, 6),
    ] {
        let f = ast::Expression::Identifier(
            Name::new(TEST_NAMESPACE, "f".to_string()),
            SourceLocation {
                line: 0,
                column: (source.len() - 1) as u64,
            },
        );
        let expected = ast::Expression::Lambda {
            parameters: vec![
                LambdaParameter::new(
                    Name::new(TEST_NAMESPACE, "f".to_string()),
                    SourceLocation {
                        line: 0,
                        column: *column_f,
                    },
                    None,
                ),
                LambdaParameter::new(
                    Name::new(TEST_NAMESPACE, "g".to_string()),
                    SourceLocation {
                        line: 0,
                        column: *column_g,
                    },
                    None,
                ),
            ],
            body: Box::new(f),
        };
        test_wellformed_parsing(source, expected);
    }
}

#[test_log::test]
fn test_parse_nested_lambda() {
    let f = Name::new(TEST_NAMESPACE, "f".to_string());
    let g = Name::new(TEST_NAMESPACE, "g".to_string());
    let expected = ast::Expression::Lambda {
        parameters: vec![LambdaParameter::new(
            f.clone(),
            SourceLocation { line: 0, column: 1 },
            None,
        )],
        body: Box::new(ast::Expression::Lambda {
            parameters: vec![LambdaParameter::new(
                g,
                SourceLocation { line: 0, column: 8 },
                None,
            )],
            body: Box::new(ast::Expression::Identifier(
                f,
                SourceLocation {
                    line: 0,
                    column: 14,
                },
            )),
        }),
    };
    test_wellformed_parsing(r#"(f) => (g) => f"#, expected);
}

#[test_log::test]
fn test_parse_function_call_1_argument() {
    let name = Name::new(TEST_NAMESPACE, "f".to_string());
    let expected = ast::Expression::Lambda {
        parameters: vec![LambdaParameter::new(
            name.clone(),
            SourceLocation { line: 0, column: 1 },
            None,
        )],
        body: Box::new(ast::Expression::Apply {
            callee: Box::new(ast::Expression::Identifier(
                name.clone(),
                SourceLocation { line: 0, column: 7 },
            )),
            arguments: vec![ast::Expression::Identifier(
                name,
                SourceLocation { line: 0, column: 9 },
            )],
        }),
    };
    test_wellformed_parsing(r#"(f) => f(f)"#, expected);
}

#[test_log::test]
fn test_parse_missing_argument() {
    let tokens = tokenize_default_syntax(r#"(f) => f(,)"#);
    let mut token_iterator = tokens.iter().peekable();
    let output = parse_expression_tolerantly(&mut token_iterator, &TEST_NAMESPACE);
    assert_eq!(
        Some(&Token::new(
            TokenContent::Comma,
            SourceLocation { line: 0, column: 9 }
        )),
        token_iterator.next()
    );
    let expected = ParserOutput::new(
        None,
        vec![CompilerError::new(
            "Parser error: Expected expression, found comma.".to_string(),
            SourceLocation::new(0, 9),
        )],
    );
    assert_eq!(expected, output);
}

#[test_log::test]
fn test_parse_tree_construction_0_children() {
    for source in &["[]", " []", "[ ]", " [] ", "[  ]", "[ ] "] {
        let expected = ast::Expression::ConstructTree(vec![]);
        test_wellformed_parsing(source, expected);
    }
}

#[test_log::test]
fn test_parse_tree_construction_1_child() {
    for source in &[
        "[a]", "[ a ]", "[ a, ]", "[a,]", "[a, ]", "[ a,]", "[ a ,]", " [ a ,] ",
    ] {
        let name = Name::new(TEST_NAMESPACE, "a".to_string());
        let a = ast::Expression::Identifier(
            name.clone(),
            SourceLocation {
                line: 0,
                column: source.find("a").unwrap() as u64,
            },
        );
        let expected = ast::Expression::ConstructTree(vec![a]);
        test_wellformed_parsing(source, expected);
    }
}

#[test_log::test]
fn test_parse_tree_construction_2_children() {
    for source in &[
        "[a, b]",
        "[ a, b ]",
        "[ a, b, ]",
        "[a, b,]",
        "[a, b, ]",
        "[ a , b]",
        "[ a , b ]",
        "[ a , b, ]",
        " [ a , b , ] ",
    ] {
        let a = ast::Expression::Identifier(
            Name::new(TEST_NAMESPACE, "a".to_string()),
            SourceLocation {
                line: 0,
                column: source.find("a").unwrap() as u64,
            },
        );
        let b = ast::Expression::Identifier(
            Name::new(TEST_NAMESPACE, "b".to_string()),
            SourceLocation {
                line: 0,
                column: source.find("b").unwrap() as u64,
            },
        );
        let expected = ast::Expression::ConstructTree(vec![a, b]);
        test_wellformed_parsing(source, expected);
    }
}

#[test_log::test]
fn test_parse_missing_comma_between_parameters() {
    let tokens = tokenize_default_syntax(r#"(f g) => f()"#);
    let mut token_iterator = tokens.iter().peekable();
    let output = parse_expression_tolerantly(&mut token_iterator, &TEST_NAMESPACE);
    assert_eq!(
        Some(&Token::new(
            crate::tokenization::TokenContent::Identifier("g".to_string()),
            SourceLocation { line: 0, column: 3 }
        )),
        token_iterator.next()
    );
    let expected = ParserOutput::new(
        None,
        vec![CompilerError::new(
            "Parser error: Expected comma or right parenthesis in lambda parameter list."
                .to_string(),
            SourceLocation::new(0, 3),
        )],
    );
    assert_eq!(expected, output);
}

#[test_log::test]
fn test_parse_braces() {
    for source in &["{a}", "{ a}", "{ a }", "{a }", " {a}"] {
        let expected = ast::Expression::Braces(Box::new(ast::Expression::Identifier(
            Name::new(TEST_NAMESPACE, "a".to_string()),
            SourceLocation {
                line: 0,
                column: source.find("a").unwrap() as u64,
            },
        )));
        test_wellformed_parsing(source, expected);
    }
}
