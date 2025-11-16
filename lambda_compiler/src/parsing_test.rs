use crate::ast::{self, LambdaParameter};
use crate::compilation::{CompilerError, SourceLocation};
use crate::parsing::{parse_expression_tolerantly, ParserOutput};
use crate::tokenization::{Token, TokenContent};
use crate::{parsing::parse_expression, tokenization::tokenize_default_syntax};
use lambda::name::{Name, NamespaceId};
use pretty_assertions::assert_eq;

const TEST_NAMESPACE: NamespaceId =
    NamespaceId([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]);

fn find_end_of_file_location(source: &str) -> SourceLocation {
    SourceLocation {
        line: source.chars().filter(|c| *c == '\n').count() as u64,
        column: (source.len() as i64 - 1 - source.rfind('\n').map_or(-1i64, |pos| pos as i64))
            as u64,
    }
}

#[test]
fn test_find_end_of_file_location() {
    assert_eq!(
        find_end_of_file_location(""),
        SourceLocation { line: 0, column: 0 }
    );
    assert_eq!(
        find_end_of_file_location(" "),
        SourceLocation { line: 0, column: 1 }
    );
    assert_eq!(
        find_end_of_file_location("\n"),
        SourceLocation { line: 1, column: 0 }
    );
    assert_eq!(
        find_end_of_file_location("\n "),
        SourceLocation { line: 1, column: 1 }
    );
}

fn parse_wellformed_expression(source: &str) -> ast::Expression {
    let tokens = tokenize_default_syntax(source).expect("tokenization failed");
    let mut token_iterator = tokens.iter().peekable();
    let output = parse_expression(&mut token_iterator, &TEST_NAMESPACE).unwrap();
    assert_eq!(
        Some(&Token::new(
            TokenContent::EndOfFile,
            find_end_of_file_location(source)
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
fn test_parse_comment() {
    let name = Name::new(TEST_NAMESPACE, "f".to_string());
    let expected = ast::Expression::Comment(
        " Comment".to_string(),
        Box::new(ast::Expression::Identifier(
            name,
            SourceLocation { line: 1, column: 0 },
        )),
        SourceLocation { line: 0, column: 0 },
    );
    test_wellformed_parsing("# Comment\nf", expected);
}

#[test_log::test]
fn test_parse_comment_missing_expression() {
    // Currently a comment cannot stand alone, it must be followed by an expression. This makes the code formatter easier to implement.
    let tokens = tokenize_default_syntax("# comment").expect("tokenization failed");
    let mut token_iterator = tokens.iter().peekable();
    let output = parse_expression_tolerantly(&mut token_iterator, &TEST_NAMESPACE);
    assert_eq!(
        Some(&Token::new(
            TokenContent::EndOfFile,
            SourceLocation { line: 0, column: 9 }
        )),
        token_iterator.next()
    );
    let expected = ParserOutput::new(
        None,
        vec![CompilerError::new(
            "Parser error: Expected expression, got end of file.".to_string(),
            SourceLocation::new(0, 9),
        )],
    );
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
fn test_parse_lambda_1_parameter_no_type() {
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
fn test_parse_lambda_1_parameter_with_type() {
    for source in &[
        "(f:String) => f",
        "(f: String) => f",
        "(f: String,) => f",
        "(f: String, ) => f",
    ] {
        let name = Name::new(TEST_NAMESPACE, "f".to_string());
        let expected = ast::Expression::Lambda {
            parameters: vec![LambdaParameter::new(
                name.clone(),
                SourceLocation { line: 0, column: 1 },
                Some(ast::Expression::Identifier(
                    Name::new(TEST_NAMESPACE, "String".to_string()),
                    SourceLocation {
                        line: 0,
                        column: source.find("String").unwrap() as u64,
                    },
                )),
            )],
            body: Box::new(ast::Expression::Identifier(
                name,
                SourceLocation {
                    line: 0,
                    column: (source.len() - 1) as u64,
                },
            )),
        };
        test_wellformed_parsing(source, expected);
    }
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
fn test_parse_missing_fat_arrow() {
    let tokens = tokenize_default_syntax("(f) a").expect("tokenization failed");
    let mut token_iterator = tokens.iter().peekable();
    let output = parse_expression_tolerantly(&mut token_iterator, &TEST_NAMESPACE);
    assert_eq!(
        Some(&Token::new(
            TokenContent::EndOfFile,
            SourceLocation { line: 0, column: 5 }
        )),
        token_iterator.next()
    );
    let expected = ParserOutput::new(
        None,
        vec![CompilerError::new(
            "Parser error: Expected fat arrow (=>).".to_string(),
            SourceLocation::new(0, 4),
        )],
    );
    assert_eq!(expected, output);
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
    let tokens = tokenize_default_syntax(r#"(f) => f(,)"#).expect("tokenization failed");
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
fn test_parse_call_missing_comma() {
    let tokens = tokenize_default_syntax(r#"f(a b)"#).expect("tokenization failed");
    let mut token_iterator = tokens.iter().peekable();
    let output = parse_expression_tolerantly(&mut token_iterator, &TEST_NAMESPACE);
    assert_eq!(
        Some(&Token::new(
            TokenContent::RightParenthesis,
            SourceLocation { line: 0, column: 5 }
        )),
        token_iterator.next()
    );
    let expected = ParserOutput::new(
        None,
        vec![CompilerError::new(
            "Parser error: Expected comma.".to_string(),
            SourceLocation::new(0, 4),
        )],
    );
    assert_eq!(expected, output);
}

#[test_log::test]
fn test_parse_missing_parameter_type() {
    let tokens = tokenize_default_syntax(r#"(f:) => f"#).expect("tokenization failed");
    let mut token_iterator = tokens.iter().peekable();
    let output = parse_expression_tolerantly(&mut token_iterator, &TEST_NAMESPACE);
    assert_eq!(
        Some(&Token::new(
            TokenContent::RightParenthesis,
            SourceLocation { line: 0, column: 3 }
        )),
        token_iterator.next()
    );
    let expected = ParserOutput::new(
        None,
        vec![CompilerError::new(
            "Parser error: Expected expression, found right parenthesis.".to_string(),
            SourceLocation::new(0, 3),
        )],
    );
    assert_eq!(expected, output);
}

#[test_log::test]
fn test_parse_tree_construction_0_children() {
    for source in &["[]", " []", "[ ]", " [] ", "[  ]", "[ ] "] {
        let expected = ast::Expression::ConstructTree(
            vec![],
            SourceLocation {
                line: 0,
                column: source.find("[").unwrap() as u64,
            },
        );
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
        let expected = ast::Expression::ConstructTree(
            vec![a],
            SourceLocation {
                line: 0,
                column: source.find("[").unwrap() as u64,
            },
        );
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
        let expected = ast::Expression::ConstructTree(
            vec![a, b],
            SourceLocation {
                line: 0,
                column: source.find("[").unwrap() as u64,
            },
        );
        test_wellformed_parsing(source, expected);
    }
}

#[test_log::test]
fn test_parse_comment_before_right_bracket() {
    let tokens = tokenize_default_syntax("[#test\n]").expect("tokenization failed");
    let mut token_iterator = tokens.iter().peekable();
    let output = parse_expression_tolerantly(&mut token_iterator, &TEST_NAMESPACE);
    assert_eq!(
        Some(&Token::new(
            TokenContent::Comment("test".to_string()),
            SourceLocation { line: 0, column: 1 }
        )),
        token_iterator.next()
    );
    let expected = ParserOutput::new(
        None,
        vec![CompilerError::new(
            "Parser error: Comments are currently not supported where a right bracket could appear.".to_string(),
            SourceLocation::new(0, 1),
        )],
    );
    assert_eq!(expected, output);
}

#[test_log::test]
fn test_parse_missing_comma_between_parameters() {
    let tokens = tokenize_default_syntax(r#"(f g) => f()"#).expect("tokenization failed");
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

#[test_log::test]
fn test_parse_missing_right_brace() {
    let tokens = tokenize_default_syntax("{a").expect("tokenization failed");
    let mut token_iterator = tokens.iter().peekable();
    let output = parse_expression_tolerantly(&mut token_iterator, &TEST_NAMESPACE);
    assert_eq!(
        Some(&Token::new(
            TokenContent::EndOfFile,
            SourceLocation { line: 0, column: 2 }
        )),
        token_iterator.next()
    );
    let expected = ParserOutput::new(
        None,
        vec![CompilerError::new(
            "Parser error: Expected right brace.".to_string(),
            SourceLocation::new(0, 2),
        )],
    );
    assert_eq!(expected, output);
}

#[test_log::test]
fn test_parse_let() {
    for source in &["let a = b\na", "let a=b\na"] {
        let expected = ast::Expression::Let {
            name: Name::new(TEST_NAMESPACE, "a".to_string()),
            location: SourceLocation {
                line: 0,
                column: source.find("a").unwrap() as u64,
            },
            value: Box::new(ast::Expression::Identifier(
                Name::new(TEST_NAMESPACE, "b".to_string()),
                SourceLocation {
                    line: 0,
                    column: source.find("b").unwrap() as u64,
                },
            )),
            body: Box::new(ast::Expression::Identifier(
                Name::new(TEST_NAMESPACE, "a".to_string()),
                SourceLocation { line: 1, column: 0 },
            )),
        };
        test_wellformed_parsing(source, expected);
    }
}

#[test_log::test]
fn test_parse_let_ambiguity() {
    // b() is parsed as a function call
    let tokens = tokenize_default_syntax("let a = b () => a").expect("tokenization failed");
    let mut token_iterator = tokens.iter().peekable();
    let output = parse_expression_tolerantly(&mut token_iterator, &TEST_NAMESPACE);
    assert_eq!(
        Some(&Token::new(
            TokenContent::FatArrow,
            SourceLocation {
                line: 0,
                column: 13
            }
        )),
        token_iterator.next()
    );
    let expected = ParserOutput::new(
        None,
        vec![CompilerError::new(
            "Parser error: Expected expression, found fat arrow.".to_string(),
            SourceLocation::new(0, 13),
        )],
    );
    assert_eq!(expected, output);
}

#[test_log::test]
fn test_parse_type_of() {
    for source in ["type_of(a)", "type_of (a)", "type_of( a)", "type_of(a )"] {
        let expected = ast::Expression::TypeOf(Box::new(ast::Expression::Identifier(
            Name::new(TEST_NAMESPACE, "a".to_string()),
            SourceLocation {
                line: 0,
                column: source.find("a").unwrap() as u64,
            },
        )));
        test_wellformed_parsing(source, expected);
    }
}

#[test_log::test]
fn test_parse_type_of_missing_left_parenthesis() {
    let tokens = tokenize_default_syntax("type_of a").expect("tokenization failed");
    let mut token_iterator = tokens.iter().peekable();
    let output = parse_expression_tolerantly(&mut token_iterator, &TEST_NAMESPACE);
    assert_eq!(
        Some(&Token::new(
            TokenContent::Identifier("a".to_string()),
            SourceLocation { line: 0, column: 8 }
        )),
        token_iterator.next()
    );
    let expected = ParserOutput::new(
        None,
        vec![CompilerError::new(
            "Parser error: Expected '(' after 'type_of' keyword.".to_string(),
            SourceLocation::new(0, 0),
        )],
    );
    assert_eq!(expected, output);
}

#[test_log::test]
fn test_parse_type_of_missing_right_parenthesis() {
    let tokens = tokenize_default_syntax("type_of(a b").expect("tokenization failed");
    let mut token_iterator = tokens.iter().peekable();
    let output = parse_expression_tolerantly(&mut token_iterator, &TEST_NAMESPACE);
    assert_eq!(
        Some(&Token::new(
            TokenContent::Identifier("b".to_string()),
            SourceLocation {
                line: 0,
                column: 10
            }
        )),
        token_iterator.next()
    );
    let expected = ParserOutput::new(
        None,
        vec![CompilerError::new(
            "Parser error: Expected ')' after expression in 'type_of'.".to_string(),
            SourceLocation::new(0, 8),
        )],
    );
    assert_eq!(expected, output);
}

#[test_log::test]
fn test_parse_dot() {
    let tokens = tokenize_default_syntax(".").expect("tokenization failed");
    let mut token_iterator = tokens.iter().peekable();
    let output = parse_expression_tolerantly(&mut token_iterator, &TEST_NAMESPACE);
    assert_eq!(
        Some(&Token::new(
            TokenContent::Dot,
            SourceLocation { line: 0, column: 0 }
        )),
        token_iterator.next()
    );
    let expected = ParserOutput::new(
        None,
        vec![CompilerError::new(
            "Parser error: Expected expression, found dot.".to_string(),
            SourceLocation::new(0, 0),
        )],
    );
    assert_eq!(expected, output);
}

#[test_log::test]
fn test_parse_colon() {
    let tokens = tokenize_default_syntax(":").expect("tokenization failed");
    let mut token_iterator = tokens.iter().peekable();
    let output = parse_expression_tolerantly(&mut token_iterator, &TEST_NAMESPACE);
    assert_eq!(
        Some(&Token::new(
            TokenContent::Colon,
            SourceLocation { line: 0, column: 0 }
        )),
        token_iterator.next()
    );
    let expected = ParserOutput::new(
        None,
        vec![CompilerError::new(
            "Parser error: Expected expression, found colon.".to_string(),
            SourceLocation::new(0, 0),
        )],
    );
    assert_eq!(expected, output);
}

#[test_log::test]
fn test_parse_assign() {
    let tokens = tokenize_default_syntax("=").expect("tokenization failed");
    let mut token_iterator = tokens.iter().peekable();
    let output = parse_expression_tolerantly(&mut token_iterator, &TEST_NAMESPACE);
    assert_eq!(
        Some(&Token::new(
            TokenContent::Assign,
            SourceLocation { line: 0, column: 0 }
        )),
        token_iterator.next()
    );
    let expected = ParserOutput::new(
        None,
        vec![CompilerError::new(
            "Parser error: Expected expression, found assignment operator.".to_string(),
            SourceLocation::new(0, 0),
        )],
    );
    assert_eq!(expected, output);
}

#[test_log::test]
fn test_parse_right_parenthesis() {
    let tokens = tokenize_default_syntax(")").expect("tokenization failed");
    let mut token_iterator = tokens.iter().peekable();
    let output = parse_expression_tolerantly(&mut token_iterator, &TEST_NAMESPACE);
    assert_eq!(
        Some(&Token::new(
            TokenContent::RightParenthesis,
            SourceLocation { line: 0, column: 0 }
        )),
        token_iterator.next()
    );
    let expected = ParserOutput::new(
        None,
        vec![CompilerError::new(
            "Parser error: Expected expression, found right parenthesis.".to_string(),
            SourceLocation::new(0, 0),
        )],
    );
    assert_eq!(expected, output);
}

#[test_log::test]
fn test_parse_right_bracket() {
    let tokens = tokenize_default_syntax("]").expect("tokenization failed");
    let mut token_iterator = tokens.iter().peekable();
    let output = parse_expression_tolerantly(&mut token_iterator, &TEST_NAMESPACE);
    assert_eq!(
        Some(&Token::new(
            TokenContent::RightBracket,
            SourceLocation { line: 0, column: 0 }
        )),
        token_iterator.next()
    );
    let expected = ParserOutput::new(
        None,
        vec![CompilerError::new(
            "Parser error: Expected expression, found right bracket.".to_string(),
            SourceLocation::new(0, 0),
        )],
    );
    assert_eq!(expected, output);
}

#[test_log::test]
fn test_parse_right_brace() {
    let tokens = tokenize_default_syntax("}").expect("tokenization failed");
    let mut token_iterator = tokens.iter().peekable();
    let output = parse_expression_tolerantly(&mut token_iterator, &TEST_NAMESPACE);
    assert_eq!(
        Some(&Token::new(
            TokenContent::RightBrace,
            SourceLocation { line: 0, column: 0 }
        )),
        token_iterator.next()
    );
    let expected = ParserOutput::new(
        None,
        vec![CompilerError::new(
            "Parser error: Expected expression, found right brace.".to_string(),
            SourceLocation::new(0, 0),
        )],
    );
    assert_eq!(expected, output);
}

#[test_log::test]
fn test_parse_fat_arrow() {
    let tokens = tokenize_default_syntax("=>").expect("tokenization failed");
    let mut token_iterator = tokens.iter().peekable();
    let output = parse_expression_tolerantly(&mut token_iterator, &TEST_NAMESPACE);
    assert_eq!(
        Some(&Token::new(
            TokenContent::FatArrow,
            SourceLocation { line: 0, column: 0 }
        )),
        token_iterator.next()
    );
    let expected = ParserOutput::new(
        None,
        vec![CompilerError::new(
            "Parser error: Expected expression, found fat arrow.".to_string(),
            SourceLocation::new(0, 0),
        )],
    );
    assert_eq!(expected, output);
}

#[test_log::test]
fn test_parse_comma() {
    let tokens = tokenize_default_syntax(",").expect("tokenization failed");
    let mut token_iterator = tokens.iter().peekable();
    let output = parse_expression_tolerantly(&mut token_iterator, &TEST_NAMESPACE);
    assert_eq!(
        Some(&Token::new(
            TokenContent::Comma,
            SourceLocation { line: 0, column: 0 }
        )),
        token_iterator.next()
    );
    let expected = ParserOutput::new(
        None,
        vec![CompilerError::new(
            "Parser error: Expected expression, found comma.".to_string(),
            SourceLocation::new(0, 0),
        )],
    );
    assert_eq!(expected, output);
}

#[test_log::test]
fn test_parse_empty_string() {
    let tokens = tokenize_default_syntax("").expect("tokenization failed");
    let mut token_iterator = tokens.iter().peekable();
    let output = parse_expression_tolerantly(&mut token_iterator, &TEST_NAMESPACE);
    assert_eq!(
        Some(&Token::new(
            TokenContent::EndOfFile,
            SourceLocation { line: 0, column: 0 }
        )),
        token_iterator.next()
    );
    let expected = ParserOutput::new(
        None,
        vec![CompilerError::new(
            "Parser error: Expected expression, got end of file.".to_string(),
            SourceLocation::new(0, 0),
        )],
    );
    assert_eq!(expected, output);
}

#[test_log::test]
fn test_parse_let_missing_identifier() {
    let tokens = tokenize_default_syntax("let = b\na").expect("tokenization failed");
    let mut token_iterator = tokens.iter().peekable();
    let output = parse_expression_tolerantly(&mut token_iterator, &TEST_NAMESPACE);
    assert_eq!(
        Some(&Token::new(
            TokenContent::Assign,
            SourceLocation { line: 0, column: 4 }
        )),
        token_iterator.next()
    );
    let expected = ParserOutput::new(
        None,
        vec![CompilerError::new(
            "Parser error: Expected identifier after 'let' keyword.".to_string(),
            SourceLocation::new(0, 0),
        )],
    );
    assert_eq!(expected, output);
}

#[test_log::test]
fn test_parse_let_missing_equals() {
    let tokens = tokenize_default_syntax("let a b\nc").expect("tokenization failed");
    let mut token_iterator = tokens.iter().peekable();
    let output = parse_expression_tolerantly(&mut token_iterator, &TEST_NAMESPACE);
    assert_eq!(
        Some(&Token::new(
            TokenContent::Identifier("b".to_string()),
            SourceLocation { line: 0, column: 6 }
        )),
        token_iterator.next()
    );
    let expected = ParserOutput::new(
        None,
        vec![CompilerError::new(
            "Parser error: Expected '=' after 'let' identifier.".to_string(),
            SourceLocation::new(0, 0),
        )],
    );
    assert_eq!(expected, output);
}

#[test_log::test]
fn test_parse_tree_construction_missing_comma() {
    let tokens = tokenize_default_syntax("[a b]").expect("tokenization failed");
    let mut token_iterator = tokens.iter().peekable();
    let output = parse_expression_tolerantly(&mut token_iterator, &TEST_NAMESPACE);
    assert_eq!(
        Some(&Token::new(
            TokenContent::RightBracket,
            SourceLocation { line: 0, column: 4 }
        )),
        token_iterator.next()
    );
    let expected = ParserOutput::new(
        None,
        vec![CompilerError::new(
            "Parser error: Expected comma.".to_string(),
            SourceLocation::new(0, 3),
        )],
    );
    assert_eq!(expected, output);
}

#[test_log::test]
fn test_parse_string_literal() {
    let source = r#""hello world""#;
    let expected = ast::Expression::StringLiteral(
        "hello world".to_string(),
        SourceLocation { line: 0, column: 0 },
    );
    test_wellformed_parsing(source, expected);
}

#[test_log::test]
fn test_parse_integer_literal_decimal() {
    let source = "42";
    let expected = ast::Expression::IntegerLiteral(
        42,
        crate::tokenization::IntegerBase::Decimal,
        SourceLocation { line: 0, column: 0 },
    );
    test_wellformed_parsing(source, expected);
}

#[test_log::test]
fn test_parse_integer_literal_negative() {
    // Note: Negative integers are not directly supported at the tokenization level
    // They would be parsed as a unary minus operator followed by a positive integer
    // This test verifies that positive integers work correctly
    let source = "123";
    let expected = ast::Expression::IntegerLiteral(
        123,
        crate::tokenization::IntegerBase::Decimal,
        SourceLocation { line: 0, column: 0 },
    );
    test_wellformed_parsing(source, expected);
}

#[test_log::test]
fn test_parse_function_call_0_arguments() {
    for source in &["f()", "f(  )"] {
        let name = Name::new(TEST_NAMESPACE, "f".to_string());
        let expected = ast::Expression::Apply {
            callee: Box::new(ast::Expression::Identifier(
                name,
                SourceLocation {
                    line: 0,
                    column: 0,
                },
            )),
            arguments: vec![],
        };
        test_wellformed_parsing(source, expected);
    }
}

#[test_log::test]
fn test_parse_function_call_2_arguments() {
    for source in &["f(a, b)", "f(a,b)", "f( a, b )", "f(a, b, )"] {
        let name_f = Name::new(TEST_NAMESPACE, "f".to_string());
        let name_a = Name::new(TEST_NAMESPACE, "a".to_string());
        let name_b = Name::new(TEST_NAMESPACE, "b".to_string());
        let expected = ast::Expression::Apply {
            callee: Box::new(ast::Expression::Identifier(
                name_f,
                SourceLocation { line: 0, column: 0 },
            )),
            arguments: vec![
                ast::Expression::Identifier(
                    name_a,
                    SourceLocation {
                        line: 0,
                        column: source.find("a").unwrap() as u64,
                    },
                ),
                ast::Expression::Identifier(
                    name_b,
                    SourceLocation {
                        line: 0,
                        column: source.find("b").unwrap() as u64,
                    },
                ),
            ],
        };
        test_wellformed_parsing(source, expected);
    }
}

#[test_log::test]
fn test_parse_nested_function_call() {
    let source = "f(g(h))";
    let name_f = Name::new(TEST_NAMESPACE, "f".to_string());
    let name_g = Name::new(TEST_NAMESPACE, "g".to_string());
    let name_h = Name::new(TEST_NAMESPACE, "h".to_string());
    let expected = ast::Expression::Apply {
        callee: Box::new(ast::Expression::Identifier(
            name_f,
            SourceLocation { line: 0, column: 0 },
        )),
        arguments: vec![ast::Expression::Apply {
            callee: Box::new(ast::Expression::Identifier(
                name_g,
                SourceLocation { line: 0, column: 2 },
            )),
            arguments: vec![ast::Expression::Identifier(
                name_h,
                SourceLocation { line: 0, column: 4 },
            )],
        }],
    };
    test_wellformed_parsing(source, expected);
}

#[test_log::test]
fn test_parse_comment_with_special_characters() {
    let name = Name::new(TEST_NAMESPACE, "x".to_string());
    let expected = ast::Expression::Comment(
        " This is a comment with !@#$%^&*()".to_string(),
        Box::new(ast::Expression::Identifier(
            name,
            SourceLocation { line: 1, column: 0 },
        )),
        SourceLocation { line: 0, column: 0 },
    );
    test_wellformed_parsing("# This is a comment with !@#$%^&*()\nx", expected);
}

#[test_log::test]
fn test_parse_lambda_with_multiple_type_annotations() {
    let source = "(x: Int, y: String) => x";
    let name_x = Name::new(TEST_NAMESPACE, "x".to_string());
    let name_y = Name::new(TEST_NAMESPACE, "y".to_string());
    let expected = ast::Expression::Lambda {
        parameters: vec![
            LambdaParameter::new(
                name_x.clone(),
                SourceLocation { line: 0, column: 1 },
                Some(ast::Expression::Identifier(
                    Name::new(TEST_NAMESPACE, "Int".to_string()),
                    SourceLocation { line: 0, column: 4 },
                )),
            ),
            LambdaParameter::new(
                name_y,
                SourceLocation { line: 0, column: 9 },
                Some(ast::Expression::Identifier(
                    Name::new(TEST_NAMESPACE, "String".to_string()),
                    SourceLocation {
                        line: 0,
                        column: 12,
                    },
                )),
            ),
        ],
        body: Box::new(ast::Expression::Identifier(
            name_x,
            SourceLocation {
                line: 0,
                column: 23,
            },
        )),
    };
    test_wellformed_parsing(source, expected);
}

#[test_log::test]
fn test_parse_nested_braces() {
    let source = "{{x}}";
    let name = Name::new(TEST_NAMESPACE, "x".to_string());
    let expected = ast::Expression::Braces(Box::new(ast::Expression::Braces(Box::new(
        ast::Expression::Identifier(name, SourceLocation { line: 0, column: 2 }),
    ))));
    test_wellformed_parsing(source, expected);
}

#[test_log::test]
fn test_parse_tree_with_nested_tree() {
    let source = "[[a], [b]]";
    let name_a = Name::new(TEST_NAMESPACE, "a".to_string());
    let name_b = Name::new(TEST_NAMESPACE, "b".to_string());
    let expected = ast::Expression::ConstructTree(
        vec![
            ast::Expression::ConstructTree(
                vec![ast::Expression::Identifier(
                    name_a,
                    SourceLocation { line: 0, column: 2 },
                )],
                SourceLocation { line: 0, column: 1 },
            ),
            ast::Expression::ConstructTree(
                vec![ast::Expression::Identifier(
                    name_b,
                    SourceLocation { line: 0, column: 7 },
                )],
                SourceLocation { line: 0, column: 6 },
            ),
        ],
        SourceLocation { line: 0, column: 0 },
    );
    test_wellformed_parsing(source, expected);
}

#[test_log::test]
fn test_parse_let_with_lambda() {
    let source = "let f = (x) => x\nf";
    let name_f = Name::new(TEST_NAMESPACE, "f".to_string());
    let name_x = Name::new(TEST_NAMESPACE, "x".to_string());
    let expected = ast::Expression::Let {
        name: name_f.clone(),
        location: SourceLocation { line: 0, column: 4 },
        value: Box::new(ast::Expression::Lambda {
            parameters: vec![LambdaParameter::new(
                name_x.clone(),
                SourceLocation { line: 0, column: 9 },
                None,
            )],
            body: Box::new(ast::Expression::Identifier(
                name_x,
                SourceLocation {
                    line: 0,
                    column: 15,
                },
            )),
        }),
        body: Box::new(ast::Expression::Identifier(
            name_f,
            SourceLocation { line: 1, column: 0 },
        )),
    };
    test_wellformed_parsing(source, expected);
}

#[test_log::test]
fn test_parse_tree_with_trailing_comma_and_whitespace() {
    let source = "[a, b, c, ]";
    let name_a = Name::new(TEST_NAMESPACE, "a".to_string());
    let name_b = Name::new(TEST_NAMESPACE, "b".to_string());
    let name_c = Name::new(TEST_NAMESPACE, "c".to_string());
    let expected = ast::Expression::ConstructTree(
        vec![
            ast::Expression::Identifier(name_a, SourceLocation { line: 0, column: 1 }),
            ast::Expression::Identifier(name_b, SourceLocation { line: 0, column: 4 }),
            ast::Expression::Identifier(name_c, SourceLocation { line: 0, column: 7 }),
        ],
        SourceLocation { line: 0, column: 0 },
    );
    test_wellformed_parsing(source, expected);
}

#[test_log::test]
fn test_parse_comment_before_comma_in_tree() {
    let tokens = tokenize_default_syntax("[a#comment\n, b]").expect("tokenization failed");
    let mut token_iterator = tokens.iter().peekable();
    let output = parse_expression_tolerantly(&mut token_iterator, &TEST_NAMESPACE);
    // The parser encounters the comment when trying to check for a right bracket after parsing 'a'
    // This happens before it checks for a comma
    let expected = ParserOutput::new(
        None,
        vec![CompilerError::new(
            "Parser error: Comments are currently not supported where a right bracket could appear."
                .to_string(),
            SourceLocation::new(0, 2),
        )],
    );
    assert_eq!(expected, output);
}

#[test_log::test]
fn test_parse_comment_before_colon_in_lambda() {
    let tokens = tokenize_default_syntax("(x#comment\n: Int) => x").expect("tokenization failed");
    let mut token_iterator = tokens.iter().peekable();
    let output = parse_expression_tolerantly(&mut token_iterator, &TEST_NAMESPACE);
    let expected = ParserOutput::new(
        None,
        vec![CompilerError::new(
            "Parser error: Comments are currently not supported where a colon could appear."
                .to_string(),
            SourceLocation::new(0, 2),
        )],
    );
    assert_eq!(expected, output);
}

#[test_log::test]
fn test_parse_comment_before_assign_in_let() {
    let tokens = tokenize_default_syntax("let a#comment\n= b\nc").expect("tokenization failed");
    let mut token_iterator = tokens.iter().peekable();
    let output = parse_expression_tolerantly(&mut token_iterator, &TEST_NAMESPACE);
    let expected = ParserOutput::new(
        None,
        vec![CompilerError::new(
            "Parser error: Comments are currently not supported where an assignment operator could appear."
                .to_string(),
            SourceLocation::new(0, 5),
        )],
    );
    assert_eq!(expected, output);
}

#[test_log::test]
fn test_parse_comment_before_left_paren_in_type_of() {
    let tokens = tokenize_default_syntax("type_of#comment\n(x)").expect("tokenization failed");
    let mut token_iterator = tokens.iter().peekable();
    let output = parse_expression_tolerantly(&mut token_iterator, &TEST_NAMESPACE);
    let expected = ParserOutput::new(
        None,
        vec![CompilerError::new(
            "Parser error: Comments are currently not supported where a left parenthesis could appear."
                .to_string(),
            SourceLocation::new(0, 7),
        )],
    );
    assert_eq!(expected, output);
}

#[test_log::test]
fn test_parse_comment_before_fat_arrow() {
    let tokens = tokenize_default_syntax("(x)#comment\n=> x").expect("tokenization failed");
    let mut token_iterator = tokens.iter().peekable();
    let output = parse_expression_tolerantly(&mut token_iterator, &TEST_NAMESPACE);
    let expected = ParserOutput::new(
        None,
        vec![CompilerError::new(
            "Parser error: Comments are currently not supported where a fat arrow could appear."
                .to_string(),
            SourceLocation::new(0, 3),
        )],
    );
    assert_eq!(expected, output);
}

#[test_log::test]
fn test_parse_comment_before_right_brace() {
    let tokens = tokenize_default_syntax("{x#comment\n}").expect("tokenization failed");
    let mut token_iterator = tokens.iter().peekable();
    let output = parse_expression_tolerantly(&mut token_iterator, &TEST_NAMESPACE);
    let expected = ParserOutput::new(
        None,
        vec![CompilerError::new(
            "Parser error: Comments are currently not supported where a right brace is expected."
                .to_string(),
            SourceLocation::new(0, 2),
        )],
    );
    assert_eq!(expected, output);
}

#[test_log::test]
fn test_parse_comment_before_identifier_in_lambda() {
    let tokens = tokenize_default_syntax("(#comment\nx) => x").expect("tokenization failed");
    let mut token_iterator = tokens.iter().peekable();
    let output = parse_expression_tolerantly(&mut token_iterator, &TEST_NAMESPACE);
    let expected = ParserOutput::new(
        None,
        vec![CompilerError::new(
            "Parser error: Comments are currently not supported where an identifier could appear."
                .to_string(),
            SourceLocation::new(0, 1),
        )],
    );
    assert_eq!(expected, output);
}
