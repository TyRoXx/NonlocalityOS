use crate::{
    compilation::SourceLocation,
    tokenization::{tokenize_default_syntax, Token, TokenContent},
};

fn test_tokenize_default_syntax(source: &str, expected_tokens: &[Token]) {
    let tokenized = tokenize_default_syntax(source);
    assert_eq!(expected_tokens, &tokenized[..]);
}

#[test_log::test]
fn test_tokenize_default_syntax_empty_source() {
    test_tokenize_default_syntax(
        "",
        &[Token {
            content: TokenContent::EndOfFile,
            location: SourceLocation { line: 0, column: 0 },
        }],
    );
}

#[test_log::test]
fn test_tokenize_default_syntax_space() {
    test_tokenize_default_syntax(
        " ",
        &[
            Token {
                content: TokenContent::Whitespace,
                location: SourceLocation { line: 0, column: 0 },
            },
            Token {
                content: TokenContent::EndOfFile,
                location: SourceLocation { line: 0, column: 1 },
            },
        ],
    );
}

#[test_log::test]
fn test_tokenize_default_syntax_newline() {
    test_tokenize_default_syntax(
        "\n",
        &[
            Token {
                content: TokenContent::Whitespace,
                location: SourceLocation { line: 0, column: 0 },
            },
            Token {
                content: TokenContent::EndOfFile,
                location: SourceLocation { line: 1, column: 0 },
            },
        ],
    );
}

#[test_log::test]
fn test_tokenize_default_syntax_source_locations() {
    test_tokenize_default_syntax(
        " \n  test=\n().\"\"=>[]{}:",
        &[
            Token {
                content: TokenContent::Whitespace,
                location: SourceLocation { line: 0, column: 0 },
            },
            Token {
                content: TokenContent::Whitespace,
                location: SourceLocation { line: 0, column: 1 },
            },
            Token {
                content: TokenContent::Whitespace,
                location: SourceLocation { line: 1, column: 0 },
            },
            Token {
                content: TokenContent::Whitespace,
                location: SourceLocation { line: 1, column: 1 },
            },
            Token {
                content: TokenContent::Identifier("test".to_string()),
                location: SourceLocation { line: 1, column: 2 },
            },
            Token {
                content: TokenContent::Assign,
                location: SourceLocation { line: 1, column: 6 },
            },
            Token {
                content: TokenContent::Whitespace,
                location: SourceLocation { line: 1, column: 7 },
            },
            Token {
                content: TokenContent::LeftParenthesis,
                location: SourceLocation { line: 2, column: 0 },
            },
            Token {
                content: TokenContent::RightParenthesis,
                location: SourceLocation { line: 2, column: 1 },
            },
            Token {
                content: TokenContent::Dot,
                location: SourceLocation { line: 2, column: 2 },
            },
            Token {
                content: TokenContent::Quotes("".to_string()),
                location: SourceLocation { line: 2, column: 3 },
            },
            Token {
                content: TokenContent::FatArrow,
                location: SourceLocation { line: 2, column: 5 },
            },
            Token {
                content: TokenContent::LeftBracket,
                location: SourceLocation { line: 2, column: 7 },
            },
            Token {
                content: TokenContent::RightBracket,
                location: SourceLocation { line: 2, column: 8 },
            },
            Token {
                content: TokenContent::LeftBrace,
                location: SourceLocation { line: 2, column: 9 },
            },
            Token {
                content: TokenContent::RightBrace,
                location: SourceLocation {
                    line: 2,
                    column: 10,
                },
            },
            Token {
                content: TokenContent::Colon,
                location: SourceLocation {
                    line: 2,
                    column: 11,
                },
            },
            Token {
                content: TokenContent::EndOfFile,
                location: SourceLocation {
                    line: 2,
                    column: 12,
                },
            },
        ],
    );
}

#[test_log::test]
fn test_tokenize_default_syntax_assign_ambiguity_1() {
    test_tokenize_default_syntax(
        "==>==",
        &[
            Token {
                content: TokenContent::Assign,
                location: SourceLocation { line: 0, column: 0 },
            },
            Token {
                content: TokenContent::FatArrow,
                location: SourceLocation { line: 0, column: 1 },
            },
            Token {
                content: TokenContent::Assign,
                location: SourceLocation { line: 0, column: 3 },
            },
            Token {
                content: TokenContent::Assign,
                location: SourceLocation { line: 0, column: 4 },
            },
            Token {
                content: TokenContent::EndOfFile,
                location: SourceLocation { line: 0, column: 5 },
            },
        ],
    );
}

#[test_log::test]
fn test_tokenize_default_syntax_assign_ambiguity_2() {
    test_tokenize_default_syntax(
        "==>",
        &[
            Token {
                content: TokenContent::Assign,
                location: SourceLocation { line: 0, column: 0 },
            },
            Token {
                content: TokenContent::FatArrow,
                location: SourceLocation { line: 0, column: 1 },
            },
            Token {
                content: TokenContent::EndOfFile,
                location: SourceLocation { line: 0, column: 3 },
            },
        ],
    );
}

#[test_log::test]
fn test_tokenize_default_syntax_identifier_lowercase() {
    test_tokenize_default_syntax(
        "testabcxyz",
        &[
            Token {
                content: TokenContent::Identifier("testabcxyz".to_string()),
                location: SourceLocation { line: 0, column: 0 },
            },
            Token {
                content: TokenContent::EndOfFile,
                location: SourceLocation {
                    line: 0,
                    column: 10,
                },
            },
        ],
    );
}

#[test_log::test]
fn test_tokenize_default_syntax_identifier_uppercase() {
    test_tokenize_default_syntax(
        "TestabcxyZ",
        &[
            Token {
                content: TokenContent::Identifier("TestabcxyZ".to_string()),
                location: SourceLocation { line: 0, column: 0 },
            },
            Token {
                content: TokenContent::EndOfFile,
                location: SourceLocation {
                    line: 0,
                    column: 10,
                },
            },
        ],
    );
}

#[test_log::test]
fn test_tokenize_default_syntax_let() {
    test_tokenize_default_syntax(
        "let",
        &[
            Token {
                content: TokenContent::Identifier("let".to_string()),
                location: SourceLocation { line: 0, column: 0 },
            },
            Token {
                content: TokenContent::EndOfFile,
                location: SourceLocation { line: 0, column: 3 },
            },
        ],
    );
}

#[test_log::test]
fn test_tokenize_default_syntax_assign() {
    test_tokenize_default_syntax(
        "=",
        &[
            Token {
                content: TokenContent::Assign,
                location: SourceLocation { line: 0, column: 0 },
            },
            Token {
                content: TokenContent::EndOfFile,
                location: SourceLocation { line: 0, column: 1 },
            },
        ],
    );
}

#[test_log::test]
fn test_tokenize_default_syntax_left_parenthesis() {
    test_tokenize_default_syntax(
        "(",
        &[
            Token {
                content: TokenContent::LeftParenthesis,
                location: SourceLocation { line: 0, column: 0 },
            },
            Token {
                content: TokenContent::EndOfFile,
                location: SourceLocation { line: 0, column: 1 },
            },
        ],
    );
}

#[test_log::test]
fn test_tokenize_default_syntax_right_parenthesis() {
    test_tokenize_default_syntax(
        ")",
        &[
            Token {
                content: TokenContent::RightParenthesis,
                location: SourceLocation { line: 0, column: 0 },
            },
            Token {
                content: TokenContent::EndOfFile,
                location: SourceLocation { line: 0, column: 1 },
            },
        ],
    );
}

#[test_log::test]
fn test_tokenize_default_syntax_left_bracket() {
    test_tokenize_default_syntax(
        "[",
        &[
            Token {
                content: TokenContent::LeftBracket,
                location: SourceLocation { line: 0, column: 0 },
            },
            Token {
                content: TokenContent::EndOfFile,
                location: SourceLocation { line: 0, column: 1 },
            },
        ],
    );
}

#[test_log::test]
fn test_tokenize_default_syntax_right_bracket() {
    test_tokenize_default_syntax(
        "]",
        &[
            Token {
                content: TokenContent::RightBracket,
                location: SourceLocation { line: 0, column: 0 },
            },
            Token {
                content: TokenContent::EndOfFile,
                location: SourceLocation { line: 0, column: 1 },
            },
        ],
    );
}

#[test_log::test]
fn test_tokenize_default_syntax_dot() {
    test_tokenize_default_syntax(
        ".",
        &[
            Token {
                content: TokenContent::Dot,
                location: SourceLocation { line: 0, column: 0 },
            },
            Token {
                content: TokenContent::EndOfFile,
                location: SourceLocation { line: 0, column: 1 },
            },
        ],
    );
}

#[test_log::test]
fn test_tokenize_default_syntax_colon() {
    test_tokenize_default_syntax(
        ":",
        &[
            Token {
                content: TokenContent::Colon,
                location: SourceLocation { line: 0, column: 0 },
            },
            Token {
                content: TokenContent::EndOfFile,
                location: SourceLocation { line: 0, column: 1 },
            },
        ],
    );
}

#[test_log::test]
fn test_tokenize_default_syntax_fat_arrow() {
    test_tokenize_default_syntax(
        "=>",
        &[
            Token {
                content: TokenContent::FatArrow,
                location: SourceLocation { line: 0, column: 0 },
            },
            Token {
                content: TokenContent::EndOfFile,
                location: SourceLocation { line: 0, column: 2 },
            },
        ],
    );
}

fn wellformed_quotes(literal: &str, decoded: &str) {
    test_tokenize_default_syntax(
        &format!("\"{literal}\""),
        &[
            Token {
                content: TokenContent::Quotes(decoded.to_string()),
                location: SourceLocation { line: 0, column: 0 },
            },
            Token {
                content: TokenContent::EndOfFile,
                location: SourceLocation {
                    line: 0,
                    column: 2 + literal.len() as u64,
                },
            },
        ],
    );
}

#[test_log::test]
fn test_tokenize_default_syntax_string_empty() {
    wellformed_quotes("", "");
}

#[test_log::test]
fn test_tokenize_default_syntax_string_short() {
    wellformed_quotes("hello", "hello");
}

#[test_log::test]
fn test_tokenize_default_syntax_string_longer() {
    let literal = std::iter::repeat_n('A', 1000).collect::<String>();
    wellformed_quotes(&literal, &literal);
}

#[test_log::test]
fn test_tokenize_default_syntax_string_new_line() {
    // TODO: perhaps we should disallow line breaks in string literals
    test_tokenize_default_syntax(
        "\"\n\"",
        &[
            Token {
                content: TokenContent::Quotes("\n".to_string()),
                location: SourceLocation { line: 0, column: 0 },
            },
            Token {
                content: TokenContent::EndOfFile,
                location: SourceLocation { line: 1, column: 1 },
            },
        ],
    );
}

#[test_log::test]
fn test_tokenize_default_syntax_string_carriage_return() {
    // TODO: perhaps we should disallow line breaks in string literals
    wellformed_quotes("\r", "\r");
}

#[test_log::test]
fn test_tokenize_default_syntax_string_tab() {
    wellformed_quotes("\t", "\t");
}

#[test_log::test]
fn test_tokenize_default_syntax_string_escape_sequences() {
    wellformed_quotes(r#"\\"#, r#"\"#);
    wellformed_quotes(r#"\""#, r#"""#);
    wellformed_quotes(r#"\'"#, r#"'"#);
    wellformed_quotes(r#"\n"#, "\n");
    wellformed_quotes(r#"\r"#, "\r");
    wellformed_quotes(r#"\t"#, "\t");
}
