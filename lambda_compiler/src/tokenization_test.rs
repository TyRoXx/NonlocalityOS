use crate::{
    compilation::SourceLocation,
    tokenization::{tokenize_default_syntax, Token, TokenContent},
};

fn test_tokenize_default_syntax(source: &str, expected_tokens: &[Token]) {
    let tokenized = tokenize_default_syntax(source);
    assert_eq!(&expected_tokens[..], &tokenized[..]);
}

#[test]
fn test_tokenize_default_syntax_empty_source() {
    test_tokenize_default_syntax("", &[]);
}

#[test]
fn test_tokenize_default_syntax_space() {
    test_tokenize_default_syntax(
        " ",
        &[Token {
            content: TokenContent::Whitespace,
            location: SourceLocation { line: 0, column: 0 },
        }],
    );
}

#[test]
fn test_tokenize_default_syntax_newline() {
    test_tokenize_default_syntax(
        "\n",
        &[Token {
            content: TokenContent::Whitespace,
            location: SourceLocation { line: 0, column: 0 },
        }],
    );
}

#[test]
fn test_tokenize_default_syntax_source_locations() {
    test_tokenize_default_syntax(
        " \n  test=\n().\"\"=>",
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
        ],
    );
}

#[test]
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
        ],
    );
}

#[test]
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
        ],
    );
}

#[test]
fn test_tokenize_default_syntax_identifier() {
    test_tokenize_default_syntax(
        "test",
        &[Token {
            content: TokenContent::Identifier("test".to_string()),
            location: SourceLocation { line: 0, column: 0 },
        }],
    );
}

#[test]
fn test_tokenize_default_syntax_assign() {
    test_tokenize_default_syntax(
        "=",
        &[Token {
            content: TokenContent::Assign,
            location: SourceLocation { line: 0, column: 0 },
        }],
    );
}

#[test]
fn test_tokenize_default_syntax_left_parenthesis() {
    test_tokenize_default_syntax(
        "(",
        &[Token {
            content: TokenContent::LeftParenthesis,
            location: SourceLocation { line: 0, column: 0 },
        }],
    );
}

#[test]
fn test_tokenize_default_syntax_right_parenthesis() {
    test_tokenize_default_syntax(
        ")",
        &[Token {
            content: TokenContent::RightParenthesis,
            location: SourceLocation { line: 0, column: 0 },
        }],
    );
}

#[test]
fn test_tokenize_default_syntax_dot() {
    test_tokenize_default_syntax(
        ".",
        &[Token {
            content: TokenContent::Dot,
            location: SourceLocation { line: 0, column: 0 },
        }],
    );
}

#[test]
fn test_tokenize_default_syntax_fat_arrow() {
    test_tokenize_default_syntax(
        "=>",
        &[Token {
            content: TokenContent::FatArrow,
            location: SourceLocation { line: 0, column: 0 },
        }],
    );
}

fn wellformed_quotes(string_content: &str) {
    test_tokenize_default_syntax(
        &format!("\"{}\"", string_content),
        &[Token {
            content: TokenContent::Quotes(string_content.to_string()),
            location: SourceLocation { line: 0, column: 0 },
        }],
    );
}

#[test]
fn test_tokenize_default_syntax_string_empty() {
    wellformed_quotes("");
}

#[test]
fn test_tokenize_default_syntax_string_short() {
    wellformed_quotes("hello");
}

#[test]
fn test_tokenize_default_syntax_string_longer() {
    wellformed_quotes(&std::iter::repeat_n('A', 1000).collect::<String>());
}

#[test]
fn test_tokenize_default_syntax_string_escape_sequences() {
    // TODO: support escape sequences, test \"
    wellformed_quotes("\\\\");
}
