use crate::compilation::SourceLocation;
use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, PartialOrd, Ord, Clone)]
pub enum TokenContent {
    Whitespace,
    Identifier(String),
    // =
    Assign,
    // ^
    Caret,
    // (
    LeftParenthesis,
    // )
    RightParenthesis,
    // .
    Dot,
    // "..."
    Quotes(String),
    // =>
    FatArrow,
}

#[derive(PartialEq, Debug)]
pub struct Token {
    pub content: TokenContent,
    pub location: SourceLocation,
}

impl Token {
    pub fn new(content: TokenContent, location: SourceLocation) -> Self {
        Self {
            content: content,
            location,
        }
    }
}

pub struct SourceLocationTrackingInput<Next: hippeus_parser_generator::ReadPeekInput> {
    next: Next,
    current_location: SourceLocation,
}

impl<Next: hippeus_parser_generator::ReadPeekInput> SourceLocationTrackingInput<Next> {
    pub fn new(next: Next, current_location: SourceLocation) -> Self {
        Self {
            next,
            current_location,
        }
    }

    pub fn current_location(&self) -> SourceLocation {
        self.current_location
    }
}

impl<Next: hippeus_parser_generator::ReadPeekInput> hippeus_parser_generator::ReadInput
    for SourceLocationTrackingInput<Next>
{
    fn read_input(&mut self) -> Option<u8> {
        match self.next.read_input() {
            Some(character) => {
                if character == b'\n' {
                    self.current_location.line += 1;
                    self.current_location.column = 0;
                } else {
                    self.current_location.column += 1;
                }
                Some(character)
            }
            None => None,
        }
    }
}

impl<Next: hippeus_parser_generator::ReadPeekInput> hippeus_parser_generator::PeekInput
    for SourceLocationTrackingInput<Next>
{
    fn peek_input(&self) -> Option<u8> {
        self.next.peek_input()
    }
}

impl<Next: hippeus_parser_generator::ReadPeekInput> hippeus_parser_generator::ReadPeekInput
    for SourceLocationTrackingInput<Next>
{
}

fn tokenize(source: &str, syntax: &hippeus_parser_generator::Parser) -> Vec<Token> {
    let mut tokens = Vec::new();
    let mut input = SourceLocationTrackingInput::new(
        hippeus_parser_generator::Slice::new(source),
        SourceLocation::new(0, 0),
    );
    let mut previous_source_location = input.current_location();
    loop {
        match hippeus_parser_generator::parse(syntax, &mut input) {
            hippeus_parser_generator::ParseResult::Success {
                output,
                has_extraneous_input,
            } => {
                if !output.is_empty() {
                    let mut object_buffer = Vec::new();
                    let mut postcard_length_prefix_mode: Option<Vec<u8>> = None;
                    for chunk in &output {
                        match chunk {
                            Some(blob) => match &mut postcard_length_prefix_mode {
                                Some(buffer) => {
                                    buffer.extend_from_slice(&blob);
                                }
                                None => {
                                    object_buffer.extend_from_slice(&blob);
                                }
                            },
                            None => match &mut postcard_length_prefix_mode {
                                Some(buffer) => {
                                    object_buffer =
                                        postcard::to_extend(&buffer, object_buffer).unwrap();
                                    postcard_length_prefix_mode = None;
                                }
                                None => {
                                    postcard_length_prefix_mode = Some(Vec::new());
                                }
                            },
                        }
                    }
                    assert!(postcard_length_prefix_mode.is_none(), "the token parser failed to generate a final separator after a variable-length byte array");
                    let token_content: TokenContent = postcard::from_bytes(&object_buffer[..])
                        .expect("the token parser generated invalid postcard data");
                    tokens.push(Token::new(token_content, previous_source_location));
                }
                if !has_extraneous_input {
                    return tokens;
                }
                let new_source_location = input.current_location();
                assert_ne!(
                    previous_source_location, new_source_location,
                    "something is wrong with the parser if we don't make any forward progress"
                );
                previous_source_location = new_source_location;
            }
            hippeus_parser_generator::ParseResult::Failed => todo!(),
            hippeus_parser_generator::ParseResult::ErrorInParser => {
                panic!("this is a bug in the token parser")
            }
        }
    }
}

pub fn tokenize_default_syntax(source: &str) -> Vec<Token> {
    const IS_END_OF_INPUT: hippeus_parser_generator::RegisterId =
        hippeus_parser_generator::RegisterId(0);
    const STILL_SOMETHING_TO_CHECK: hippeus_parser_generator::RegisterId =
        hippeus_parser_generator::RegisterId(1);
    const FIRST_INPUT: hippeus_parser_generator::RegisterId =
        hippeus_parser_generator::RegisterId(2);
    const IS_ANY_OF_RESULT: hippeus_parser_generator::RegisterId =
        hippeus_parser_generator::RegisterId(3);
    const TOKEN_TAG_WHITESPACE: hippeus_parser_generator::RegisterId =
        hippeus_parser_generator::RegisterId(4);
    const TOKEN_TAG_IDENTIFIER: hippeus_parser_generator::RegisterId =
        hippeus_parser_generator::RegisterId(5);
    const LOOP_CONDITION: hippeus_parser_generator::RegisterId =
        hippeus_parser_generator::RegisterId(6);
    const TOKEN_TAG_ASSIGN: hippeus_parser_generator::RegisterId =
        hippeus_parser_generator::RegisterId(7);
    const SUBSEQUENT_INPUT: hippeus_parser_generator::RegisterId =
        hippeus_parser_generator::RegisterId(8);
    const OUTPUT_BYTE: hippeus_parser_generator::RegisterId =
        hippeus_parser_generator::RegisterId(9);
    const TOKEN_TAG_CARET: hippeus_parser_generator::RegisterId =
        hippeus_parser_generator::RegisterId(10);
    const TOKEN_TAG_LEFT_PARENTHESIS: hippeus_parser_generator::RegisterId =
        hippeus_parser_generator::RegisterId(11);
    const TOKEN_TAG_RIGHT_PARENTHESIS: hippeus_parser_generator::RegisterId =
        hippeus_parser_generator::RegisterId(12);
    const TOKEN_TAG_DOT: hippeus_parser_generator::RegisterId =
        hippeus_parser_generator::RegisterId(13);
    const TOKEN_TAG_QUOTES: hippeus_parser_generator::RegisterId =
        hippeus_parser_generator::RegisterId(14);
    const TOKEN_TAG_FAT_ARROW: hippeus_parser_generator::RegisterId =
        hippeus_parser_generator::RegisterId(15);
    const IF_CONDITION: hippeus_parser_generator::RegisterId =
        hippeus_parser_generator::RegisterId(16);
    lazy_static! {
        static ref TOKEN_PARSER: hippeus_parser_generator::Parser =
            hippeus_parser_generator::Parser::Sequence(vec![
                hippeus_parser_generator::Parser::IsEndOfInput(IS_END_OF_INPUT),
                hippeus_parser_generator::Parser::IfElse(
                    IS_END_OF_INPUT,
                    Box::new(hippeus_parser_generator::Parser::no_op()),
                    Box::new(hippeus_parser_generator::Parser::Sequence(vec![
                        hippeus_parser_generator::Parser::ReadInputByte(FIRST_INPUT),

                        // whitespace
                        hippeus_parser_generator::Parser::IsAnyOf {
                            input: FIRST_INPUT,
                            result: IS_ANY_OF_RESULT,
                            candidates: vec![
                                hippeus_parser_generator::RegisterValue::Byte(b' '),
                                hippeus_parser_generator::RegisterValue::Byte(b'\n')
                            ]
                        },
                        hippeus_parser_generator::Parser::IfElse(
                            IS_ANY_OF_RESULT,
                            Box::new(hippeus_parser_generator::Parser::Sequence(vec![
                                hippeus_parser_generator::Parser::Constant(
                                    TOKEN_TAG_WHITESPACE,
                                    hippeus_parser_generator::RegisterValue::Byte(0)
                                ),
                                hippeus_parser_generator::Parser::WriteOutputByte(
                                    TOKEN_TAG_WHITESPACE
                                )
                            ])),
                            Box::new(hippeus_parser_generator::Parser::no_op())
                        ),

                        // identifier
                        hippeus_parser_generator::Parser::IsAnyOf {
                            input: FIRST_INPUT,
                            result: IS_ANY_OF_RESULT,
                            candidates: (b'a'..b'z').map(|c|
                                hippeus_parser_generator::RegisterValue::Byte( c)).collect(),
                        },
                        hippeus_parser_generator::Parser::IfElse(
                            IS_ANY_OF_RESULT,
                            Box::new(hippeus_parser_generator::Parser::Sequence(vec![
                                hippeus_parser_generator::Parser::Constant(
                                    TOKEN_TAG_IDENTIFIER,
                                    hippeus_parser_generator::RegisterValue::Byte(1)
                                ),
                                hippeus_parser_generator::Parser::WriteOutputByte(
                                    TOKEN_TAG_IDENTIFIER
                                ),
                                // convention: separator starts a variable-length byte array
                                hippeus_parser_generator::Parser::WriteOutputSeparator,
                                hippeus_parser_generator::Parser::Constant(
                                    LOOP_CONDITION,
                                    hippeus_parser_generator::RegisterValue::Boolean(true)
                                ),
                                hippeus_parser_generator::Parser::Copy{from: FIRST_INPUT, to: OUTPUT_BYTE},
                                hippeus_parser_generator::Parser::Loop{condition: LOOP_CONDITION, body: Box::new(
                                    hippeus_parser_generator::Parser::Sequence(vec![
                                        hippeus_parser_generator::Parser::WriteOutputByte(OUTPUT_BYTE ),
                                        hippeus_parser_generator::Parser::IsEndOfInput(IS_END_OF_INPUT),
                                        hippeus_parser_generator::Parser::Not {
                                            from: IS_END_OF_INPUT,
                                            to: LOOP_CONDITION,
                                        },
                                        hippeus_parser_generator::Parser::IfElse(
                                            LOOP_CONDITION,
                                            Box::new(hippeus_parser_generator::Parser::Sequence(vec![
                                                hippeus_parser_generator::Parser::PeekInputByte(SUBSEQUENT_INPUT),
                                                hippeus_parser_generator::Parser::IsAnyOf {
                                                    input: SUBSEQUENT_INPUT,
                                                    result: LOOP_CONDITION,
                                                    candidates: (b'a'..b'z').map(|c|
                                                        hippeus_parser_generator::RegisterValue::Byte(c)).collect(),
                                                },
                                                hippeus_parser_generator::Parser::IfElse(
                                                    LOOP_CONDITION,
                                                    Box::new( hippeus_parser_generator::Parser::Sequence(vec![
                                                        hippeus_parser_generator::Parser::Copy{from: SUBSEQUENT_INPUT, to: OUTPUT_BYTE},
                                                        // pop the byte we had peeked at before
                                                        hippeus_parser_generator::Parser::ReadInputByte(SUBSEQUENT_INPUT),
                                                    ])),
                                                    Box::new(hippeus_parser_generator::Parser::no_op())
                                                ),
                                            ])),
                                            Box::new(hippeus_parser_generator::Parser::no_op())
                                        )
                                    ])
                                )},
                                // convention: separator also ends a variable-length byte array
                                hippeus_parser_generator::Parser::WriteOutputSeparator,
                            ])),
                            Box::new(hippeus_parser_generator::Parser::no_op())
                        ),

                        // assign
                        hippeus_parser_generator::Parser::IsAnyOf {
                            input: FIRST_INPUT,
                            result: IS_ANY_OF_RESULT,
                            candidates: vec![
                                hippeus_parser_generator::RegisterValue::Byte(b'=')
                            ]
                        },
                        hippeus_parser_generator::Parser::IfElse(
                            IS_ANY_OF_RESULT,
                            Box::new(hippeus_parser_generator::Parser::Sequence(vec![
                                hippeus_parser_generator::Parser::Constant(STILL_SOMETHING_TO_CHECK, hippeus_parser_generator::RegisterValue::Boolean(true)),
                                hippeus_parser_generator::Parser::IsEndOfInput(IS_END_OF_INPUT),
                                hippeus_parser_generator::Parser::IfElse(
                                    IS_END_OF_INPUT,
                                    Box::new(hippeus_parser_generator::Parser::no_op()),
                                    Box::new(hippeus_parser_generator::Parser::Sequence(vec![
                                        hippeus_parser_generator::Parser::PeekInputByte(SUBSEQUENT_INPUT),
                                        hippeus_parser_generator::Parser::IsAnyOf {
                                            input: SUBSEQUENT_INPUT,
                                            result: IF_CONDITION,
                                            candidates: vec![
                                                hippeus_parser_generator::RegisterValue::Byte(b'>')
                                            ]
                                        },
                                        hippeus_parser_generator::Parser::IfElse(
                                            IF_CONDITION,
                                            Box::new(hippeus_parser_generator::Parser::Sequence(vec![
                                                hippeus_parser_generator::Parser::ReadInputByte(SUBSEQUENT_INPUT),
                                                hippeus_parser_generator::Parser::Constant(
                                                    TOKEN_TAG_FAT_ARROW,
                                                    hippeus_parser_generator::RegisterValue::Byte(8)
                                                ),
                                                hippeus_parser_generator::Parser::WriteOutputByte(
                                                    TOKEN_TAG_FAT_ARROW
                                                ),
                                                hippeus_parser_generator::Parser::Constant(STILL_SOMETHING_TO_CHECK, hippeus_parser_generator::RegisterValue::Boolean(false)),
                                            ])),
                                            Box::new(hippeus_parser_generator::Parser::no_op())
                                        ),
                                    ])),
                                ),
                                hippeus_parser_generator::Parser::IfElse(
                                    STILL_SOMETHING_TO_CHECK,
                                    Box::new(hippeus_parser_generator::Parser::Sequence(vec![
                                        hippeus_parser_generator::Parser::Constant(
                                            TOKEN_TAG_ASSIGN,
                                            hippeus_parser_generator::RegisterValue::Byte(2)
                                        ),
                                        hippeus_parser_generator::Parser::WriteOutputByte(
                                            TOKEN_TAG_ASSIGN
                                        )
                                    ])),
                                    Box::new(hippeus_parser_generator::Parser::no_op())
                                ),
                            ])),
                            Box::new(hippeus_parser_generator::Parser::no_op())
                        ),

                        // caret
                        hippeus_parser_generator::Parser::IsAnyOf {
                            input: FIRST_INPUT,
                            result: IS_ANY_OF_RESULT,
                            candidates: vec![
                                hippeus_parser_generator::RegisterValue::Byte(b'^')
                            ]
                        },
                        hippeus_parser_generator::Parser::IfElse(
                            IS_ANY_OF_RESULT,
                            Box::new(hippeus_parser_generator::Parser::Sequence(vec![
                                hippeus_parser_generator::Parser::Constant(
                                    TOKEN_TAG_CARET,
                                    hippeus_parser_generator::RegisterValue::Byte(3)
                                ),
                                hippeus_parser_generator::Parser::WriteOutputByte(
                                    TOKEN_TAG_CARET
                                )
                            ])),
                            Box::new(hippeus_parser_generator::Parser::no_op())
                        ),

                        // left parenthesis
                        hippeus_parser_generator::Parser::IsAnyOf {
                            input: FIRST_INPUT,
                            result: IS_ANY_OF_RESULT,
                            candidates: vec![
                                hippeus_parser_generator::RegisterValue::Byte(b'(')
                            ]
                        },
                        hippeus_parser_generator::Parser::IfElse(
                            IS_ANY_OF_RESULT,
                            Box::new(hippeus_parser_generator::Parser::Sequence(vec![
                                hippeus_parser_generator::Parser::Constant(
                                    TOKEN_TAG_LEFT_PARENTHESIS,
                                    hippeus_parser_generator::RegisterValue::Byte(4)
                                ),
                                hippeus_parser_generator::Parser::WriteOutputByte(
                                    TOKEN_TAG_LEFT_PARENTHESIS
                                )
                            ])),
                            Box::new(hippeus_parser_generator::Parser::no_op())
                        ),

                        // right parenthesis
                        hippeus_parser_generator::Parser::IsAnyOf {
                            input: FIRST_INPUT,
                            result: IS_ANY_OF_RESULT,
                            candidates: vec![
                                hippeus_parser_generator::RegisterValue::Byte(b')')
                            ]
                        },
                        hippeus_parser_generator::Parser::IfElse(
                            IS_ANY_OF_RESULT,
                            Box::new(hippeus_parser_generator::Parser::Sequence(vec![
                                hippeus_parser_generator::Parser::Constant(
                                    TOKEN_TAG_RIGHT_PARENTHESIS,
                                    hippeus_parser_generator::RegisterValue::Byte(5)
                                ),
                                hippeus_parser_generator::Parser::WriteOutputByte(
                                    TOKEN_TAG_RIGHT_PARENTHESIS
                                )
                            ])),
                            Box::new(hippeus_parser_generator::Parser::no_op())
                        ),

                        // dot
                        hippeus_parser_generator::Parser::IsAnyOf {
                            input: FIRST_INPUT,
                            result: IS_ANY_OF_RESULT,
                            candidates: vec![
                                hippeus_parser_generator::RegisterValue::Byte(b'.')
                            ]
                        },
                        hippeus_parser_generator::Parser::IfElse(
                            IS_ANY_OF_RESULT,
                            Box::new(hippeus_parser_generator::Parser::Sequence(vec![
                                hippeus_parser_generator::Parser::Constant(
                                    TOKEN_TAG_DOT,
                                    hippeus_parser_generator::RegisterValue::Byte(6)
                                ),
                                hippeus_parser_generator::Parser::WriteOutputByte(
                                    TOKEN_TAG_DOT
                                )
                            ])),
                            Box::new(hippeus_parser_generator::Parser::no_op())
                        ),

                        // quotes
                        hippeus_parser_generator::Parser::IsAnyOf {
                            input: FIRST_INPUT,
                            result: IS_ANY_OF_RESULT,
                            candidates: vec![hippeus_parser_generator::RegisterValue::Byte(b'"')],
                        },
                        hippeus_parser_generator::Parser::IfElse(
                            IS_ANY_OF_RESULT,
                            Box::new(hippeus_parser_generator::Parser::Sequence(vec![
                                hippeus_parser_generator::Parser::Constant(
                                    TOKEN_TAG_QUOTES,
                                    hippeus_parser_generator::RegisterValue::Byte(7)
                                ),
                                hippeus_parser_generator::Parser::WriteOutputByte(
                                    TOKEN_TAG_QUOTES
                                ),
                                // convention: separator starts a variable-length byte array
                                hippeus_parser_generator::Parser::WriteOutputSeparator,
                                hippeus_parser_generator::Parser::Constant(
                                    LOOP_CONDITION,
                                    hippeus_parser_generator::RegisterValue::Boolean(true)
                                ),
                                hippeus_parser_generator::Parser::Loop{condition: LOOP_CONDITION, body: Box::new(
                                    hippeus_parser_generator::Parser::Sequence(vec![
                                        hippeus_parser_generator::Parser::ReadInputByte(SUBSEQUENT_INPUT),
                                        // TODO: support escape sequences
                                        hippeus_parser_generator::Parser::IsAnyOf {
                                            input: SUBSEQUENT_INPUT,
                                            result: IS_ANY_OF_RESULT,
                                            candidates: vec![hippeus_parser_generator::RegisterValue::Byte(b'"')],
                                        },
                                        hippeus_parser_generator::Parser::Not{from: IS_ANY_OF_RESULT, to: LOOP_CONDITION},
                                        hippeus_parser_generator::Parser::IfElse(
                                            IS_ANY_OF_RESULT,
                                            Box::new(hippeus_parser_generator::Parser::no_op()),
                                            Box::new(hippeus_parser_generator::Parser::Sequence(vec![
                                                hippeus_parser_generator::Parser::Copy{from: SUBSEQUENT_INPUT, to: OUTPUT_BYTE},
                                                hippeus_parser_generator::Parser::WriteOutputByte(OUTPUT_BYTE),
                                            ])),
                                        ),
                                    ])
                                )},
                                // convention: separator also ends a variable-length byte array
                                hippeus_parser_generator::Parser::WriteOutputSeparator,
                            ])),
                            Box::new(hippeus_parser_generator::Parser::no_op())
                        )
                    ])),
                ),
            ]);
    }
    tokenize(source, &TOKEN_PARSER)
}

#[cfg(test)]
mod tests {
    use super::*;

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
            " \n  test=\n^().\"\"=>",
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
                    content: TokenContent::Caret,
                    location: SourceLocation { line: 2, column: 0 },
                },
                Token {
                    content: TokenContent::LeftParenthesis,
                    location: SourceLocation { line: 2, column: 1 },
                },
                Token {
                    content: TokenContent::RightParenthesis,
                    location: SourceLocation { line: 2, column: 2 },
                },
                Token {
                    content: TokenContent::Dot,
                    location: SourceLocation { line: 2, column: 3 },
                },
                Token {
                    content: TokenContent::Quotes("".to_string()),
                    location: SourceLocation { line: 2, column: 4 },
                },
                Token {
                    content: TokenContent::FatArrow,
                    location: SourceLocation { line: 2, column: 6 },
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
    fn test_tokenize_default_syntax_caret() {
        test_tokenize_default_syntax(
            "^",
            &[Token {
                content: TokenContent::Caret,
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
}
