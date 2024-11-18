use astraea::{
    storage::StoreValue,
    tree::{HashedValue, TypeId, TypedReference, Value},
};
use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::expressions::{make_lambda, CompilerError, CompilerOutput, Lambda, SourceLocation};

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
}

#[derive(PartialEq, Debug)]
pub struct Token {
    content: TokenContent,
    location: SourceLocation,
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
                            None => {
                                match &mut postcard_length_prefix_mode {
                                    Some(buffer) => {
                                        // https://postcard.jamesmunns.com/wire-format.html#16---byte-array
                                        if buffer.len() > 127 {
                                            todo!("Support variable length byte arrays longer than 127 bytes");
                                        }
                                        object_buffer.push(buffer.len() as u8);
                                        object_buffer.extend_from_slice(&buffer);
                                        postcard_length_prefix_mode = None;
                                    }
                                    None => {
                                        postcard_length_prefix_mode = Some(Vec::new());
                                    }
                                }
                            }
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

fn tokenize_default_syntax(source: &str) -> Vec<Token> {
    const IS_END_OF_INPUT: hippeus_parser_generator::RegisterId =
        hippeus_parser_generator::RegisterId(0);
    const IS_INPUT_AVAILABLE: hippeus_parser_generator::RegisterId =
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
    lazy_static! {
        static ref TOKEN_PARSER: hippeus_parser_generator::Parser =
            hippeus_parser_generator::Parser::Sequence(vec![
                hippeus_parser_generator::Parser::IsEndOfInput(IS_END_OF_INPUT),
                hippeus_parser_generator::Parser::Not {
                    from: IS_END_OF_INPUT,
                    to: IS_INPUT_AVAILABLE,
                },
                hippeus_parser_generator::Parser::Condition(
                    IS_INPUT_AVAILABLE,
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
                        hippeus_parser_generator::Parser::Condition(
                            IS_ANY_OF_RESULT,
                            Box::new(hippeus_parser_generator::Parser::Sequence(vec![
                                hippeus_parser_generator::Parser::Constant(
                                    TOKEN_TAG_WHITESPACE,
                                    hippeus_parser_generator::RegisterValue::Byte(0)
                                ),
                                hippeus_parser_generator::Parser::WriteOutputByte(
                                    TOKEN_TAG_WHITESPACE
                                )
                            ]))
                        ),

                        // identifier
                        hippeus_parser_generator::Parser::IsAnyOf {
                            input: FIRST_INPUT,
                            result: IS_ANY_OF_RESULT,
                            candidates: (b'a'..b'z').map(|c|
                                hippeus_parser_generator::RegisterValue::Byte( c)).collect(),
                        },
                        hippeus_parser_generator::Parser::Condition(
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
                                        },hippeus_parser_generator::Parser::Condition(
                                            LOOP_CONDITION,
                                            Box::new(hippeus_parser_generator::Parser::Sequence(vec![
                                                hippeus_parser_generator::Parser::PeekInputByte(SUBSEQUENT_INPUT),
                                                hippeus_parser_generator::Parser::IsAnyOf {
                                                    input: SUBSEQUENT_INPUT,
                                                    result: LOOP_CONDITION,
                                                    candidates: (b'a'..b'z').map(|c|
                                                        hippeus_parser_generator::RegisterValue::Byte( c)).collect(),
                                                },
                                                hippeus_parser_generator::Parser::Condition(
                                                    LOOP_CONDITION,
                                                    Box::new( hippeus_parser_generator::Parser::Sequence(vec![
                                                        hippeus_parser_generator::Parser::Copy{from: SUBSEQUENT_INPUT, to: OUTPUT_BYTE},
                                                        // pop the byte we had peeked at before
                                                        hippeus_parser_generator::Parser::ReadInputByte(SUBSEQUENT_INPUT),
                                                        ]))),
                                            ]))),
                                    ])
                                )},
                                // convention: separator also ends a variable-length byte array
                                hippeus_parser_generator::Parser::WriteOutputSeparator,
                            ]))
                        ),

                        // assign
                        hippeus_parser_generator::Parser::IsAnyOf {
                            input: FIRST_INPUT,
                            result: IS_ANY_OF_RESULT,
                            candidates: vec![
                                hippeus_parser_generator::RegisterValue::Byte(b'=')
                            ]
                        },
                        hippeus_parser_generator::Parser::Condition(
                            IS_ANY_OF_RESULT,
                            Box::new(hippeus_parser_generator::Parser::Sequence(vec![
                                hippeus_parser_generator::Parser::Constant(
                                    TOKEN_TAG_ASSIGN,
                                    hippeus_parser_generator::RegisterValue::Byte(2)
                                ),
                                hippeus_parser_generator::Parser::WriteOutputByte(
                                    TOKEN_TAG_ASSIGN
                                )
                            ]))
                        ),

                        // caret
                        hippeus_parser_generator::Parser::IsAnyOf {
                            input: FIRST_INPUT,
                            result: IS_ANY_OF_RESULT,
                            candidates: vec![
                                hippeus_parser_generator::RegisterValue::Byte(b'^')
                            ]
                        },
                        hippeus_parser_generator::Parser::Condition(
                            IS_ANY_OF_RESULT,
                            Box::new(hippeus_parser_generator::Parser::Sequence(vec![
                                hippeus_parser_generator::Parser::Constant(
                                    TOKEN_TAG_CARET,
                                    hippeus_parser_generator::RegisterValue::Byte(3)
                                ),
                                hippeus_parser_generator::Parser::WriteOutputByte(
                                    TOKEN_TAG_CARET
                                )
                            ]))
                        ),

                        // left parenthesis
                        hippeus_parser_generator::Parser::IsAnyOf {
                            input: FIRST_INPUT,
                            result: IS_ANY_OF_RESULT,
                            candidates: vec![
                                hippeus_parser_generator::RegisterValue::Byte(b'(')
                            ]
                        },
                        hippeus_parser_generator::Parser::Condition(
                            IS_ANY_OF_RESULT,
                            Box::new(hippeus_parser_generator::Parser::Sequence(vec![
                                hippeus_parser_generator::Parser::Constant(
                                    TOKEN_TAG_LEFT_PARENTHESIS,
                                    hippeus_parser_generator::RegisterValue::Byte(4)
                                ),
                                hippeus_parser_generator::Parser::WriteOutputByte(
                                    TOKEN_TAG_LEFT_PARENTHESIS
                                )
                            ]))
                        ),

                        // right parenthesis
                        hippeus_parser_generator::Parser::IsAnyOf {
                            input: FIRST_INPUT,
                            result: IS_ANY_OF_RESULT,
                            candidates: vec![
                                hippeus_parser_generator::RegisterValue::Byte(b')')
                            ]
                        },
                        hippeus_parser_generator::Parser::Condition(
                            IS_ANY_OF_RESULT,
                            Box::new(hippeus_parser_generator::Parser::Sequence(vec![
                                hippeus_parser_generator::Parser::Constant(
                                    TOKEN_TAG_RIGHT_PARENTHESIS,
                                    hippeus_parser_generator::RegisterValue::Byte(5)
                                ),
                                hippeus_parser_generator::Parser::WriteOutputByte(
                                    TOKEN_TAG_RIGHT_PARENTHESIS
                                )
                            ]))
                        ),

                        // dot
                        hippeus_parser_generator::Parser::IsAnyOf {
                            input: FIRST_INPUT,
                            result: IS_ANY_OF_RESULT,
                            candidates: vec![
                                hippeus_parser_generator::RegisterValue::Byte(b'.')
                            ]
                        },
                        hippeus_parser_generator::Parser::Condition(
                            IS_ANY_OF_RESULT,
                            Box::new(hippeus_parser_generator::Parser::Sequence(vec![
                                hippeus_parser_generator::Parser::Constant(
                                    TOKEN_TAG_DOT,
                                    hippeus_parser_generator::RegisterValue::Byte(6)
                                ),
                                hippeus_parser_generator::Parser::WriteOutputByte(
                                    TOKEN_TAG_DOT
                                )
                            ]))
                        ),
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
            " \n  test=\n^().",
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
}

fn pop_next_non_whitespace_token<'t>(tokens: &'t mut std::slice::Iter<Token>) -> Option<&'t Token> {
    loop {
        let next = tokens.next();
        match next {
            Some(token) => match token.content {
                TokenContent::Whitespace => continue,
                TokenContent::Identifier(_) => return next,
                TokenContent::Assign => return next,
                TokenContent::Caret => return next,
                TokenContent::LeftParenthesis => return next,
                TokenContent::RightParenthesis => return next,
                TokenContent::Dot => return next,
            },
            None => return None,
        }
    }
}

fn expect_dot(tokens: &mut std::slice::Iter<Token>) {
    match pop_next_non_whitespace_token(tokens) {
        Some(non_whitespace) => match &non_whitespace.content {
            TokenContent::Whitespace => todo!(),
            TokenContent::Identifier(_) => todo!(),
            TokenContent::Assign => todo!(),
            TokenContent::Caret => todo!(),
            TokenContent::LeftParenthesis => todo!(),
            TokenContent::RightParenthesis => todo!(),
            TokenContent::Dot => {}
        },
        None => todo!(),
    }
}

async fn parse_expression<'t>(
    tokens: &mut std::slice::Iter<'t, Token>,
    storage: &dyn StoreValue,
) -> TypedReference {
    match pop_next_non_whitespace_token(tokens) {
        Some(non_whitespace) => match &non_whitespace.content {
            TokenContent::Whitespace => todo!(),
            TokenContent::Identifier(identifier) => storage
                .store_value(&HashedValue::from(Arc::new(
                    Value::from_string(&identifier).unwrap(/*TODO*/),
                )))
                .await
                .unwrap()
                .add_type(TypeId(0)),
            TokenContent::Assign => todo!(),
            TokenContent::Caret => todo!(),
            TokenContent::LeftParenthesis => todo!(),
            TokenContent::RightParenthesis => todo!(),
            TokenContent::Dot => todo!(),
        },
        None => todo!(),
    }
}

async fn parse_lambda<'t>(
    tokens: &mut std::slice::Iter<'t, Token>,
    storage: &dyn StoreValue,
) -> TypedReference {
    let parameter_name = match pop_next_non_whitespace_token(tokens) {
        Some(non_whitespace) => match &non_whitespace.content {
            TokenContent::Whitespace => todo!(),
            TokenContent::Identifier(identifier) => identifier,
            TokenContent::Assign => todo!(),
            TokenContent::Caret => todo!(),
            TokenContent::LeftParenthesis => todo!(),
            TokenContent::RightParenthesis => todo!(),
            TokenContent::Dot => todo!(),
        },
        None => todo!(),
    };
    let parameter = storage
        .store_value(&HashedValue::from(Arc::new(
            Value::from_string(parameter_name).unwrap(/*TODO*/),
        )))
        .await
        .unwrap()
        .add_type(TypeId(0));
    expect_dot(tokens);
    let body = parse_expression(tokens, storage).await;
    let result = storage
        .store_value(&HashedValue::from(Arc::new(
            make_lambda(Lambda::new(parameter, body)).value,
        )))
        .await
        .unwrap()
        .add_type(TypeId(7));
    result
}

pub async fn parse_entry_point_lambda<'t>(
    tokens: &mut std::slice::Iter<'t, Token>,
    storage: &dyn StoreValue,
) -> CompilerOutput {
    let mut errors = Vec::new();
    match pop_next_non_whitespace_token(tokens) {
        Some(non_whitespace) => match non_whitespace.content {
            TokenContent::Whitespace => todo!(),
            TokenContent::Identifier(_) => todo!(),
            TokenContent::Assign => todo!(),
            TokenContent::Caret => {
                let entry_point = parse_lambda(tokens, storage).await;
                CompilerOutput::new(entry_point, errors)
            }
            TokenContent::LeftParenthesis => todo!(),
            TokenContent::RightParenthesis => todo!(),
            TokenContent::Dot => todo!(),
        },
        None => {
            errors.push(CompilerError::new(
                "Expected entry point lambda".to_string(),
                SourceLocation::new(0, 0),
            ));
            let entry_point = storage
                .store_value(&HashedValue::from(Arc::new(Value::from_unit())))
                .await
                .unwrap()
                .add_type(TypeId(1));
            CompilerOutput::new(entry_point, errors)
        }
    }
}

pub async fn compile(source: &str, storage: &dyn StoreValue) -> CompilerOutput {
    let tokens = tokenize_default_syntax(source);
    let mut token_iterator = tokens.iter();
    let mut result = parse_entry_point_lambda(&mut token_iterator, storage).await;
    match pop_next_non_whitespace_token(&mut token_iterator) {
        Some(extra_token) => {
            result.errors.push(CompilerError::new(
                "Unexpected token after the entry point lambda".to_string(),
                extra_token.location,
            ));
        }
        None => {}
    }
    result
}

#[cfg(test)]
mod tests2 {
    use super::*;
    use astraea::storage::InMemoryValueStorage;
    use tokio::sync::Mutex;

    #[test_log::test(tokio::test)]
    async fn test_compile_empty_source() {
        let value_storage =
            InMemoryValueStorage::new(Mutex::new(std::collections::BTreeMap::new()));
        let output = compile("", &value_storage).await;
        let expected = CompilerOutput::new(
            value_storage
                .store_value(&HashedValue::from(Arc::new(Value::from_unit())))
                .await
                .unwrap()
                .add_type(TypeId(1)),
            vec![CompilerError::new(
                "Expected entry point lambda".to_string(),
                SourceLocation::new(0, 0),
            )],
        );
        assert_eq!(expected, output);
        assert_eq!(1, value_storage.len().await);
    }

    #[test_log::test(tokio::test)]
    async fn test_compile_simple_program() {
        let value_storage =
            InMemoryValueStorage::new(Mutex::new(std::collections::BTreeMap::new()));
        let output = compile(r#"^x . x"#, &value_storage).await;
        let parameter = value_storage
            .store_value(&HashedValue::from(Arc::new(
                Value::from_string("x").unwrap(),
            )))
            .await
            .unwrap()
            .add_type(TypeId(0));
        let entry_point = value_storage
            .store_value(&HashedValue::from(Arc::new(
                make_lambda(Lambda::new(parameter, parameter)).value,
            )))
            .await
            .unwrap()
            .add_type(TypeId(7));
        let expected = CompilerOutput::new(entry_point, Vec::new());
        assert_eq!(expected, output);
        assert_eq!(2, value_storage.len().await);
    }
}
