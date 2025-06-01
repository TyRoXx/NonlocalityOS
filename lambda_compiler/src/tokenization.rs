use crate::compilation::SourceLocation;
use hippeus_parser_generator::{ParseResult, Parser, RegisterId, RegisterValue};
use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, PartialOrd, Ord, Clone)]
pub enum TokenContent {
    Whitespace,
    Identifier(String),
    // =
    Assign,
    // (
    LeftParenthesis,
    // )
    RightParenthesis,
    // [
    LeftBracket,
    // ]
    RightBracket,
    // .
    Dot,
    // "..."
    Quotes(String),
    // =>
    FatArrow,
    // ,
    Comma,
    EndOfFile,
    // {
    LeftBrace,
    // }
    RightBrace,
    // :
    Colon,
    // #
    Comment(String),
}

#[derive(PartialEq, Debug)]
pub struct Token {
    pub content: TokenContent,
    pub location: SourceLocation,
}

impl Token {
    pub fn new(content: TokenContent, location: SourceLocation) -> Self {
        Self { content, location }
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

fn tokenize(source: &str, syntax: &Parser) -> Vec<Token> {
    let mut tokens = Vec::new();
    let mut input = SourceLocationTrackingInput::new(
        hippeus_parser_generator::Slice::new(source),
        SourceLocation::new(0, 0),
    );
    let mut previous_source_location = input.current_location();
    loop {
        match hippeus_parser_generator::parse(syntax, &mut input) {
            ParseResult::Success {
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
                                    buffer.extend_from_slice(blob);
                                }
                                None => {
                                    object_buffer.extend_from_slice(blob);
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
                    tokens.push(Token::new(
                        TokenContent::EndOfFile,
                        input.current_location(),
                    ));
                    return tokens;
                }
                let new_source_location = input.current_location();
                assert_ne!(
                    previous_source_location, new_source_location,
                    "something is wrong with the parser if we don't make any forward progress"
                );
                previous_source_location = new_source_location;
            }
            ParseResult::Failed => todo!(),
            ParseResult::ErrorInParser => {
                panic!("this is a bug in the token parser")
            }
        }
    }
}

pub fn tokenize_default_syntax(source: &str) -> Vec<Token> {
    const IS_END_OF_INPUT: RegisterId = RegisterId(0);
    const STILL_SOMETHING_TO_CHECK: RegisterId = RegisterId(1);
    const FIRST_INPUT: RegisterId = RegisterId(2);
    const IS_ANY_OF_RESULT: RegisterId = RegisterId(3);
    const TOKEN_TAG_WHITESPACE: RegisterId = RegisterId(4);
    const TOKEN_TAG_IDENTIFIER: RegisterId = RegisterId(5);
    const LOOP_CONDITION: RegisterId = RegisterId(6);
    const TOKEN_TAG_ASSIGN: RegisterId = RegisterId(7);
    const SUBSEQUENT_INPUT: RegisterId = RegisterId(8);
    const OUTPUT_BYTE: RegisterId = RegisterId(9);
    const IF_CONDITION: RegisterId = RegisterId(10);
    const TOKEN_TAG_LEFT_PARENTHESIS: RegisterId = RegisterId(11);
    const TOKEN_TAG_RIGHT_PARENTHESIS: RegisterId = RegisterId(12);
    const TOKEN_TAG_LEFT_BRACKET: RegisterId = RegisterId(13);
    const TOKEN_TAG_RIGHT_BRACKET: RegisterId = RegisterId(14);
    const TOKEN_TAG_DOT: RegisterId = RegisterId(15);
    const TOKEN_TAG_COMMA: RegisterId = RegisterId(16);
    const TOKEN_TAG_QUOTES: RegisterId = RegisterId(17);
    const TOKEN_TAG_FAT_ARROW: RegisterId = RegisterId(18);
    const TOKEN_TAG_LEFT_BRACE: RegisterId = RegisterId(19);
    const TOKEN_TAG_RIGHT_BRACE: RegisterId = RegisterId(20);
    const TOKEN_TAG_COLON: RegisterId = RegisterId(21);
    const TOKEN_TAG_COMMENT: RegisterId = RegisterId(22);
    lazy_static! {
        static ref IDENTIFIER_CHARACTERS: Vec<RegisterValue> = (b'a'..=b'z').chain(b'A'..=b'Z').map(RegisterValue::Byte).collect();
        static ref COPY_SUBSEQUENT_INPUT_TO_OUTPUT: Parser = Parser::Sequence(vec![
                                    Parser::Copy{from: SUBSEQUENT_INPUT, to: OUTPUT_BYTE},
                                    Parser::WriteOutputByte(OUTPUT_BYTE),
                                ]);
        static ref QUOTES_PARSING: [Parser; 2] = [
            // quotes
            Parser::IsAnyOf {
                input: FIRST_INPUT,
                result: IS_ANY_OF_RESULT,
                candidates: vec![RegisterValue::Byte(b'"')],
            },
            Parser::IfElse(
                IS_ANY_OF_RESULT,
                Box::new(Parser::Sequence(vec![
                    Parser::Constant(TOKEN_TAG_QUOTES, RegisterValue::Byte(8)),
                    Parser::WriteOutputByte(TOKEN_TAG_QUOTES),
                    // convention: separator starts a variable-length byte array
                    Parser::WriteOutputSeparator,
                    Parser::Constant(LOOP_CONDITION, RegisterValue::Boolean(true)),
                    Parser::Loop{condition: LOOP_CONDITION, body: Box::new(
                        Parser::Sequence(vec![
                            Parser::ReadInputByte(SUBSEQUENT_INPUT),
                            Parser::Match {
                                input: SUBSEQUENT_INPUT,
                                cases: BTreeMap::from([
                                    (RegisterValue::Byte(b'"'), Parser::Constant(LOOP_CONDITION, RegisterValue::Boolean(false))),
                                    (RegisterValue::Byte(b'\\'), Parser::Sequence(vec![
                                        Parser::ReadInputByte(SUBSEQUENT_INPUT),
                                        Parser::Match {
                                            input: SUBSEQUENT_INPUT,
                                            cases: BTreeMap::from([
                                                (RegisterValue::Byte(b'\\'), COPY_SUBSEQUENT_INPUT_TO_OUTPUT.clone()),
                                                (RegisterValue::Byte(b'\"'), COPY_SUBSEQUENT_INPUT_TO_OUTPUT.clone()),
                                                (RegisterValue::Byte(b'\''), COPY_SUBSEQUENT_INPUT_TO_OUTPUT.clone()),
                                                (RegisterValue::Byte(b'n'), Parser::Sequence(vec![
                                                    Parser::Constant(OUTPUT_BYTE, RegisterValue::Byte(b'\n')),
                                                    Parser::WriteOutputByte(OUTPUT_BYTE),
                                                ])),
                                                (RegisterValue::Byte(b'r'), Parser::Sequence(vec![
                                                    Parser::Constant(OUTPUT_BYTE, RegisterValue::Byte(b'\r')),
                                                    Parser::WriteOutputByte(OUTPUT_BYTE),
                                                ])),
                                                (RegisterValue::Byte(b't'), Parser::Sequence(vec![
                                                    Parser::Constant(OUTPUT_BYTE, RegisterValue::Byte(b'\t')),
                                                    Parser::WriteOutputByte(OUTPUT_BYTE),
                                                ]))
                                            ]),
                                            default: Box::new(Parser::Fail),
                                        }
                                    ])),
                                ]),
                                default: Box::new(COPY_SUBSEQUENT_INPUT_TO_OUTPUT.clone()),
                            },
                        ])
                    )},
                    // convention: separator also ends a variable-length byte array
                    Parser::WriteOutputSeparator,
                ])),
                Box::new(Parser::no_op())
            )];

        static ref COMMENT_PARSING: [Parser; 2]  = [
            Parser::IsAnyOf {
                input: FIRST_INPUT,
                result: IS_ANY_OF_RESULT,
                candidates: vec![RegisterValue::Byte(b'#')],
            },
            Parser::IfElse(
                IS_ANY_OF_RESULT,
                Box::new(Parser::Sequence(vec![
                    Parser::Constant(TOKEN_TAG_COMMENT, RegisterValue::Byte(15)),
                    Parser::WriteOutputByte(TOKEN_TAG_COMMENT),
                    // convention: separator starts a variable-length byte array
                    Parser::WriteOutputSeparator,
                    Parser::Constant(
                        LOOP_CONDITION,
                        RegisterValue::Boolean(true)
                    ),
                    Parser::Loop{condition: LOOP_CONDITION, body: Box::new(
                        Parser::Sequence(vec![
                            Parser::IsEndOfInput(IS_END_OF_INPUT),
                            Parser::Not{from: IS_END_OF_INPUT, to: LOOP_CONDITION},
                            Parser::IfElse(
                                LOOP_CONDITION,
                                Box::new(Parser::Sequence(vec![
                                    Parser::ReadInputByte(SUBSEQUENT_INPUT),
                                    Parser::IsAnyOf {
                                        input: SUBSEQUENT_INPUT,
                                        result: IS_ANY_OF_RESULT,
                                        candidates: vec![RegisterValue::Byte(b'\n')],
                                    },
                                    Parser::Not{from: IS_ANY_OF_RESULT, to: LOOP_CONDITION},
                                    Parser::IfElse(
                                        IS_ANY_OF_RESULT,
                                        Box::new(Parser::no_op()),
                                        Box::new(Parser::Sequence(vec![
                                            Parser::Copy{from: SUBSEQUENT_INPUT, to: OUTPUT_BYTE},
                                            Parser::WriteOutputByte(OUTPUT_BYTE),
                                        ])),
                                    ),
                                ])),
                                Box::new(Parser::no_op())
                            )
                        ])
                    )},
                    // convention: separator also ends a variable-length byte array
                    Parser::WriteOutputSeparator,
                ])),
                Box::new(Parser::no_op())
            )
        ];


        static ref TOKEN_PARSER: Parser =
            Parser::Sequence(vec![
                Parser::IsEndOfInput(IS_END_OF_INPUT),
                Parser::IfElse(
                    IS_END_OF_INPUT,
                    Box::new(Parser::no_op()),
                    Box::new(Parser::Sequence([
                        Parser::ReadInputByte(FIRST_INPUT),

                        // whitespace
                        Parser::IsAnyOf {
                            input: FIRST_INPUT,
                            result: IS_ANY_OF_RESULT,
                            candidates: vec![
                                RegisterValue::Byte(b' '),
                                RegisterValue::Byte(b'\n')
                            ]
                        },
                        Parser::IfElse(
                            IS_ANY_OF_RESULT,
                            Box::new(Parser::Sequence(vec![
                                Parser::Constant(TOKEN_TAG_WHITESPACE, RegisterValue::Byte(0)),
                                Parser::WriteOutputByte(TOKEN_TAG_WHITESPACE)
                            ])),
                            Box::new(Parser::no_op())
                        ),

                        // identifier
                        Parser::IsAnyOf {
                            input: FIRST_INPUT,
                            result: IS_ANY_OF_RESULT,
                            candidates: IDENTIFIER_CHARACTERS.clone(),
                        },
                        Parser::IfElse(
                            IS_ANY_OF_RESULT,
                            Box::new(Parser::Sequence(vec![
                                Parser::Constant(TOKEN_TAG_IDENTIFIER, RegisterValue::Byte(1)),
                                Parser::WriteOutputByte(TOKEN_TAG_IDENTIFIER),
                                // convention: separator starts a variable-length byte array
                                Parser::WriteOutputSeparator,
                                Parser::Constant(LOOP_CONDITION, RegisterValue::Boolean(true)),
                                Parser::Copy{from: FIRST_INPUT, to: OUTPUT_BYTE},
                                Parser::Loop{condition: LOOP_CONDITION, body: Box::new(
                                    Parser::Sequence(vec![
                                        Parser::WriteOutputByte(OUTPUT_BYTE),
                                        Parser::IsEndOfInput(IS_END_OF_INPUT),
                                        Parser::Not { from: IS_END_OF_INPUT, to: LOOP_CONDITION },
                                        Parser::IfElse(
                                            LOOP_CONDITION,
                                            Box::new(Parser::Sequence(vec![
                                                Parser::PeekInputByte(SUBSEQUENT_INPUT),
                                                Parser::IsAnyOf {
                                                    input: SUBSEQUENT_INPUT,
                                                    result: LOOP_CONDITION,
                                                    candidates: IDENTIFIER_CHARACTERS.clone(),
                                                },
                                                Parser::IfElse(
                                                    LOOP_CONDITION,
                                                    Box::new( Parser::Sequence(vec![
                                                        Parser::Copy{from: SUBSEQUENT_INPUT, to: OUTPUT_BYTE},
                                                        // pop the byte we had peeked at before
                                                        Parser::ReadInputByte(SUBSEQUENT_INPUT),
                                                    ])),
                                                    Box::new(Parser::no_op())
                                                ),
                                            ])),
                                            Box::new(Parser::no_op())
                                        )
                                    ])
                                )},
                                // convention: separator also ends a variable-length byte array
                                Parser::WriteOutputSeparator,
                            ])),
                            Box::new(Parser::no_op())
                        ),

                        // assign
                        Parser::IsAnyOf {
                            input: FIRST_INPUT,
                            result: IS_ANY_OF_RESULT,
                            candidates: vec![
                                RegisterValue::Byte(b'=')
                            ]
                        },
                        Parser::IfElse(
                            IS_ANY_OF_RESULT,
                            Box::new(Parser::Sequence(vec![
                                Parser::Constant(STILL_SOMETHING_TO_CHECK, RegisterValue::Boolean(true)),
                                Parser::IsEndOfInput(IS_END_OF_INPUT),
                                Parser::IfElse(
                                    IS_END_OF_INPUT,
                                    Box::new(Parser::no_op()),
                                    Box::new(Parser::Sequence(vec![
                                        Parser::PeekInputByte(SUBSEQUENT_INPUT),
                                        Parser::IsAnyOf {
                                            input: SUBSEQUENT_INPUT,
                                            result: IF_CONDITION,
                                            candidates: vec![
                                                RegisterValue::Byte(b'>')
                                            ]
                                        },
                                        Parser::IfElse(
                                            IF_CONDITION,
                                            Box::new(Parser::Sequence(vec![
                                                Parser::ReadInputByte(SUBSEQUENT_INPUT),
                                                Parser::Constant(TOKEN_TAG_FAT_ARROW, RegisterValue::Byte(9)),
                                                Parser::WriteOutputByte(TOKEN_TAG_FAT_ARROW),
                                                Parser::Constant(STILL_SOMETHING_TO_CHECK, RegisterValue::Boolean(false)),
                                            ])),
                                            Box::new(Parser::no_op())
                                        ),
                                    ])),
                                ),
                                Parser::IfElse(
                                    STILL_SOMETHING_TO_CHECK,
                                    Box::new(Parser::Sequence(vec![
                                        Parser::Constant(TOKEN_TAG_ASSIGN, RegisterValue::Byte(2)),
                                        Parser::WriteOutputByte(TOKEN_TAG_ASSIGN)
                                    ])),
                                    Box::new(Parser::no_op())
                                ),
                            ])),
                            Box::new(Parser::no_op())
                        ),
                    ]
                    .into_iter()
                    .chain(QUOTES_PARSING.clone())
                    .chain(COMMENT_PARSING.clone())
                    .chain(parse_single_character(FIRST_INPUT, IS_ANY_OF_RESULT, TOKEN_TAG_LEFT_PARENTHESIS, b'(', 3))
                    .chain(parse_single_character(FIRST_INPUT, IS_ANY_OF_RESULT, TOKEN_TAG_RIGHT_PARENTHESIS, b')', 4))
                    .chain(parse_single_character(FIRST_INPUT, IS_ANY_OF_RESULT, TOKEN_TAG_LEFT_BRACKET, b'[', 5))
                    .chain(parse_single_character(FIRST_INPUT, IS_ANY_OF_RESULT, TOKEN_TAG_RIGHT_BRACKET, b']', 6))
                    .chain(parse_single_character(FIRST_INPUT, IS_ANY_OF_RESULT, TOKEN_TAG_DOT, b'.', 7))
                    .chain(parse_single_character(FIRST_INPUT, IS_ANY_OF_RESULT, TOKEN_TAG_COMMA, b',', 10))
                    .chain(parse_single_character(FIRST_INPUT, IS_ANY_OF_RESULT, TOKEN_TAG_COLON, b':', 14))
                    .chain(parse_single_character(FIRST_INPUT, IS_ANY_OF_RESULT, TOKEN_TAG_RIGHT_BRACE, b'}', 13))
                    .chain(parse_single_character(FIRST_INPUT, IS_ANY_OF_RESULT, TOKEN_TAG_LEFT_BRACE, b'{', 12))
                    .collect())),
                ),
            ]);
    }
    tokenize(source, &TOKEN_PARSER)
}

fn parse_single_character(
    first_input: RegisterId,
    is_any_of_result: RegisterId,
    token_tag: RegisterId,
    character_to_match: u8,
    out_byte: u8,
) -> [Parser; 2] {
    [
        // comma
        Parser::IsAnyOf {
            input: first_input,
            result: is_any_of_result,
            candidates: vec![RegisterValue::Byte(character_to_match)],
        },
        Parser::IfElse(
            is_any_of_result,
            Box::new(Parser::Sequence(vec![
                Parser::Constant(token_tag, RegisterValue::Byte(out_byte)),
                Parser::WriteOutputByte(token_tag),
            ])),
            Box::new(Parser::no_op()),
        ),
    ]
}
