use crate::tree::{CompilerOutput, InMemoryValueStorage, LoadValue, StoreValue, TypeId, Value};
use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, PartialOrd, Ord, Clone)]
pub enum TokenContent {
    Whitespace,
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Copy)]
pub struct SourceLocation {
    pub line: usize,
    pub column: usize,
}

impl SourceLocation {
    pub fn new(line: usize, column: usize) -> Self {
        Self { line, column }
    }
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
                for chunk in &output {
                    tokens.push(match chunk {
                        Some(blob) => {
                            let token_content: TokenContent = postcard::from_bytes(&blob[..])
                                .expect("the token parser generated invalid postcard data");
                            Token::new(token_content, previous_source_location)
                        }
                        None => todo!(),
                    });
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
    const INPUT: hippeus_parser_generator::RegisterId = hippeus_parser_generator::RegisterId(2);
    const IS_ANY_OF_RESULT: hippeus_parser_generator::RegisterId =
        hippeus_parser_generator::RegisterId(3);
    const TOKEN_TAG_WHITESPACE: hippeus_parser_generator::RegisterId =
        hippeus_parser_generator::RegisterId(4);
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
                        hippeus_parser_generator::Parser::ReadInputByte(INPUT),
                        hippeus_parser_generator::Parser::IsAnyOf {
                            input: INPUT,
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
                        )
                    ])),
                ),
            ]);
    }
    tokenize(source, &TOKEN_PARSER)
}

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

pub fn compile(source: &str, loader: &dyn LoadValue, storage: &dyn StoreValue) -> CompilerOutput {
    let errors = Vec::new();
    let tokens = tokenize_default_syntax(source);
    let entry_point = storage
        .store_value(Arc::new(Value::from_unit()))
        .add_type(TypeId(1));
    CompilerOutput::new(entry_point, errors)
}

#[test]
fn test_compile_empty_source() {
    let value_storage =
        InMemoryValueStorage::new(std::sync::Mutex::new(std::collections::BTreeMap::new()));
    let output = compile("", &value_storage, &value_storage);
    let expected = CompilerOutput::new(
        value_storage
            .store_value(Arc::new(Value::from_unit()))
            .add_type(TypeId(1)),
        Vec::new(),
    );
    assert_eq!(expected, output);
    assert_eq!(1, value_storage.len());
}
