use crate::{is_match, parse, ParseResult, Parser, RegisterId, RegisterValue, Slice};

#[test_log::test]
fn test_empty_parser() {
    let parser = Parser::Sequence(vec![]);
    assert_eq!(Some(true), is_match(&parser, &mut Slice::new("")));
    assert_eq!(Some(false), is_match(&parser, &mut Slice::new("a")));
    assert_eq!(
        Some(false),
        is_match(&parser, &mut Slice::new("aaaaaaaaaaaaaaaaaa"))
    );
}

#[test_log::test]
fn test_fail() {
    let parser = Parser::Sequence(vec![
        Parser::IsEndOfInput(RegisterId(0)),
        Parser::Not {
            from: RegisterId(0),
            to: RegisterId(1),
        },
        Parser::IfElse(
            RegisterId(1),
            Box::new(Parser::Fail),
            Box::new(Parser::no_op()),
        ),
    ]);
    assert_eq!(Some(true), is_match(&parser, &mut Slice::new("")));
    assert_eq!(Some(false), is_match(&parser, &mut Slice::new("a")));
    assert_eq!(
        Some(false),
        is_match(&parser, &mut Slice::new("aaaaaaaaaaaaaaaaaa"))
    );
}

#[test_log::test]
fn test_extraneous_input() {
    let parser = Parser::Sequence(vec![]);
    let result = parse(&parser, &mut Slice::new("a"));
    match result {
        ParseResult::Success {
            output,
            has_extraneous_input,
        } => {
            assert_eq!(0, output.len());
            assert!(has_extraneous_input);
        }
        ParseResult::Failed => panic!(),
        ParseResult::ErrorInParser => panic!(),
    }
}

#[test_log::test]
fn test_read_input_success() {
    let parser = Parser::Sequence(vec![
        Parser::ReadInputByte(RegisterId(0)),
        Parser::WriteOutputByte(RegisterId(0)),
    ]);
    let result = parse(&parser, &mut Slice::new("a"));
    match result {
        ParseResult::Success {
            output,
            has_extraneous_input,
        } => {
            assert_eq!(1, output.len());
            {
                let element = &output[0];
                let non_separator = element.as_ref().unwrap();
                assert_eq!(&[b'a'][..], &non_separator[..]);
            }
            assert!(!has_extraneous_input);
        }
        ParseResult::Failed => panic!(),
        ParseResult::ErrorInParser => panic!(),
    }
}

#[test_log::test]
fn test_read_input_failure() {
    let parser = Parser::Sequence(vec![
        Parser::ReadInputByte(RegisterId(0)),
        Parser::WriteOutputByte(RegisterId(0)),
    ]);
    let result = parse(&parser, &mut Slice::new(""));
    match result {
        ParseResult::Success {
            output: _,
            has_extraneous_input: _,
        } => panic!(),
        ParseResult::Failed => {}
        ParseResult::ErrorInParser => panic!(),
    }
}

#[test_log::test]
fn test_output_byte() {
    let parser = Parser::Sequence(vec![
        Parser::Constant(RegisterId(0), RegisterValue::Byte(123)),
        Parser::WriteOutputByte(RegisterId(0)),
    ]);
    let result = parse(&parser, &mut Slice::new(""));
    match result {
        ParseResult::Success {
            output,
            has_extraneous_input,
        } => {
            assert_eq!(1, output.len());
            {
                let element = &output[0];
                let non_separator = element.as_ref().unwrap();
                assert_eq!(&[123u8][..], &non_separator[..]);
            }
            assert!(!has_extraneous_input);
        }
        ParseResult::Failed => panic!(),
        ParseResult::ErrorInParser => panic!(),
    }
}

#[test_log::test]
fn test_output_bytes() {
    let parser = Parser::Sequence(vec![
        Parser::Constant(RegisterId(0), RegisterValue::Byte(123)),
        Parser::WriteOutputByte(RegisterId(0)),
        Parser::Constant(RegisterId(1), RegisterValue::Byte(76)),
        Parser::WriteOutputByte(RegisterId(1)),
    ]);
    let result = parse(&parser, &mut Slice::new(""));
    match result {
        ParseResult::Success {
            output,
            has_extraneous_input,
        } => {
            assert_eq!(1, output.len());
            {
                let element = &output[0];
                let non_separator = element.as_ref().unwrap();
                assert_eq!(&[123u8, 76u8][..], &non_separator[..]);
            }
            assert!(!has_extraneous_input);
        }
        ParseResult::Failed => panic!(),
        ParseResult::ErrorInParser => panic!(),
    }
}

#[test_log::test]
fn test_output_separator() {
    let parser = Parser::Sequence(vec![Parser::WriteOutputSeparator]);
    let result = parse(&parser, &mut Slice::new(""));
    match result {
        ParseResult::Success {
            output,
            has_extraneous_input,
        } => {
            assert_eq!(1, output.len());
            {
                let element = &output[0];
                assert!(element.is_none());
            }
            assert!(!has_extraneous_input);
        }
        ParseResult::Failed => panic!(),
        ParseResult::ErrorInParser => panic!(),
    }
}

#[test_log::test]
fn test_mixed_output() {
    let parser = Parser::Sequence(vec![
        Parser::Constant(RegisterId(0), RegisterValue::Byte(123)),
        Parser::WriteOutputByte(RegisterId(0)),
        Parser::WriteOutputSeparator,
        Parser::Constant(RegisterId(1), RegisterValue::Byte(76)),
        Parser::WriteOutputByte(RegisterId(1)),
        Parser::WriteOutputSeparator,
    ]);
    let result = parse(&parser, &mut Slice::new(""));
    match result {
        ParseResult::Success {
            output,
            has_extraneous_input,
        } => {
            assert_eq!(4, output.len());
            {
                let element = &output[0];
                let non_separator = element.as_ref().unwrap();
                assert_eq!(&[123u8][..], &non_separator[..]);
            }
            {
                let element = &output[1];
                assert!(element.is_none());
            }
            {
                let element = &output[2];
                let non_separator = element.as_ref().unwrap();
                assert_eq!(&[76u8][..], &non_separator[..]);
            }
            {
                let element = &output[3];
                assert!(element.is_none());
            }
            assert!(!has_extraneous_input);
        }
        ParseResult::Failed => panic!(),
        ParseResult::ErrorInParser => panic!(),
    }
}

#[test_log::test]
fn test_number_parsing() {
    let accumulator = RegisterId(0);
    let loop_condition = RegisterId(1);
    let constant_10 = RegisterId(2);
    let input_byte = RegisterId(3);
    let input_digit = RegisterId(4);
    let parser = Parser::Sequence(vec![
        Parser::Constant(accumulator, RegisterValue::Byte(0)),
        Parser::Constant(loop_condition, RegisterValue::Boolean(true)),
        Parser::Constant(constant_10, RegisterValue::Byte(10)),
        Parser::Loop {
            condition: loop_condition,
            body: Box::new(Parser::Sequence(vec![
                Parser::ReadInputByte(input_byte),
                Parser::RequireDigit {
                    input: input_byte,
                    output: input_digit,
                },
                Parser::Multiply {
                    destination: accumulator,
                    factor: constant_10,
                },
                Parser::Add {
                    destination: accumulator,
                    summand: input_digit,
                },
                Parser::IsEndOfInput(loop_condition),
                Parser::Not {
                    from: loop_condition,
                    to: loop_condition,
                },
            ])),
        },
        Parser::WriteOutputByte(accumulator),
    ]);
    expect_single_byte_output(&parser, "0", 0);
    expect_single_byte_output(&parser, "9", 9);
    expect_single_byte_output(&parser, "99", 99);
    expect_single_byte_output(&parser, "123", 123);
    expect_single_byte_output(&parser, "255", 255);
}

#[test_log::test]
fn test_or_none() {
    let parser = Parser::Or(vec![]);
    let result = parse(&parser, &mut Slice::new(""));
    match result {
        ParseResult::Success {
            output,
            has_extraneous_input,
        } => {
            assert_eq!(0, output.len());
            assert!(!has_extraneous_input);
        }
        ParseResult::Failed => panic!(),
        ParseResult::ErrorInParser => panic!(),
    }
}

#[test_log::test]
fn test_or_one() {
    let parser = Parser::Or(vec![Parser::Sequence(vec![
        Parser::ReadInputByte(RegisterId(0)),
        Parser::IsAnyOf {
            input: RegisterId(0),
            result: RegisterId(1),
            candidates: vec![RegisterValue::Byte(b'A')],
        },
        Parser::Not {
            from: RegisterId(1),
            to: RegisterId(1),
        },
        Parser::IfElse(
            RegisterId(1),
            Box::new(Parser::Fail),
            Box::new(Parser::no_op()),
        ),
        Parser::Constant(RegisterId(2), RegisterValue::Byte(0)),
        Parser::WriteOutputByte(RegisterId(2)),
    ])]);
    expect_single_byte_output(&parser, "A", 0);
}

#[test_log::test]
fn test_or_first() {
    let parser = Parser::Or(vec![
        Parser::Sequence(vec![
            Parser::ReadInputByte(RegisterId(0)),
            Parser::IsAnyOf {
                input: RegisterId(0),
                result: RegisterId(1),
                candidates: vec![RegisterValue::Byte(b'A')],
            },
            Parser::Not {
                from: RegisterId(1),
                to: RegisterId(1),
            },
            Parser::IfElse(
                RegisterId(1),
                Box::new(Parser::Fail),
                Box::new(Parser::no_op()),
            ),
            Parser::Constant(RegisterId(2), RegisterValue::Byte(0)),
            Parser::WriteOutputByte(RegisterId(2)),
        ]),
        Parser::Sequence(vec![
            Parser::ReadInputByte(RegisterId(0)),
            Parser::IsAnyOf {
                input: RegisterId(0),
                result: RegisterId(1),
                candidates: vec![RegisterValue::Byte(b'B')],
            },
            Parser::Not {
                from: RegisterId(1),
                to: RegisterId(1),
            },
            Parser::IfElse(
                RegisterId(1),
                Box::new(Parser::Fail),
                Box::new(Parser::no_op()),
            ),
            Parser::Constant(RegisterId(2), RegisterValue::Byte(1)),
            Parser::WriteOutputByte(RegisterId(2)),
        ]),
    ]);
    expect_single_byte_output(&parser, "A", 0);
}

#[test_log::test]
fn test_or_second() {
    let parser = Parser::Or(vec![
        Parser::Sequence(vec![
            Parser::ReadInputByte(RegisterId(0)),
            Parser::IsAnyOf {
                input: RegisterId(0),
                result: RegisterId(1),
                candidates: vec![RegisterValue::Byte(b'A')],
            },
            Parser::Not {
                from: RegisterId(1),
                to: RegisterId(1),
            },
            Parser::IfElse(
                RegisterId(1),
                Box::new(Parser::Fail),
                Box::new(Parser::no_op()),
            ),
            Parser::Constant(RegisterId(2), RegisterValue::Byte(0)),
            Parser::WriteOutputByte(RegisterId(2)),
        ]),
        Parser::Sequence(vec![
            Parser::ReadInputByte(RegisterId(0)),
            Parser::IsAnyOf {
                input: RegisterId(0),
                result: RegisterId(1),
                candidates: vec![RegisterValue::Byte(b'B')],
            },
            Parser::Not {
                from: RegisterId(1),
                to: RegisterId(1),
            },
            Parser::IfElse(
                RegisterId(1),
                Box::new(Parser::Fail),
                Box::new(Parser::no_op()),
            ),
            Parser::Constant(RegisterId(2), RegisterValue::Byte(1)),
            Parser::WriteOutputByte(RegisterId(2)),
        ]),
    ]);
    expect_single_byte_output(
        &parser, /*this is obviously wrong. TODO: support arbitrary lookahead*/ "BB", 1,
    );
}

#[cfg(test)]
fn expect_single_byte_output(parser: &Parser, input: &str, expected_output: u8) {
    let result = parse(&parser, &mut Slice::new(input));
    match result {
        ParseResult::Success {
            output,
            has_extraneous_input,
        } => {
            assert_eq!(1, output.len());
            {
                let element = &output[0];
                let non_separator = element.as_ref().unwrap();
                assert_eq!(&[expected_output][..], &non_separator[..]);
            }
            assert!(!has_extraneous_input);
        }
        ParseResult::Failed => panic!(),
        ParseResult::ErrorInParser => panic!(),
    }
}

#[test_log::test]
fn test_if_else() {
    let parser = Parser::Sequence(vec![
        Parser::ReadInputByte(RegisterId(0)),
        Parser::IsAnyOf {
            input: RegisterId(0),
            result: RegisterId(1),
            candidates: vec![RegisterValue::Byte(b'A')],
        },
        Parser::IfElse(
            RegisterId(1),
            Box::new(Parser::Constant(RegisterId(2), RegisterValue::Byte(42))),
            Box::new(Parser::Constant(RegisterId(2), RegisterValue::Byte(43))),
        ),
        Parser::WriteOutputByte(RegisterId(2)),
    ]);
    expect_single_byte_output(&parser, "A", 42);
    expect_single_byte_output(&parser, "B", 43);
}

#[test_log::test]
fn test_if_else_overwriting_condition() {
    let parser = Parser::Sequence(vec![
        Parser::ReadInputByte(RegisterId(0)),
        Parser::IsAnyOf {
            input: RegisterId(0),
            result: RegisterId(1),
            candidates: vec![RegisterValue::Byte(b'A')],
        },
        Parser::IfElse(
            RegisterId(1),
            Box::new(Parser::Sequence(vec![
                Parser::Constant(RegisterId(2), RegisterValue::Byte(42)),
                // The condition is only checked once, so this change won't cause the alternative to be executed.
                Parser::Not {
                    from: RegisterId(1),
                    to: RegisterId(1),
                },
            ])),
            Box::new(Parser::Sequence(vec![
                Parser::Constant(RegisterId(2), RegisterValue::Byte(43)),
                // The condition is only checked once, so this change won't cause the consequent to be executed.
                Parser::Not {
                    from: RegisterId(1),
                    to: RegisterId(1),
                },
            ])),
        ),
        Parser::WriteOutputByte(RegisterId(2)),
    ]);
    expect_single_byte_output(&parser, "A", 42);
    expect_single_byte_output(&parser, "B", 43);
}
