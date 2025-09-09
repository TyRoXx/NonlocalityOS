use arbitrary::Unstructured;
use lambda_compiler::{parsing::parse_expression_tolerantly, tokenization::Token};

pub fn fuzz_function(data: &[u8]) -> bool {
    let mut unstructured = Unstructured::new(data);
    let mut tokens: Vec<Token> = match unstructured.arbitrary() {
        Ok(success) => success,
        Err(_) => return false,
    };
    // The parser always expects an EOF token at the end. The tokenizer guarantees this.
    if tokens.is_empty()
        || tokens.last().unwrap().content != lambda_compiler::tokenization::TokenContent::EndOfFile
    {
        tokens.push(Token::new(
            lambda_compiler::tokenization::TokenContent::EndOfFile,
            match unstructured.arbitrary() {
                Ok(location) => location,
                Err(_) => return false,
            },
        ));
    }
    let output = parse_expression_tolerantly(
        &mut tokens.iter().peekable(),
        &lambda::name::NamespaceId([0; 16]),
    );
    assert!(output.entry_point.is_some() || !output.errors.is_empty());
    true
}

#[test]
fn crash_0() {
    assert!(fuzz_function(&[1, 0, 108, 0, 108, 0]));
}

#[test]
fn crash_1() {
    assert!(fuzz_function(&[63, 246, 14, 1, 58]));
}

#[test]
fn crash_2() {
    assert!(fuzz_function(&[41, 63, 64, 131, 41, 131]));
}

#[test]
fn crash_3() {
    assert!(fuzz_function(&[73, 78, 73, 73, 78]));
}

#[test]
fn crash_4() {
    assert!(fuzz_function(&[
        255, 63, 41, 131, 131, 131, 131, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 141, 255, 255, 255, 255, 255, 191, 255, 157
    ]));
}

#[test]
fn crash_5() {
    assert!(fuzz_function(&[
        63, 41, 131, 85, 85, 85, 42, 255, 255, 255, 255, 255, 213, 213, 213, 213, 213, 213, 213,
        213, 213, 213, 255, 255, 131, 131, 157
    ]));
}

#[test]
fn crash_6() {
    assert!(fuzz_function(&[
        93, 93, 186, 186, 186, 186, 93, 46, 93, 93, 186, 93, 46, 93, 93, 93, 93, 63, 255, 255, 93,
        93, 255, 131, 131, 255
    ]));
}

#[test]
fn crash_7() {
    assert!(fuzz_function(&[
        255, 255, 131, 57, 57, 57, 19, 0, 0, 0, 0, 0, 0, 0, 255, 255, 36, 36, 36, 0, 255, 131, 255,
        0, 255, 195
    ]));
}

#[test]
fn crash_8() {
    assert!(fuzz_function(&[
        43, 64, 63, 41, 78, 255, 127, 255, 47, 41, 20, 1, 0, 255, 42, 41, 255, 255, 255, 255, 255,
        255, 255, 64, 139, 227
    ]));
}

#[test]
fn crash_9() {
    assert!(fuzz_function(&[
        33, 45, 255, 64, 53, 64, 0, 0, 132, 0, 0, 41, 64, 236, 236, 236, 236, 33, 117, 0, 236, 103,
        103, 64, 53, 64, 33
    ]));
}

#[test]
fn crash_10() {
    assert!(fuzz_function(&[
        63, 41, 36, 131, 131, 131, 255, 255, 255, 255, 33, 255, 255, 255, 255, 255, 255, 255, 255,
        185, 191, 255, 255, 252, 41, 117, 131, 157
    ]));
}

#[test]
fn crash_11() {
    assert!(fuzz_function(&[
        63, 255, 255, 241, 47, 255, 87, 87, 87, 255, 255, 87, 87, 255, 87, 255, 255, 41, 33, 255,
        255, 255, 41, 33, 64, 236, 236, 236, 172
    ]));
}
