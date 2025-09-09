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
