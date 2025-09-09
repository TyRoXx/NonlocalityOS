use lambda_compiler::tokenization::tokenize_default_syntax;

pub fn fuzz_function(data: &[u8]) -> bool {
    let source = match std::str::from_utf8(data) {
        Ok(success) => success,
        Err(_) => return false,
    };
    // TODO: check if the result roundtrips?
    let tokens = tokenize_default_syntax(source);
    match tokens {
        None => {}
        Some(tokens) => {
            assert_ne!(0, tokens.len());
            assert_eq!(
                tokens.last().unwrap().content,
                lambda_compiler::tokenization::TokenContent::EndOfFile
            );
        }
    }
    true
}

#[test]
fn crash_0() {
    assert!(fuzz_function(&[4, 34, 83]));
}
