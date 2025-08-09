// libfuzzer doesn't support Windows. no_main causes a linker error on Windows.
#![cfg_attr(target_os = "linux", no_main)]

#[cfg(not(target_os = "linux"))]
fn main() {
    panic!("Fuzzing is not supported on this platform.");
}

use arbitrary::Unstructured;
use lambda_compiler::{parsing::parse_expression_tolerantly, tokenization::Token};
use libfuzzer_sys::{fuzz_target, Corpus};

fn fuzz_function(data: &[u8]) -> libfuzzer_sys::Corpus {
    let mut unstructured = Unstructured::new(data);
    let mut tokens: Vec<Token> = match unstructured.arbitrary() {
        Ok(success) => success,
        Err(_) => return Corpus::Reject,
    };
    // The parser always expects an EOF token at the end. The tokenizer guarantees this.
    if tokens.is_empty()
        || tokens.last().unwrap().content != lambda_compiler::tokenization::TokenContent::EndOfFile
    {
        tokens.push(Token::new(
            lambda_compiler::tokenization::TokenContent::EndOfFile,
            match unstructured.arbitrary() {
                Ok(location) => location,
                Err(_) => return Corpus::Reject,
            },
        ));
    }
    let output = parse_expression_tolerantly(
        &mut tokens.iter().peekable(),
        &lambda::name::NamespaceId([0; 16]),
    );
    assert!(output.entry_point.is_some() || !output.errors.is_empty());
    Corpus::Keep
}

fuzz_target!(|data: &[u8]| -> libfuzzer_sys::Corpus {
    // Write the actual code in a proper function because VS Code freaks out when there is an error in this macro invocation.
    fuzz_function(data)
});
