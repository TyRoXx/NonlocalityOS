// libfuzzer doesn't support Windows. no_main causes a linker error on Windows.
#![cfg_attr(target_os = "linux", no_main)]

#[cfg(not(target_os = "linux"))]
fn main() {
    panic!("Fuzzing is not supported on this platform.");
}

use lambda_compiler::tokenization::tokenize_default_syntax;
use libfuzzer_sys::{fuzz_target, Corpus};

fuzz_target!(|data: &[u8]| -> libfuzzer_sys::Corpus {
    let source = match std::str::from_utf8(data) {
        Ok(success) => success,
        Err(_) => return Corpus::Reject,
    };
    // TODO: check if the result roundtrips?
    let tokens = tokenize_default_syntax(source);
    assert_ne!(0, tokens.len());
    assert_eq!(
        tokens.last().unwrap().content,
        lambda_compiler::tokenization::TokenContent::EndOfFile
    );
    Corpus::Keep
});
