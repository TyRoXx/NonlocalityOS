// libfuzzer doesn't support Windows. no_main causes a linker error on Windows.
#![cfg_attr(target_os = "linux", no_main)]

#[cfg(not(target_os = "linux"))]
fn main() {
    panic!("Fuzzing is not supported on this platform.");
}

use fuzz_functions::lambda_compiler_tokenize;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| -> libfuzzer_sys::Corpus {
    if lambda_compiler_tokenize::fuzz_function(data) {
        libfuzzer_sys::Corpus::Keep
    } else {
        libfuzzer_sys::Corpus::Reject
    }
});
