use crate::serialization::{FileNameContent, FileNameError};
use pretty_assertions::assert_eq;

#[test_log::test]
fn test_file_name_content_from() {
    assert_eq!(
        Err(FileNameError::Empty),
        FileNameContent::from("".to_string())
    );
    assert_eq!(
        String::from_iter(std::iter::repeat_n(
            'a',
            FileNameContent::MAX_LENGTH_IN_BYTES
        ))
        .as_str(),
        FileNameContent::from(String::from_iter(std::iter::repeat_n(
            'a',
            FileNameContent::MAX_LENGTH_IN_BYTES
        )))
        .unwrap()
        .as_str()
    );
    assert_eq!(
        Err(FileNameError::TooLong),
        FileNameContent::from(String::from_iter(std::iter::repeat_n(
            'a',
            FileNameContent::MAX_LENGTH_IN_BYTES + 1
        )))
    );
    assert_eq!(
        Err(FileNameError::Null),
        FileNameContent::from("\0".to_string())
    );
    assert_eq!(
        Err(FileNameError::AsciiControlCharacter),
        FileNameContent::from("\x01".to_string())
    );
    assert_eq!(
        Err(FileNameError::AsciiControlCharacter),
        FileNameContent::from("\x1e".to_string())
    );
    assert_eq!(
        Err(FileNameError::AsciiControlCharacter),
        FileNameContent::from("\x1f".to_string())
    );
    assert_eq!(
        " ",
        FileNameContent::from("\x20".to_string()).unwrap().as_str()
    );
    assert_eq!(
        Err(FileNameError::WindowsSpecialCharacter),
        FileNameContent::from("<".to_string())
    );
    assert_eq!(
        Err(FileNameError::WindowsSpecialCharacter),
        FileNameContent::from("*".to_string())
    );
    assert_eq!(
        " ",
        FileNameContent::from(" ".to_string()).unwrap().as_str()
    );
    assert_eq!(
        "a",
        FileNameContent::from("a".to_string()).unwrap().as_str()
    );
    assert_eq!(
        "aaaaaaaaaaaaaaaaaaaaaaa",
        FileNameContent::from("aaaaaaaaaaaaaaaaaaaaaaa".to_string())
            .unwrap()
            .as_str()
    );
}
