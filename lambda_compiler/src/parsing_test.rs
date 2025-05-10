use crate::ast;
use crate::compilation::{CompilerError, SourceLocation};
use crate::parsing::{parse_expression_tolerantly, ParserOutput};
use crate::{parsing::parse_expression, tokenization::tokenize_default_syntax};
use lambda::name::{Name, NamespaceId};

const TEST_NAMESPACE: NamespaceId =
    NamespaceId([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]);

fn parse_wellformed_expression(source: &str) -> ast::Expression {
    let tokens = tokenize_default_syntax(source);
    let mut token_iterator = tokens.iter().peekable();
    let output = parse_expression(&mut token_iterator, &TEST_NAMESPACE).unwrap();
    assert_eq!(None, token_iterator.next());
    output
}

fn test_wellformed_parsing(source: &str, expected: ast::Expression) {
    let output = parse_wellformed_expression(source);
    assert_eq!(expected, output);
}

#[test_log::test]
fn test_parse_lambda_0_parameters() {
    let name = Name::new(TEST_NAMESPACE, "f".to_string());
    let expected = ast::Expression::Lambda {
        parameter_names: vec![],
        body: Box::new(ast::Expression::Identifier(name)),
    };
    test_wellformed_parsing(r#"() => f"#, expected);
}

#[test_log::test]
fn test_parse_lambda_1_parameter() {
    let name = Name::new(TEST_NAMESPACE, "f".to_string());
    let expected = ast::Expression::Lambda {
        parameter_names: vec![name.clone()],
        body: Box::new(ast::Expression::Identifier(name)),
    };
    test_wellformed_parsing(r#"(f) => f"#, expected);
}

#[test_log::test]
fn test_parse_lambda_2_parameters() {
    let f = Name::new(TEST_NAMESPACE, "f".to_string());
    let g = Name::new(TEST_NAMESPACE, "g".to_string());
    let expected = ast::Expression::Lambda {
        parameter_names: vec![f.clone(), g],
        body: Box::new(ast::Expression::Identifier(f)),
    };
    test_wellformed_parsing(r#"(f g) => f"#, expected);
}

#[test_log::test]
fn test_parse_nested_lambda() {
    let f = Name::new(TEST_NAMESPACE, "f".to_string());
    let g = Name::new(TEST_NAMESPACE, "g".to_string());
    let expected = ast::Expression::Lambda {
        parameter_names: vec![f.clone()],
        body: Box::new(ast::Expression::Lambda {
            parameter_names: vec![g],
            body: Box::new(ast::Expression::Identifier(f)),
        }),
    };
    test_wellformed_parsing(r#"(f) => (g) => f"#, expected);
}

#[test_log::test]
fn test_parse_function_call() {
    let name = Name::new(TEST_NAMESPACE, "f".to_string());
    let f = Box::new(ast::Expression::Identifier(name.clone()));
    let expected = ast::Expression::Lambda {
        parameter_names: vec![name],
        body: Box::new(ast::Expression::Apply {
            callee: f.clone(),
            argument: f,
        }),
    };
    test_wellformed_parsing(r#"(f) => f(f)"#, expected);
}

#[test_log::test]
fn test_parse_missing_argument() {
    let tokens = tokenize_default_syntax(r#"(f) => f()"#);
    let mut token_iterator = tokens.iter().peekable();
    let output = parse_expression_tolerantly(&mut token_iterator, &TEST_NAMESPACE);
    assert_eq!(None, token_iterator.next());
    let expected = ParserOutput::new(
        None,
        vec![CompilerError::new(
            "Parser error: Expected expression, found right parenthesis.".to_string(),
            SourceLocation::new(0, 0),
        )],
    );
    assert_eq!(expected, output);
}

#[test_log::test]
fn test_parse_tree_construction_0_children() {
    for source in &["[]", " []", "[ ]", " [] ", "[  ]", "[ ] "] {
        let expected = ast::Expression::ConstructTree(vec![]);
        test_wellformed_parsing(source, expected);
    }
}

#[test_log::test]
fn test_parse_tree_construction_1_child() {
    for source in &[
        "[a]", "[ a ]", "[ a, ]", "[a,]", "[a, ]", "[ a,]", "[ a ,]", " [ a ,] ",
    ] {
        let name = Name::new(TEST_NAMESPACE, "a".to_string());
        let a = ast::Expression::Identifier(name.clone());
        let expected = ast::Expression::ConstructTree(vec![a]);
        test_wellformed_parsing(source, expected);
    }
}

#[test_log::test]
fn test_parse_tree_construction_2_children() {
    for source in &[
        "[a, b]",
        "[ a, b ]",
        "[ a, b, ]",
        "[a, b,]",
        "[a, b, ]",
        "[ a , b]",
        "[ a , b ]",
        "[ a , b, ]",
        " [ a , b , ] ",
    ] {
        let a = ast::Expression::Identifier(Name::new(TEST_NAMESPACE, "a".to_string()));
        let b = ast::Expression::Identifier(Name::new(TEST_NAMESPACE, "b".to_string()));
        let expected = ast::Expression::ConstructTree(vec![a, b]);
        test_wellformed_parsing(source, expected);
    }
}
