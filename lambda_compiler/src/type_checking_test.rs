use crate::{
    ast,
    compilation::CompilerOutput,
    type_checking::{check_types, combine_parameter_names},
};
use astraea::storage::InMemoryTreeStorage;
use lambda::{
    expressions::DeepExpression,
    name::{Name, NamespaceId},
};
use std::sync::Arc;

const TEST_SOURCE_NAMESPACE: NamespaceId =
    NamespaceId([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]);

const TEST_GENERATED_NAME_NAMESPACE: NamespaceId = NamespaceId([
    17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32,
]);

#[test_log::test]
fn test_combine_parameter_names() {
    assert_eq!(
        Name::new(TEST_GENERATED_NAME_NAMESPACE, "".to_string()),
        combine_parameter_names(&[], &TEST_GENERATED_NAME_NAMESPACE)
    );
    assert_eq!(
        Name::new(TEST_GENERATED_NAME_NAMESPACE, "a".to_string()),
        combine_parameter_names(
            &[Name::new(TEST_SOURCE_NAMESPACE, "a".to_string())],
            &TEST_GENERATED_NAME_NAMESPACE
        )
    );
    assert_eq!(
        Name::new(TEST_GENERATED_NAME_NAMESPACE, "a_b".to_string()),
        combine_parameter_names(
            &[
                Name::new(TEST_SOURCE_NAMESPACE, "a".to_string()),
                Name::new(TEST_SOURCE_NAMESPACE, "b".to_string())
            ],
            &TEST_GENERATED_NAME_NAMESPACE
        )
    );
}

#[test_log::test(tokio::test)]
async fn test_check_types_lambda_0_parameters() {
    let x_in_source = Name::new(TEST_SOURCE_NAMESPACE, "x".to_string());
    let input = ast::Expression::Lambda {
        parameter_names: vec![],
        body: Box::new(ast::Expression::Identifier(x_in_source.clone())),
    };
    let storage = Arc::new(InMemoryTreeStorage::empty());
    let output = check_types(&input, &TEST_GENERATED_NAME_NAMESPACE, &*storage).await;
    let parameter_name_in_output = Name::new(TEST_GENERATED_NAME_NAMESPACE, "".to_string());
    let expected = CompilerOutput::new(
        Some(DeepExpression(lambda::expressions::Expression::Lambda {
            parameter_name: parameter_name_in_output,
            body: Arc::new(DeepExpression(
                lambda::expressions::Expression::ReadVariable(x_in_source),
            )),
        })),
        Vec::new(),
    );
    assert_eq!(output, Ok(expected));
}

#[test_log::test(tokio::test)]
async fn test_check_types_lambda_1_parameter() {
    let x_in_source = Name::new(TEST_SOURCE_NAMESPACE, "x".to_string());
    let input = ast::Expression::Lambda {
        parameter_names: vec![x_in_source.clone()],
        body: Box::new(ast::Expression::Identifier(x_in_source.clone())),
    };
    let storage = Arc::new(InMemoryTreeStorage::empty());
    let output = check_types(&input, &TEST_GENERATED_NAME_NAMESPACE, &*storage).await;
    let parameter_name_in_output = Name::new(TEST_GENERATED_NAME_NAMESPACE, "x".to_string());
    let expected = CompilerOutput::new(
        Some(DeepExpression(lambda::expressions::Expression::Lambda {
            parameter_name: parameter_name_in_output,
            body: Arc::new(DeepExpression(
                lambda::expressions::Expression::ReadVariable(x_in_source),
            )),
        })),
        Vec::new(),
    );
    assert_eq!(output, Ok(expected));
}

#[test_log::test(tokio::test)]
async fn test_check_types_lambda_2_parameters() {
    let x_in_source = Name::new(TEST_SOURCE_NAMESPACE, "x".to_string());
    let y_in_source = Name::new(TEST_SOURCE_NAMESPACE, "y".to_string());
    let input = ast::Expression::Lambda {
        parameter_names: vec![x_in_source.clone(), y_in_source.clone()],
        body: Box::new(ast::Expression::Identifier(x_in_source.clone())),
    };
    let storage = Arc::new(InMemoryTreeStorage::empty());
    let output = check_types(&input, &TEST_GENERATED_NAME_NAMESPACE, &*storage).await;
    let parameter_name_in_output = Name::new(TEST_GENERATED_NAME_NAMESPACE, "x_y".to_string());
    let expected = CompilerOutput::new(
        Some(DeepExpression(lambda::expressions::Expression::Lambda {
            parameter_name: parameter_name_in_output,
            body: Arc::new(DeepExpression(
                lambda::expressions::Expression::ReadVariable(x_in_source),
            )),
        })),
        Vec::new(),
    );
    assert_eq!(output, Ok(expected));
}
