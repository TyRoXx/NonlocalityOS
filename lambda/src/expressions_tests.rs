use crate::expressions::{
    to_reference_expression, DeepExpression, Expression, PrintExpression, ReferenceExpression,
    ShallowExpression,
};
use astraea::{
    deep_tree::DeepTree,
    tree::{BlobDigest, ReferenceIndex},
};
use std::sync::Arc;

#[test_log::test(tokio::test)]
async fn test_to_reference_expression_argument() {
    let expression = ShallowExpression::make_argument();
    let (reference_expression, references) = to_reference_expression(&expression);
    let expected_reference_expression = ReferenceExpression::make_argument();
    assert_eq!(reference_expression, expected_reference_expression);
    assert_eq!(references.len(), 0);
}

#[test_log::test(tokio::test)]
async fn print_all_expression_types() {
    let literal_1: DeepExpression = DeepExpression(Expression::make_literal(
        DeepTree::try_from_string("Hello, world!").unwrap(),
    ));
    let literal_2: DeepExpression = DeepExpression(Expression::make_literal(
        DeepTree::try_from_string("2").unwrap(),
    ));
    let argument = DeepExpression(Expression::make_argument());
    let environment = DeepExpression(Expression::make_environment());
    let construct = DeepExpression(Expression::make_construct_tree(vec![
        Arc::new(argument),
        Arc::new(environment),
    ]));
    let lambda = DeepExpression(Expression::make_lambda(
        Arc::new(literal_1),
        Arc::new(construct),
    ));
    let apply: DeepExpression = DeepExpression(Expression::make_apply(
        Arc::new(lambda),
        Arc::new(literal_2),
    ));
    let mut writer = String::new();
    apply.print(&mut writer, 0).unwrap();
    assert_eq!(
        concat!(
            "$env={literal(DeepTree { blob: TreeBlob { content.len(): 13 }, references: [] })}($arg) =>\n",
            "  [$arg, $env, ](literal(DeepTree { blob: TreeBlob { content.len(): 1 }, references: [] }))"),
        writer.as_str());
}

#[test_log::test(tokio::test)]
async fn test_print_expression_blob_digest() {
    let blob_digest = BlobDigest(([0; 32], [0; 32]));
    let mut writer = String::new();
    blob_digest.print(&mut writer, 0).unwrap();
    assert_eq!("00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000", writer.as_str());
}

#[test_log::test(tokio::test)]
async fn test_print_expression_reference_index() {
    let reference_index = ReferenceIndex(12);
    let mut writer = String::new();
    reference_index.print(&mut writer, 0).unwrap();
    assert_eq!("12", writer.as_str());
}

#[test_log::test(tokio::test)]
async fn print_shallow_expression() {
    let expression = ShallowExpression::make_literal(BlobDigest(([0; 32], [0; 32])));
    let mut writer = String::new();
    expression.print(&mut writer, 0).unwrap();
    assert_eq!(
        "literal(BlobDigest(\"00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000\"))",
        writer.as_str());
}
