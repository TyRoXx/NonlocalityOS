use crate::{
    expressions::{
        to_reference_expression, DeepExpression, Expression, PrintExpression, ReferenceExpression,
        ShallowExpression,
    },
    name::{Name, NamespaceId},
};
use astraea::tree::{BlobDigest, ReferenceIndex};
use std::sync::Arc;

#[test_log::test(tokio::test)]
async fn test_to_reference_expression_read_variable() {
    let name = Name::new(NamespaceId([0xff; 16]), "name".to_string());
    let read_variable = ShallowExpression::ReadVariable(name.clone());
    let (reference_expression, references) = to_reference_expression(&read_variable);
    let expected_reference_expression = ReferenceExpression::ReadVariable(name);
    assert_eq!(reference_expression, expected_reference_expression);
    assert_eq!(references.len(), 0);
}

#[test_log::test(tokio::test)]
async fn print_all_expression_types() {
    let literal: DeepExpression =
        DeepExpression(Expression::make_literal(BlobDigest(([0; 32], [0; 32]))));
    let name = Name::new(NamespaceId([0xff; 16]), "name".to_string());
    let read_variable = DeepExpression(Expression::ReadVariable(name.clone()));
    let construct = DeepExpression(Expression::make_construct(vec![Arc::new(read_variable)]));
    let lambda = DeepExpression(Expression::make_lambda(name, Arc::new(construct)));
    let apply: DeepExpression =
        DeepExpression(Expression::make_apply(Arc::new(lambda), Arc::new(literal)));
    let mut writer = String::new();
    apply.print(&mut writer, 0).unwrap();
    assert_eq!(
        concat!(
            "(ffffffff-ffff-ffff-ffff-ffffffffffff.name) =>\n",
            "  construct(name, )(literal(00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000))"),
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
        "literal(00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000)",
        writer.as_str());
}
