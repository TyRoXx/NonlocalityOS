use crate::compilation::{compile, CompilerOutput};
use astraea::tree::{HashedTree, Tree};
use lambda::{
    expressions::{DeepExpression, Expression},
    name::{Name, NamespaceId},
};
use std::sync::Arc;

const TEST_NAMESPACE: NamespaceId =
    NamespaceId([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]);

#[test_log::test(tokio::test)]
async fn test_hello_world() {
    let source = include_str!("../examples/hello_world.tl");
    let output = compile(source, &TEST_NAMESPACE).await;
    let print_name = Name::new(TEST_NAMESPACE, "print".to_string());
    let print = Arc::new(DeepExpression(Expression::ReadVariable(print_name.clone())));
    let entry_point = DeepExpression(Expression::make_lambda(
        print_name,
        Arc::new(DeepExpression(Expression::make_apply(
            print.clone(),
            Arc::new(DeepExpression(Expression::make_literal(
                HashedTree::from(Arc::new(Tree::from_string("Hello, world!").unwrap()))
                    .digest()
                    .clone(),
            ))),
        ))),
    ));
    let expected = CompilerOutput::new(Some(entry_point), Vec::new());
    assert_eq!(expected, output);
}
