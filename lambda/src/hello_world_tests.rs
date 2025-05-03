use crate::{
    expressions::{evaluate, DeepExpression, Expression, PrintExpression, ReadVariable},
    name::{Name, NamespaceId},
};
use astraea::{
    storage::{InMemoryValueStorage, LoadTree, StoreTree},
    tree::{BlobDigest, HashedTree, Tree},
};
use std::{pin::Pin, sync::Arc};

#[test_log::test(tokio::test)]
async fn hello_world() {
    let storage = Arc::new(InMemoryValueStorage::empty());
    let namespace = NamespaceId([42; 16]);
    let hello_world_string = Arc::new(Tree::from_string("Hello, world!\n").unwrap());
    let hello_world_string_ref = storage
        .store_tree(&HashedTree::from(hello_world_string))
        .await
        .unwrap();
    let console_output = crate::standard_library::ConsoleOutput {
        message: hello_world_string_ref,
    };
    let console_output_value = Arc::new(console_output.to_value());
    let console_output_expression = DeepExpression(Expression::make_literal(
        storage
            .store_tree(&HashedTree::from(console_output_value.clone()))
            .await
            .unwrap(),
    ));
    let lambda_parameter_name = Name::new(namespace, "unused_arg".to_string());
    let lambda_expression = DeepExpression(Expression::make_lambda(
        lambda_parameter_name.clone(),
        Arc::new(console_output_expression),
    ));
    {
        let mut program_as_string = String::new();
        lambda_expression
            .0
            .print(&mut program_as_string, 0)
            .unwrap();
        assert_eq!(
            concat!(
            "(2a2a2a2a-2a2a-2a2a-2a2a-2a2a2a2a2a2a.unused_arg) =>\n",
            "  literal(9e86496e4fc3adcfd51c8c6682e52126e7aef897832893ceeeb0fae69a44705132bb8b008efcaa4e00ac1459bfefd01e80f098c5e6dd08aec60175d0d334d5a4)"),
            program_as_string.as_str()
        );
    }
    let read_variable: Arc<ReadVariable> = Arc::new(
        move |_name: &Name| -> Pin<Box<dyn core::future::Future<Output = BlobDigest> + Send>> {
            todo!()
        },
    );
    let main_function = evaluate(&lambda_expression, &*storage, &*storage, &read_variable)
        .await
        .unwrap();
    let call_main = DeepExpression(Expression::make_apply(
        Arc::new(DeepExpression(Expression::make_literal(main_function))),
        Arc::new(DeepExpression(Expression::make_literal(
            storage
                .store_tree(&HashedTree::from(Arc::new(Tree::empty())))
                .await
                .unwrap(),
        ))),
    ));
    let main_result = evaluate(&call_main, &*storage, &*storage, &read_variable)
        .await
        .unwrap();
    let serialized_result = storage
        .load_tree(&main_result)
        .await
        .unwrap()
        .hash()
        .unwrap();
    let deserialized_result =
        crate::standard_library::ConsoleOutput::from_value(serialized_result.tree()).unwrap();
    assert_eq!(&console_output, &deserialized_result);
}
