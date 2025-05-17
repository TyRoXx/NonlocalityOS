use crate::{
    expressions::{
        deserialize_recursively, evaluate, serialize_recursively, DeepExpression, Expression,
        PrintExpression, ReadVariable,
    },
    name::{Name, NamespaceId},
};
use astraea::{
    storage::{InMemoryTreeStorage, StoreTree},
    tree::{BlobDigest, HashedTree, Tree},
};
use std::{pin::Pin, sync::Arc};

#[test_log::test(tokio::test)]
async fn effect() {
    let storage = Arc::new(InMemoryTreeStorage::empty());
    let empty_tree = Arc::new(DeepExpression(Expression::make_literal(
        storage
            .store_tree(&HashedTree::from(Arc::new(Tree::empty())))
            .await
            .unwrap(),
    )));
    let namespace = NamespaceId([42; 16]);

    let first_string = Arc::new(Tree::from_string("Hello, ").unwrap());
    let first_string_ref = storage
        .store_tree(&HashedTree::from(first_string))
        .await
        .unwrap();
    let first_console_output = crate::standard_library::ConsoleOutput {
        message: first_string_ref,
    };
    let first_console_output_tree = Arc::new(first_console_output.to_tree());
    let first_console_output_expression = DeepExpression(Expression::make_literal(
        storage
            .store_tree(&HashedTree::from(first_console_output_tree.clone()))
            .await
            .unwrap(),
    ));

    let second_string = Arc::new(Tree::from_string(" world!\n").unwrap());
    let second_string_ref = storage
        .store_tree(&HashedTree::from(second_string))
        .await
        .unwrap();
    let main_lambda_parameter_name = Name::new(namespace, "main_arg".to_string());
    let second_console_output_expression =
        DeepExpression(Expression::ConstructTree(vec![Arc::new(DeepExpression(
            Expression::make_read_variable(main_lambda_parameter_name.clone()),
        ))]));

    let and_then_lambda_parameter_name = Name::new(namespace, "previous_result".to_string());
    let and_then_lambda_expression = DeepExpression(Expression::make_lambda(
        empty_tree.clone(),
        and_then_lambda_parameter_name.clone(),
        Arc::new(second_console_output_expression),
    ));

    let construct_and_then_expression = DeepExpression(Expression::make_construct_tree(vec![
        Arc::new(first_console_output_expression),
        Arc::new(and_then_lambda_expression),
    ]));

    let main_lambda_expression = DeepExpression(Expression::make_lambda(
        empty_tree.clone(),
        main_lambda_parameter_name.clone(),
        Arc::new(construct_and_then_expression),
    ));
    {
        let mut program_as_string = String::new();
        main_lambda_expression
            .0
            .print(&mut program_as_string, 0)
            .unwrap();
        assert_eq!(concat!(
            "$env={literal(f0140e314ee38d4472393680e7a72a81abb36b134b467d90ea943b7aa1ea03bf2323bc1a2df91f7230a225952e162f6629cf435e53404e9cdd727a2d94e4f909)}(2a2a2a2a-2a2a-2a2a-2a2a-2a2a2a2a2a2a.main_arg) =>\n",
            "  [literal(3d68922f2a62988e48e9734f5107de0aef4f1d088bb67bfada36bcd8d9288a750d6217bd9a88f498c78b76040ef29bbb136bfaea876601d02405546160b2fd9d), $env={literal(f0140e314ee38d4472393680e7a72a81abb36b134b467d90ea943b7aa1ea03bf2323bc1a2df91f7230a225952e162f6629cf435e53404e9cdd727a2d94e4f909)}(2a2a2a2a-2a2a-2a2a-2a2a-2a2a2a2a2a2a.previous_result) =>\n",
            "    [main_arg, ], ]"),
            program_as_string.as_str());
    }
    let read_variable: Arc<ReadVariable> = Arc::new(
        move |name: &Name| -> Pin<Box<dyn core::future::Future<Output = BlobDigest> + Send>> {
            assert_eq!(name, &main_lambda_parameter_name);
            Box::pin(async move { second_string_ref })
        },
    );
    let main_function = evaluate(
        &main_lambda_expression,
        &*storage,
        &*storage,
        &read_variable,
        &None,
    )
    .await
    .unwrap();
    let call_main = DeepExpression(Expression::make_apply(
        Arc::new(DeepExpression(Expression::make_literal(main_function))),
        Arc::new(DeepExpression(Expression::make_literal(second_string_ref))),
    ));

    // verify that this complex expression roundtrips through serialization and deserialization correctly
    let call_main_digest = serialize_recursively(&call_main, &*storage).await.unwrap();
    let deserialized_call_main = deserialize_recursively(&call_main_digest, &*storage)
        .await
        .unwrap();
    assert_eq!(call_main, deserialized_call_main);
    assert_eq!(
        concat!(
            "b824126569e1e7d12491ba15ceaad5251532f137fe767bfc43c77232883fee8c",
            "6af20b981f855fdb159210deca6f2095ef9997f7c94a45ec90b5826b61e5cd1c"
        ),
        format!("{}", &call_main_digest)
    );

    let main_result = evaluate(&call_main, &*storage, &*storage, &read_variable, &None)
        .await
        .unwrap();
    assert_eq!(
        concat!(
            "07303dad8ad5bf347e9234a938b0d2c7fefcd6e8e505aa48ada0fbcaa7e509ca",
            "810edee144769422aa1d29b8906a393d7f11735444f573037b349a6cac9a96e3"
        ),
        format!("{}", &main_result)
    );
}
