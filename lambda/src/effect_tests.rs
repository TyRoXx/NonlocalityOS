use crate::{
    expressions::{
        deserialize_recursively, evaluate, serialize_recursively, DeepExpression, Expression,
        PrintExpression, ReadVariable,
    },
    name::{Name, NamespaceId},
};
use astraea::{
    storage::{InMemoryValueStorage, StoreValue},
    tree::{BlobDigest, HashedTree, Tree},
};
use std::{pin::Pin, sync::Arc};

#[test_log::test(tokio::test)]
async fn effect() {
    let storage = Arc::new(InMemoryValueStorage::empty());
    let namespace = NamespaceId([42; 16]);

    let first_string = Arc::new(Tree::from_string("Hello, ").unwrap());
    let first_string_ref = storage
        .store_value(&HashedTree::from(first_string))
        .await
        .unwrap();
    let first_console_output = crate::standard_library::ConsoleOutput {
        message: first_string_ref,
    };
    let first_console_output_value = Arc::new(first_console_output.to_value());
    let first_console_output_expression = DeepExpression(Expression::make_literal(
        storage
            .store_value(&HashedTree::from(first_console_output_value.clone()))
            .await
            .unwrap(),
    ));

    let second_string = Arc::new(Tree::from_string(" world!\n").unwrap());
    let second_string_ref = storage
        .store_value(&HashedTree::from(second_string))
        .await
        .unwrap();
    let main_lambda_parameter_name = Name::new(namespace, "main_arg".to_string());
    let second_console_output_expression =
        DeepExpression(Expression::Construct(vec![Arc::new(DeepExpression(
            Expression::make_read_variable(main_lambda_parameter_name.clone()),
        ))]));

    let and_then_lambda_parameter_name = Name::new(namespace, "previous_result".to_string());
    let and_then_lambda_expression = DeepExpression(Expression::make_lambda(
        and_then_lambda_parameter_name.clone(),
        Arc::new(second_console_output_expression),
    ));

    let construct_and_then_expression = DeepExpression(Expression::make_construct(vec![
        Arc::new(first_console_output_expression),
        Arc::new(and_then_lambda_expression),
    ]));

    let main_lambda_expression = DeepExpression(Expression::make_lambda(
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
            "(2a2a2a2a-2a2a-2a2a-2a2a-2a2a2a2a2a2a.main_arg) =>\n",
            "  construct(literal(3d68922f2a62988e48e9734f5107de0aef4f1d088bb67bfada36bcd8d9288a750d6217bd9a88f498c78b76040ef29bbb136bfaea876601d02405546160b2fd9d), (2a2a2a2a-2a2a-2a2a-2a2a-2a2a2a2a2a2a.previous_result) =>\n",
            "    construct(main_arg, ), )"),
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
            "e43bae530b6c212df1e2fc3284723a87f3f1449a76f6a0ee45b048391ffe182a",
            "ed2f88975a478a7db6001879ac12d4d837b988401b1be1cb4e14789600f134a9"
        ),
        format!("{}", &call_main_digest)
    );

    let main_result = evaluate(&call_main, &*storage, &*storage, &read_variable)
        .await
        .unwrap();
    assert_eq!(
        concat!(
            "37efb7833e4c3b04558ab90bfb56209ea92657f4791332d97e40556b57be4554",
            "04a5a202d58718994be05dbeece093c57a2a708bfaee625db1a3136bb591b457"
        ),
        format!("{}", &main_result)
    );
}
