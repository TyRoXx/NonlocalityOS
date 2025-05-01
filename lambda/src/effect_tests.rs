use crate::{
    expressions::{
        deserialize_recursively, evaluate, serialize_recursively, DeepExpression, Expression,
        PrintExpression, ReadVariable,
    },
    name::{Name, NamespaceId},
};
use astraea::{
    storage::{InMemoryValueStorage, StoreValue},
    tree::{BlobDigest, HashedValue, Value},
};
use std::{pin::Pin, sync::Arc};

#[tokio::test]
async fn effect() {
    let storage = Arc::new(InMemoryValueStorage::empty());
    let namespace = NamespaceId([42; 16]);

    let first_string = Arc::new(Value::from_string("Hello, ").unwrap());
    let first_string_ref = storage
        .store_value(&HashedValue::from(first_string))
        .await
        .unwrap();
    let first_console_output = crate::standard_library::ConsoleOutput {
        message: first_string_ref,
    };
    let first_console_output_value = Arc::new(first_console_output.to_value());
    let first_console_output_expression = DeepExpression(Expression::make_literal(
        storage
            .store_value(&HashedValue::from(first_console_output_value.clone()))
            .await
            .unwrap(),
    ));

    let second_string = Arc::new(Value::from_string(" world!\n").unwrap());
    let second_string_ref = storage
        .store_value(&HashedValue::from(second_string))
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
            "  construct(literal(eabe5159d5b6c20554d74248e4f7c32021cbec092e1ce1221e90d2454e95c6e57b3524a5089a6dcbf7084f3389d61cbaf32e98559fe0684c2eb4883dcac1a322), (2a2a2a2a-2a2a-2a2a-2a2a-2a2a2a2a2a2a.previous_result) =>\n",
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
            "6ee32e09f73a6d4fee451f75b942e103e8ede078e7eb3358a64abece3de5664d",
            "2e2c39984e63636616fedd658ab3e0b1d3a1b0aa76613baca335071efeaef872"
        ),
        format!("{}", &call_main_digest)
    );

    let main_result = evaluate(&call_main, &*storage, &*storage, &read_variable)
        .await
        .unwrap();
    assert_eq!(
        concat!(
            "24fc8e4eed1a2eba5b0c1e5b9d260f12ead12f4d3fafd09fbc5cd90f8625da7f",
            "2d6234fb3bca47bb1dba4c3250245736863822ddbe6b3ed5858a2710a5ae0edc"
        ),
        format!("{}", &main_result)
    );
}
