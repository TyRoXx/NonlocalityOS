use crate::{
    expressions::{evaluate, DeepExpression, Expression, PrintExpression, ReadVariable},
    types::{Name, NamespaceId, Type, TypedExpression},
};
use astraea::{
    storage::{InMemoryValueStorage, LoadValue, StoreValue},
    tree::{BlobDigest, HashedValue, Value},
};
use std::{pin::Pin, sync::Arc};

#[tokio::test]
async fn hello_world() {
    let storage = Arc::new(InMemoryValueStorage::empty());
    let namespace = NamespaceId([42; 16]);
    let console_output_name = Name::new(namespace, "ConsoleOutput".to_string());
    let console_output_type = Type::Named(console_output_name);
    let hello_world_string = Arc::new(Value::from_string("Hello, world!\n").unwrap());
    let hello_world_string_ref = storage
        .store_value(&HashedValue::from(hello_world_string))
        .await
        .unwrap();
    let console_output = crate::standard_library::ConsoleOutput {
        message: hello_world_string_ref,
    };
    let console_output_value = Arc::new(console_output.to_value());
    let console_output_expression = TypedExpression::new(
        DeepExpression(Expression::make_literal(
            storage
                .store_value(&HashedValue::from(console_output_value.clone()))
                .await
                .unwrap(),
        )),
        console_output_type.clone(),
    );
    let lambda_parameter_name = Name::new(namespace, "unused_arg".to_string());
    let lambda_expression = DeepExpression(Expression::make_lambda(
        lambda_parameter_name.clone(),
        Arc::new(console_output_expression.expression),
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
            "  literal(09e593654f7d4be82ed8ef897a98f0c23c45d5b49ec58a5c8e9df679bf204e0bd2d7b184002cf1348726dfc5ae6d25a5ce57b36177839f474388486aa27f5ece)"),
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
        Arc::new(DeepExpression(Expression::make_unit())),
    ));
    let main_result = evaluate(&call_main, &*storage, &*storage, &read_variable)
        .await
        .unwrap();
    let serialized_result = storage
        .load_value(&main_result)
        .await
        .unwrap()
        .hash()
        .unwrap();
    let deserialized_result =
        crate::standard_library::ConsoleOutput::from_value(serialized_result.value()).unwrap();
    assert_eq!(&console_output, &deserialized_result);
}
