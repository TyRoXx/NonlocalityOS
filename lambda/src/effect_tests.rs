use crate::{
    builtins::{BUILTINS_NAMESPACE, LAMBDA_APPLY_METHOD_NAME},
    expressions::{evaluate, Expression, LambdaExpression, Pointer, ReadVariable},
    types::{Interface, Name, NamespaceId, Signature, Type, TypedExpression},
};
use astraea::{
    storage::{store_object, InMemoryValueStorage, StoreValue},
    tree::{HashedValue, Value},
};
use std::{collections::BTreeMap, pin::Pin, sync::Arc};

#[tokio::test]
async fn effect() {
    let storage = Arc::new(InMemoryValueStorage::empty());
    let namespace = NamespaceId([42; 16]);
    let console_output_name = Name::new(namespace, "ConsoleOutput".to_string());
    let console_output_type = Type::Named(console_output_name);

    let first_string = Arc::new(Value::from_string("Hello, ").unwrap());
    let first_string_ref = storage
        .store_value(&HashedValue::from(first_string))
        .await
        .unwrap();
    let first_console_output = crate::standard_library::ConsoleOutput {
        message: first_string_ref,
    };
    let first_console_output_value = Arc::new(first_console_output.to_value());
    let first_console_output_expression = TypedExpression::new(
        Expression::Literal(
            console_output_type.clone(),
            HashedValue::from(first_console_output_value.clone()),
        ),
        console_output_type.clone(),
    );

    let second_string = Arc::new(Value::from_string(" world!\n").unwrap());
    let second_string_ref = storage
        .store_value(&HashedValue::from(second_string))
        .await
        .unwrap();
    let second_console_output = crate::standard_library::ConsoleOutput {
        message: second_string_ref,
    };
    let second_console_output_value = Arc::new(second_console_output.to_value());
    let second_console_output_expression = TypedExpression::new(
        Expression::Literal(
            console_output_type.clone(),
            HashedValue::from(second_console_output_value.clone()),
        ),
        console_output_type.clone(),
    );

    let and_then_lambda_parameter_name = Name::new(namespace, "previous_result".to_string());
    let and_then_lambda_expression = TypedExpression::new(
        Expression::Lambda(Box::new(LambdaExpression::new(
            console_output_type.clone(),
            and_then_lambda_parameter_name.clone(),
            second_console_output_expression.expression,
        ))),
        Type::Function(Box::new(Signature::new(
            Type::Unit,
            console_output_type.clone(),
        ))),
    );

    let and_then_name = Name::new(namespace, "AndThen".to_string());
    let and_then_type = Type::Named(and_then_name);
    let construct_and_then_expression = TypedExpression::new(
        Expression::ConstructEffect(
            and_then_type.clone(),
            vec![
                first_console_output_expression.expression,
                and_then_lambda_expression.expression,
            ],
        ),
        and_then_type.clone(),
    );

    let main_lambda_parameter_name = Name::new(namespace, "unused_arg".to_string());
    let main_lambda_expression = TypedExpression::new(
        Expression::Lambda(Box::new(LambdaExpression::new(
            console_output_type.clone(),
            main_lambda_parameter_name.clone(),
            construct_and_then_expression.expression,
        ))),
        Type::Function(Box::new(Signature::new(Type::Unit, and_then_type.clone()))),
    );
    {
        let mut program_as_string = String::new();
        main_lambda_expression
            .expression
            .print(&mut program_as_string, 0)
            .unwrap();
        assert_eq!("(unused_arg) =>
  construct(AndThen, literal(ConsoleOutput, eabe5159d5b6c20554d74248e4f7c32021cbec092e1ce1221e90d2454e95c6e57b3524a5089a6dcbf7084f3389d61cbaf32e98559fe0684c2eb4883dcac1a322), (previous_result) =>
    literal(ConsoleOutput, 2bdfb1e268c1fa3859cc589789da27b302a76cbeb278018dffe2706cc497a9f8a3069085871b6d40fd35b0c463ad29a2dc68f94daa77a003ef462b8c71c20d4f))",
            program_as_string.as_str());
    }
    let read_variable: Arc<ReadVariable> = Arc::new(
        move |_name: &Name| -> Pin<Box<dyn core::future::Future<Output = Pointer> + Send>> {
            todo!()
        },
    );
    let read_literal = {
        let console_output_type = console_output_type.clone();
        move |literal_type: Type,
              value: HashedValue|
              -> Pin<Box<dyn core::future::Future<Output = Pointer> + Send>> {
            assert_eq!(console_output_type, literal_type);
            Box::pin(async move { Pointer::Value(value) })
        }
    };
    let main_function = evaluate(
        &main_lambda_expression.expression,
        &*storage,
        &*storage,
        &read_variable,
        &read_literal,
    )
    .await
    .unwrap();
    let apply_name = Name::new(BUILTINS_NAMESPACE, LAMBDA_APPLY_METHOD_NAME.to_string());
    let lambda_interface = Arc::new(Interface::new(BTreeMap::from([(
        apply_name.clone(),
        Signature::new(Type::Unit, console_output_type.clone()),
    )])));
    let lambda_interface_ref = store_object(&*storage, &*lambda_interface).await.unwrap();
    let result = main_function
        .call_method(
            &lambda_interface_ref,
            &apply_name,
            &Pointer::Value(HashedValue::from(Arc::new(Value::empty()))),
            &*storage,
            &*storage,
            &read_variable,
            &read_literal,
        )
        .await
        .unwrap();
    match &result {
        Pointer::InMemoryValue(value) => value,
        _ => panic!("Expected an InMemoryValue"),
    };
}
