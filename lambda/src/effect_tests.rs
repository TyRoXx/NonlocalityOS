use crate::{
    expressions::{evaluate, DeepExpression, Expression, PrintExpression, ReadVariable},
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
    let second_console_output = crate::standard_library::ConsoleOutput {
        message: second_string_ref,
    };
    let second_console_output_value = Arc::new(second_console_output.to_value());
    let second_console_output_expression = DeepExpression(Expression::make_literal(
        storage
            .store_value(&HashedValue::from(second_console_output_value.clone()))
            .await
            .unwrap(),
    ));

    let and_then_lambda_parameter_name = Name::new(namespace, "previous_result".to_string());
    let and_then_lambda_expression = DeepExpression(Expression::make_lambda(
        and_then_lambda_parameter_name.clone(),
        Arc::new(second_console_output_expression),
    ));

    let construct_and_then_expression = DeepExpression(Expression::make_construct(vec![
        Arc::new(first_console_output_expression),
        Arc::new(and_then_lambda_expression),
    ]));

    let main_lambda_parameter_name = Name::new(namespace, "unused_arg".to_string());
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
            "(2a2a2a2a-2a2a-2a2a-2a2a-2a2a2a2a2a2a.unused_arg) =>\n",
            "  construct(literal(eabe5159d5b6c20554d74248e4f7c32021cbec092e1ce1221e90d2454e95c6e57b3524a5089a6dcbf7084f3389d61cbaf32e98559fe0684c2eb4883dcac1a322), (2a2a2a2a-2a2a-2a2a-2a2a-2a2a2a2a2a2a.previous_result) =>\n",
            "    literal(2bdfb1e268c1fa3859cc589789da27b302a76cbeb278018dffe2706cc497a9f8a3069085871b6d40fd35b0c463ad29a2dc68f94daa77a003ef462b8c71c20d4f), )"),
            program_as_string.as_str());
    }
    let read_variable: Arc<ReadVariable> = Arc::new(
        move |_name: &Name| -> Pin<Box<dyn core::future::Future<Output = BlobDigest> + Send>> {
            todo!()
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
        Arc::new(DeepExpression(Expression::make_literal(
            storage
                .store_value(&HashedValue::from(Arc::new(Value::empty())))
                .await
                .unwrap(),
        ))),
    ));
    let main_result = evaluate(&call_main, &*storage, &*storage, &read_variable)
        .await
        .unwrap();
    assert_eq!("fea4987e02d4cb0222418c4656c36f94944bc5fec9bf892253ad54f00a7d80c7a35903b7593535d3baab40574eb0500fba02e4617a189d09c492638bb292a3bd", format!("{}", &main_result))
}
