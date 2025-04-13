use crate::{
    builtins::{BUILTINS_NAMESPACE, LAMBDA_APPLY_METHOD_NAME},
    expressions::{evaluate, Expression, LambdaExpression, Pointer, ReadVariable},
    types::{Interface, Name, NamespaceId, Signature, Type, TypedExpression},
};
use astraea::{
    storage::{store_object, InMemoryValueStorage, StoreValue},
    tree::{BlobDigest, HashedValue, Value, ValueBlob},
};
use std::{collections::BTreeMap, pin::Pin, sync::Arc};

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
struct ConsoleOutput {
    pub message: BlobDigest,
}

impl ConsoleOutput {
    pub fn to_value(&self) -> Value {
        Value::new(ValueBlob::empty(), vec![self.message])
    }

    pub fn from_value(value: &Value) -> Option<ConsoleOutput> {
        if value.blob().len() != 0 {
            return None;
        }
        if value.references().len() != 1 {
            return None;
        }
        Some(ConsoleOutput {
            message: value.references()[0],
        })
    }
}

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
    let console_output = ConsoleOutput {
        message: hello_world_string_ref,
    };
    let console_output_value = Arc::new(console_output.to_value());
    let console_output_expression = TypedExpression::new(
        Expression::Literal(
            console_output_type.clone(),
            HashedValue::from(console_output_value.clone()),
        ),
        console_output_type.clone(),
    );
    let lambda_parameter_name = Name::new(namespace, "unused_arg".to_string());
    let lambda_expression = TypedExpression::new(
        Expression::Lambda(Box::new(LambdaExpression::new(
            console_output_type.clone(),
            lambda_parameter_name.clone(),
            console_output_expression.expression,
        ))),
        Type::Function(Box::new(Signature::new(
            console_output_type.clone(),
            console_output_type.clone(),
        ))),
    );
    {
        let mut program_as_string = String::new();
        lambda_expression
            .expression
            .print(&mut program_as_string, 0)
            .unwrap();
        assert_eq!("(unused_arg) =>\n  literal(ConsoleOutput, 09e593654f7d4be82ed8ef897a98f0c23c45d5b49ec58a5c8e9df679bf204e0bd2d7b184002cf1348726dfc5ae6d25a5ce57b36177839f474388486aa27f5ece)", program_as_string.as_str());
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
        &lambda_expression.expression,
        &*storage,
        &read_variable,
        &read_literal,
    )
    .await;
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
            &read_variable,
            &read_literal,
        )
        .await
        .unwrap();
    let serialized_result = match result {
        Pointer::Value(value) => value,
        _ => panic!("Expected a Value"),
    };
    let deserialized_result = ConsoleOutput::from_value(serialized_result.value()).unwrap();
    assert_eq!(&console_output, &deserialized_result);
}
