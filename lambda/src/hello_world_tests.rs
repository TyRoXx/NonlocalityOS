use crate::{
    builtins::{BUILTINS_NAMESPACE, LAMBDA_APPLY_METHOD_NAME},
    expressions::{
        evaluate, Expression, LambdaExpression, Object, Pointer, ReadLiteral, ReadVariable,
    },
    types::{Interface, Name, NamespaceId, Signature, Type, TypedExpression},
};
use astraea::{
    storage::{store_object, InMemoryValueStorage, LoadValue, StoreError, StoreValue},
    tree::{BlobDigest, HashedValue, Value, ValueBlob},
};
use async_trait::async_trait;
use std::{collections::BTreeMap, pin::Pin, sync::Arc};

#[derive(Debug)]
struct MemoryByteSink {
    pub self_interface: BlobDigest,
    pub written: Vec<Arc<Value>>,
}

impl MemoryByteSink {
    pub fn write_method_name() -> Name {
        let namespace = NamespaceId([42; 16]);
        Name::new(namespace, "write".to_string())
    }

    pub async fn deserialize(
        serialized: &Value,
        storage: &(dyn LoadValue + Sync),
    ) -> Option<MemoryByteSink> {
        if serialized.blob().len() > 0 {
            return None;
        }
        let mut references_iterator = serialized.references().iter();
        let self_interface = references_iterator.next()?;
        let mut written: Vec<Arc<Value>> = Vec::new();
        written.reserve(references_iterator.len());
        for reference in references_iterator {
            let value = storage.load_value(reference).await?;
            written.push(value.hash()?.value().clone());
        }
        Some(MemoryByteSink {
            self_interface: *self_interface,
            written,
        })
    }
}

#[async_trait]
impl Object for MemoryByteSink {
    async fn call_method(
        &self,
        interface: &BlobDigest,
        method: &Name,
        argument: &Pointer,
        _storage: &(dyn LoadValue + Sync),
        _read_variable: &Arc<ReadVariable>,
        _read_literal: &ReadLiteral,
    ) -> std::result::Result<Pointer, ()> {
        if &self.self_interface == interface {
            if &Self::write_method_name() == method {
                let argument_value = match argument.serialize_to_flat_value().await {
                    Some(success) => success,
                    None => todo!(),
                };
                let mut new_written = self.written.clone();
                new_written.push(argument_value);
                Ok(Pointer::Object(Arc::new(MemoryByteSink {
                    self_interface: self.self_interface,
                    written: new_written,
                })))
            } else {
                todo!()
            }
        } else {
            todo!()
        }
    }

    async fn serialize(
        &self,
        storage: &(dyn StoreValue + Sync),
    ) -> std::result::Result<HashedValue, StoreError> {
        let mut references = Vec::new();
        references.push(self.self_interface);
        for value in &self.written {
            references.push(
                storage
                    .store_value(&HashedValue::from(value.clone()))
                    .await?,
            );
        }
        Ok(HashedValue::from(Arc::new(Value::new(
            ValueBlob::empty(),
            references,
        ))))
    }

    async fn serialize_to_flat_value(&self) -> Option<Arc<Value>> {
        todo!()
    }
}

#[tokio::test]
async fn hello_world() {
    let storage = Arc::new(InMemoryValueStorage::empty());
    let namespace = NamespaceId([42; 16]);
    let memory_byte_sink_name = Name::new(namespace, "MemoryByteSink".to_string());
    let memory_byte_sink_type = Type::Named(memory_byte_sink_name);
    let lambda_parameter_name = Name::new(namespace.clone(), "output".to_string());
    let read_lambda_parameter_expression = TypedExpression::new(
        Expression::ReadVariable(lambda_parameter_name.clone()),
        memory_byte_sink_type.clone(),
    );
    let small_blob_name = Name::new(namespace, "SmallBlob".to_string());
    let small_blob_type = Type::Named(small_blob_name);
    let memory_byte_sink_interface = Arc::new(Interface::new(BTreeMap::from([(
        MemoryByteSink::write_method_name(),
        Signature::new(small_blob_type.clone(), memory_byte_sink_type.clone()),
    )])));
    let memory_byte_sink_interface_ref = store_object(&*storage, &*memory_byte_sink_interface)
        .await
        .unwrap();
    let hello_world_string = Arc::new(Value::from_string("Hello, world!\n").unwrap());
    let write_expression = read_lambda_parameter_expression
        .apply(
            &memory_byte_sink_interface,
            &memory_byte_sink_interface_ref,
            MemoryByteSink::write_method_name(),
            TypedExpression::new(
                Expression::Literal(
                    small_blob_type.clone(),
                    HashedValue::from(hello_world_string.clone()),
                ),
                small_blob_type.clone(),
            ),
        )
        .unwrap();
    let lambda_expression = TypedExpression::new(
        Expression::Lambda(Box::new(LambdaExpression::new(
            memory_byte_sink_type.clone(),
            lambda_parameter_name.clone(),
            write_expression.expression,
        ))),
        Type::Function(Box::new(Signature::new(
            memory_byte_sink_type.clone(),
            memory_byte_sink_type.clone(),
        ))),
    );
    let apply_name = Name::new(BUILTINS_NAMESPACE, LAMBDA_APPLY_METHOD_NAME.to_string());
    let lambda_interface = Arc::new(Interface::new(BTreeMap::from([(
        apply_name.clone(),
        Signature::new(memory_byte_sink_type.clone(), memory_byte_sink_type.clone()),
    )])));
    let lambda_interface_ref = store_object(&*storage, &*lambda_interface).await.unwrap();
    {
        let mut program_as_string = String::new();
        lambda_expression
            .expression
            .print(&mut program_as_string, 0)
            .unwrap();
        assert_eq!("(output) =>\n  output.write(literal(SmallBlob, 8c2e63300f9624b6d77695ff7f60201ca23595096c40a535ab978db997204eec1066c3f3d42c868958bbbdfb7e9ce3a2d883e19512a90d94dbcc92c10b0a642f))", program_as_string.as_str());
    }
    let read_variable: Arc<ReadVariable> = Arc::new(
        move |_name: &Name| -> Pin<Box<dyn core::future::Future<Output = Pointer> + Send>> {
            todo!()
        },
    );
    let read_literal = move |literal_type: Type,
                             value: HashedValue|
          -> Pin<Box<dyn core::future::Future<Output = Pointer> + Send>> {
        assert_eq!(small_blob_type, literal_type);
        Box::pin(async move { Pointer::Value(value) })
    };
    let main_function = evaluate(
        &lambda_expression.expression,
        &*storage,
        &read_variable,
        &read_literal,
    )
    .await;
    let result = main_function
        .call_method(
            &lambda_interface_ref,
            &apply_name,
            &Pointer::Object(Arc::new(MemoryByteSink {
                self_interface: memory_byte_sink_interface_ref,
                written: vec![],
            })),
            &*storage,
            &read_variable,
            &read_literal,
        )
        .await
        .unwrap();
    let serialized_result = match result {
        Pointer::Object(object) => object.serialize(&*storage).await.unwrap(),
        _ => panic!("Expected an object"),
    };
    let deserialized_result = MemoryByteSink::deserialize(serialized_result.value(), &*storage)
        .await
        .unwrap();
    assert_eq!(
        &memory_byte_sink_interface_ref,
        &deserialized_result.self_interface
    );
    assert_eq!(1, deserialized_result.written.len());
    assert_eq!(*hello_world_string, *deserialized_result.written[0]);
}
