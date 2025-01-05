use crate::{FileNameObject, LoadedDirectory};
use astraea::storage::{store_object, LoadValue};
use astraea::tree::BlobDigest;
use astraea::{
    storage::{InMemoryValueStorage, StoreValue},
    tree::{HashedValue, Value, ValueBlob},
};
use dogbox_tree::serialization::FileName;
use dogbox_tree_editor::OpenDirectory;
use lambda::builtins::{BUILTINS_NAMESPACE, LAMBDA_APPLY_METHOD_NAME};
use lambda::expressions::{
    evaluate, Application, Expression, LambdaExpression, Object, Pointer, ReadVariable,
};
use lambda::type_checking::TypeCheckedExpression;
use lambda::types::{Interface, Name, NamespaceId, Signature, Type, TypedExpression};
use std::collections::BTreeMap;
use std::pin::Pin;
use std::sync::Arc;

#[tokio::test]
async fn complex_expression() {
    let storage = Arc::new(InMemoryValueStorage::empty());
    let clock = || std::time::SystemTime::UNIX_EPOCH;
    let directory = Arc::new(
        OpenDirectory::create_directory(storage.clone(), clock, 1)
            .await
            .unwrap(),
    );
    let empty_file_digest = storage
        .store_value(&HashedValue::from(Arc::new(Value::new(
            ValueBlob::try_from(bytes::Bytes::new()).unwrap(),
            vec![],
        ))))
        .await
        .unwrap();
    let file_name = "test.txt";
    let open_file = directory
        .clone()
        .open_file(file_name, &empty_file_digest)
        .await
        .unwrap();
    let write_permission = open_file.get_write_permission();
    let file_content = bytes::Bytes::from_static("Hello".as_bytes());
    open_file
        .write_bytes(&write_permission, 0, file_content.clone())
        .await
        .unwrap();
    open_file.flush().await.unwrap();
    drop(open_file);
    let directory_status = directory.request_save().await.unwrap();
    assert!(directory_status.digest.is_digest_up_to_date);

    let namespace = NamespaceId([42; 16]);
    let directory_type_name = Name::new(namespace, "directory".to_string());
    let directory_type = Type::Named(directory_type_name);
    let lambda_parameter_name = Name::new(namespace.clone(), "arg".to_string());
    let read_lambda_parameter_expression = TypedExpression::new(
        Expression::ReadVariable(lambda_parameter_name.clone()),
        directory_type.clone(),
    );
    let file_name_type_name = Name::new(namespace, "file_name".to_string());
    let file_name_type = Type::Named(file_name_type_name);
    let regular_file_type_name = Name::new(namespace, "regular_file".to_string());
    let regular_file_type = Type::Named(regular_file_type_name.clone());
    //let regular_file_type_ref = store_object(&*storage, &regular_file_type).await.unwrap();
    let get_name = Name::new(namespace, "get".to_string());
    let directory_interface = Arc::new(Interface::new(BTreeMap::from([(
        get_name.clone(),
        Signature::new(file_name_type.clone(), regular_file_type.clone()),
    )])));
    let directory_interface_ref = store_object(&*storage, &*directory_interface)
        .await
        .unwrap();
    let file_name_value = HashedValue::from(Arc::new(
        Value::from_object(&FileName::try_from(file_name.to_string()).unwrap()).unwrap(),
    ));
    let get_expression = read_lambda_parameter_expression
        .apply(
            &directory_interface,
            &directory_interface_ref,
            get_name.clone(),
            TypedExpression::new(
                Expression::Literal(file_name_type.clone(), file_name_value),
                file_name_type.clone(),
            ),
        )
        .unwrap();
    let bytes_type_name = Name::new(namespace, "bytes".to_string());
    let bytes_type = Type::Named(bytes_type_name);
    let bytes_interface = Arc::new(Interface::new(BTreeMap::from([
        //TODO: add methods
    ])));
    let bytes_interface_ref = store_object(&*storage, &*bytes_interface).await.unwrap();
    let read_name = Name::new(namespace, "read".to_string());
    let regular_file_interface = Arc::new(Interface::new(BTreeMap::from([(
        read_name.clone(),
        Signature::new(Type::Unit, bytes_type.clone()),
    )])));
    let regular_file_interface_ref = store_object(&*storage, &*regular_file_interface)
        .await
        .unwrap();
    let read_expression = get_expression
        .apply(
            &regular_file_interface,
            &regular_file_interface_ref,
            read_name.clone(),
            TypedExpression::unit(),
        )
        .unwrap();
    let lambda_expression = TypedExpression::new(
        Expression::Lambda(Box::new(LambdaExpression::new(
            directory_type.clone(),
            lambda_parameter_name.clone(),
            read_expression.expression,
        ))),
        Type::Function(Box::new(Signature::new(
            directory_type.clone(),
            bytes_type.clone(),
        ))),
    );
    let apply_name = Name::new(BUILTINS_NAMESPACE, LAMBDA_APPLY_METHOD_NAME.to_string());
    let lambda_interface = Arc::new(Interface::new(BTreeMap::from([(
        apply_name.clone(),
        Signature::new(file_name_type.clone(), regular_file_type.clone()),
    )])));
    let lambda_interface_ref = store_object(&*storage, &*lambda_interface).await.unwrap();
    let external_parameter_name = Name::new(namespace, "external".to_string());
    let lambda_application = TypedExpression::new(
        Expression::Apply(Box::new(Application::new(
            lambda_expression.expression,
            lambda_interface_ref,
            apply_name.clone(),
            Expression::ReadVariable(external_parameter_name.clone()),
        ))),
        bytes_type.clone(),
    );

    {
        let mut program_as_string = String::new();
        lambda_application
            .expression
            .print(&mut program_as_string, 0)
            .unwrap();
        assert_eq!("(arg) =>\n  arg.get(literal(file_name, debd5af1f5e895bbb7fc660b5193f8e1e7bc79be1ed78aa085342a21bd5722f1941247674f4ca3d5ff7d591c5ced850bf5c666723e44d6d51ded8ec5b4049533)).read(()).apply(external)", program_as_string.as_str());
    }

    {
        let find_variable = {
            let directory_type = directory_type.clone();
            let external_parameter_name = external_parameter_name.clone();
            move |name: &Name| -> Option<Type> {
                if name == &external_parameter_name {
                    let directory_type = directory_type.clone();
                    Some(directory_type)
                } else {
                    todo!()
                }
            }
        };
        let find_interface = {
            let directory_type = directory_type.clone();
            let directory_interface_ref = directory_interface_ref.clone();
            let directory_interface = directory_interface.clone();
            move |digest: &BlobDigest,
                      callee: Arc<Type>|
                      -> Pin<
                    Box<dyn core::future::Future<Output = Option<Arc<Interface>>> + Send>,
                > {
                    if let Type::Function(signature) = &*callee {
                        // TODO: check digest
                        let generated_interface = Arc::new(Interface::new(BTreeMap::from([(
                            apply_name.clone(),
                           (** signature).clone(),
                        )])));
                        Box::pin(core::future::ready(Some(generated_interface)))
                    }
                    else if &directory_type == &*callee {
                        assert_eq!(&directory_interface_ref, digest);
                        Box::pin(core::future::ready(Some(directory_interface.clone())))
                    } else if &regular_file_type == &*callee {
                        assert_eq!(&regular_file_interface_ref, digest);
                        Box::pin(core::future::ready(Some(regular_file_interface.clone())))
                    } else {
                        assert_eq!(&bytes_type, &*callee);
                        assert_eq!(&bytes_interface_ref, digest);
                        Box::pin(core::future::ready(Some(bytes_interface.clone())))
                    }
                }
        };
        let checked =
            TypeCheckedExpression::check(&lambda_application, &find_variable, &find_interface)
                .await;
        assert_eq!(
            Ok(&lambda_application),
            checked.as_ref().map(|success| success.correct())
        );
    }

    let external_argument = {
        let loaded = storage
            .load_value(&directory_status.digest.last_known_digest)
            .await;
        match loaded {
            Some(found) => match found.hash() {
                Some(hashed) => {
                    let parsed_directory: dogbox_tree::serialization::DirectoryTree =
                        match postcard::from_bytes::<dogbox_tree::serialization::DirectoryTree>(
                            hashed.value().blob().as_slice(),
                        ) {
                            Ok(success) => success,
                            Err(_) => todo!(),
                        };
                    let hydrated: Arc<(dyn Object + Sync)> = Arc::new(LoadedDirectory::new(
                        hashed.value().clone(),
                        parsed_directory,
                        directory_interface_ref.clone(),
                        get_name.clone(),
                        regular_file_interface_ref.clone(),
                        read_name.clone(),
                        storage.clone(),
                    ));
                    Pointer::Object(hydrated)
                }
                None => todo!(),
            },
            None => todo!(),
        }
    };
    let read_variable: Arc<ReadVariable> = Arc::new(
        move |name: &Name| -> Pin<Box<dyn core::future::Future<Output = Pointer> + Send>> {
            if name == &external_parameter_name {
                let external_argument = external_argument.clone();
                Box::pin(async move { external_argument })
            } else {
                todo!()
            }
        },
    );
    let read_literal = move |literal_type: Type,
                             value: HashedValue|
          -> Pin<Box<dyn core::future::Future<Output = Pointer> + Send>> {
        assert_eq!(&file_name_type, &literal_type);
        let file_name: dogbox_tree::serialization::FileName =
            value.value().to_object().unwrap(/*TODO*/);
        Box::pin(async move { Pointer::Object(Arc::new(FileNameObject::new(file_name))) })
    };
    let evaluation_result = evaluate(
        &lambda_application.expression,
        &*storage,
        &read_variable,
        &read_literal,
    )
    .await
    .serialize(&*storage)
    .await
    .unwrap();
    assert_eq!(
        &Value::new(ValueBlob::try_from(file_content).unwrap(), Vec::new()),
        &**evaluation_result.value()
    );
}
