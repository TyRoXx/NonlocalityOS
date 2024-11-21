#[cfg(test)]
use astraea::tree::TYPE_ID_SUM;
use astraea::{
    storage::{LoadValue, StoreError, StoreValue},
    tree::{
        HashedValue, Reference, TypeId, TypedReference, Value, ValueBlob, TYPE_ID_CONSOLE,
        TYPE_ID_DELAY, TYPE_ID_EFFECT, TYPE_ID_SECONDS, TYPE_ID_STRING,
    },
};
use futures::future::join;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, io::Write, pin::Pin, sync::Arc};

#[derive(Clone, PartialEq, Debug)]
pub struct TypedValue {
    pub type_id: TypeId,
    pub value: Value,
}

impl TypedValue {
    pub fn new(type_id: TypeId, value: Value) -> TypedValue {
        TypedValue {
            type_id: type_id,
            value: value,
        }
    }
}

pub trait ReduceExpression: Sync + Send {
    fn reduce<'t>(
        &'t self,
        argument: TypedValue,
        service_resolver: &'t dyn ResolveServiceId,
        loader: &'t dyn LoadValue,
        storage: &'t dyn StoreValue,
    ) -> Pin<Box<dyn std::future::Future<Output = Value> + 't>>;
}

#[derive(Clone, PartialEq, Debug)]
pub enum ReductionError {
    NoServiceForType(TypeId),
    Io(StoreError),
    UnknownReference(Reference),
}

pub trait ResolveServiceId {
    fn resolve(&self, service_id: &TypeId) -> Option<Arc<dyn ReduceExpression>>;
}

pub async fn reduce_expression_without_storing_the_final_result(
    argument: TypedValue,
    service_resolver: &dyn ResolveServiceId,
    loader: &dyn LoadValue,
    storage: &dyn StoreValue,
) -> std::result::Result<Value, ReductionError> {
    let service = match service_resolver.resolve(&argument.type_id) {
        Some(service) => service,
        None => return Err(ReductionError::NoServiceForType(argument.type_id)),
    };
    let result = service
        .reduce(argument, service_resolver, loader, storage)
        .await;
    Ok(result)
}

pub struct ReferencedValue {
    reference: TypedReference,
    value: Arc<Value>,
}

impl ReferencedValue {
    fn new(reference: TypedReference, value: Arc<Value>) -> ReferencedValue {
        ReferencedValue { reference, value }
    }
}

pub async fn reduce_expression_from_reference(
    argument: &TypedReference,
    service_resolver: &dyn ResolveServiceId,
    loader: &dyn LoadValue,
    storage: &dyn StoreValue,
) -> std::result::Result<ReferencedValue, ReductionError> {
    let argument_value = match loader.load_value(&argument.reference).await {
        Some(loaded) => loaded,
        None => return Err(ReductionError::UnknownReference(argument.reference)),
    }
    .hash()
    .unwrap();
    let value = reduce_expression_without_storing_the_final_result(
        TypedValue::new(
            argument.type_id,
            /*TODO: avoid this clone*/ (**argument_value.value()).clone(),
        ),
        service_resolver,
        loader,
        storage,
    )
    .await?;
    let arc_value = Arc::new(value);
    Ok(ReferencedValue::new(
        storage
            .store_value(&HashedValue::from(arc_value.clone()))
            .await
            .map_err(|error| ReductionError::Io(error))?
            .add_type(argument.type_id),
        arc_value,
    ))
}

pub struct ServiceRegistry {
    services: BTreeMap<TypeId, Arc<dyn ReduceExpression>>,
}

impl ServiceRegistry {
    pub fn new(services: BTreeMap<TypeId, Arc<dyn ReduceExpression>>) -> ServiceRegistry {
        ServiceRegistry { services }
    }
}

impl ResolveServiceId for ServiceRegistry {
    fn resolve(&self, service_id: &TypeId) -> Option<Arc<dyn ReduceExpression>> {
        self.services.get(service_id).cloned()
    }
}

pub struct TestConsole {
    writer: tokio::sync::mpsc::UnboundedSender<String>,
}

impl ReduceExpression for TestConsole {
    fn reduce<'t>(
        &'t self,
        argument: TypedValue,
        service_resolver: &'t dyn ResolveServiceId,
        loader: &'t dyn LoadValue,
        storage: &'t dyn StoreValue,
    ) -> Pin<Box<dyn std::future::Future<Output = Value> + 't>> {
        Box::pin(async move {
            assert_eq!(2, argument.value.references().len());
            let past = TypedReference::new(TYPE_ID_EFFECT, argument.value.references()[0].clone());
            let past_ref =
                reduce_expression_from_reference(&past, service_resolver, loader, storage);
            let message =
                TypedReference::new(TYPE_ID_STRING, argument.value.references()[1].clone());
            let message_ref =
                reduce_expression_from_reference(&message, service_resolver, loader, storage);
            let (past_result, message_result) = join(past_ref, message_ref).await;
            let past_value = past_result.unwrap();
            let message_string = message_result.unwrap().value.to_string().unwrap();
            self.writer.send(message_string).unwrap();
            make_effect(past_value.reference.reference)
        })
    }
}

pub struct Identity {}

impl ReduceExpression for Identity {
    fn reduce(
        &self,
        argument: TypedValue,
        _service_resolver: &dyn ResolveServiceId,
        _loader: &dyn LoadValue,
        _storage: &dyn StoreValue,
    ) -> Pin<Box<dyn std::future::Future<Output = Value>>> {
        Box::pin(std::future::ready(argument.value))
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_reduce_expression() {
    use astraea::storage::InMemoryValueStorage;
    use tokio::sync::Mutex;
    let identity: Arc<dyn ReduceExpression> = Arc::new(Identity {});
    let services = ServiceRegistry {
        services: BTreeMap::from([(TYPE_ID_STRING, identity.clone()), (TypeId(1), identity)]),
    };
    let value_storage = InMemoryValueStorage::new(Mutex::new(BTreeMap::new()));
    let result = reduce_expression_without_storing_the_final_result(
        TypedValue::new(
            TYPE_ID_STRING,
            Value::from_string("hello, world!\n").unwrap(),
        ),
        &services,
        &value_storage,
        &value_storage,
    )
    .await
    .unwrap();
    assert_eq!(Some("hello, world!\n".to_string()), result.to_string());
}

pub fn make_text_in_console(past: Reference, text: Reference) -> TypedValue {
    TypedValue::new(
        TYPE_ID_CONSOLE,
        Value {
            blob: ValueBlob::empty(),
            references: vec![past, text],
        },
    )
}

pub fn make_beginning_of_time() -> Value {
    Value {
        blob: ValueBlob::empty(),
        references: vec![],
    }
}

pub fn make_effect(cause: Reference) -> Value {
    Value::new(ValueBlob::empty(), vec![cause])
}

#[tokio::test(flavor = "multi_thread")]
async fn test_effect() {
    use astraea::storage::InMemoryValueStorage;
    use tokio::sync::Mutex;
    let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
    let test_console: Arc<dyn ReduceExpression> = Arc::new(TestConsole { writer: sender });
    let identity: Arc<dyn ReduceExpression> = Arc::new(Identity {});
    let services = ServiceRegistry {
        services: BTreeMap::from([
            (TYPE_ID_STRING, identity.clone()),
            (TypeId(1), identity.clone()),
            (TYPE_ID_CONSOLE, test_console),
            (TYPE_ID_EFFECT, identity),
        ]),
    };

    let value_storage = InMemoryValueStorage::new(Mutex::new(BTreeMap::new()));
    let past = value_storage
        .store_value(&HashedValue::from(Arc::new(make_beginning_of_time())))
        .await
        .unwrap();
    let message = value_storage
        .store_value(&HashedValue::from(Arc::new(
            Value::from_string("hello, world!\n").unwrap(),
        )))
        .await
        .unwrap();
    let text_in_console = make_text_in_console(past, message);
    let result = reduce_expression_without_storing_the_final_result(
        text_in_console,
        &services,
        &value_storage,
        &value_storage,
    )
    .await
    .unwrap();
    assert_eq!(make_effect(past), result);
    assert_eq!(Some("hello, world!\n".to_string()), receiver.recv().await);
}

pub fn make_seconds(amount: u64) -> Value {
    Value {
        blob: ValueBlob::try_from(bytes::Bytes::copy_from_slice(&amount.to_be_bytes())).unwrap(),
        references: Vec::new(),
    }
}

pub fn to_seconds(value: &Value) -> Option<u64> {
    let mut buf: [u8; 8] = [0; 8];
    if buf.len() != value.blob.as_slice().len() {
        return None;
    }
    buf.copy_from_slice(value.blob.as_slice());
    Some(u64::from_be_bytes(buf))
}

pub fn make_sum(summands: Vec<Reference>) -> Value {
    Value {
        blob: ValueBlob::empty(),
        references: summands,
    }
}

pub fn to_sum(value: Value) -> Option<Vec<Reference>> {
    Some(value.references)
}

pub struct SumService {}

impl ReduceExpression for SumService {
    fn reduce<'t>(
        &'t self,
        argument: TypedValue,
        service_resolver: &'t dyn ResolveServiceId,
        loader: &'t dyn LoadValue,
        storage: &'t dyn StoreValue,
    ) -> Pin<Box<dyn std::future::Future<Output = Value> + 't>> {
        let summands_expressions = to_sum(argument.value).unwrap();
        Box::pin(async move {
            let summands_futures: Vec<_> = summands_expressions
                .iter()
                .map(|summand| async {
                    reduce_expression_from_reference(
                        &TypedReference::new(TYPE_ID_SECONDS, *summand),
                        service_resolver,
                        loader,
                        storage,
                    )
                    .await
                })
                .collect();
            let summands_values = futures::future::join_all(summands_futures).await;
            let sum = summands_values.iter().fold(0u64, |accumulator, element| {
                let summand = to_seconds(&element.as_ref().unwrap().value).unwrap();
                u64::checked_add(accumulator, summand).unwrap()
            });
            make_seconds(sum)
        })
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_sum() {
    use astraea::storage::InMemoryValueStorage;
    use tokio::sync::Mutex;
    let sum_service: Arc<dyn ReduceExpression> = Arc::new(SumService {});
    let identity: Arc<dyn ReduceExpression> = Arc::new(Identity {});
    let services = ServiceRegistry {
        services: BTreeMap::from([(TYPE_ID_SECONDS, identity), (TYPE_ID_SUM, sum_service)]),
    };
    let value_storage = InMemoryValueStorage::new(Mutex::new(BTreeMap::new()));
    let a = value_storage
        .store_value(&HashedValue::from(Arc::new(make_seconds(1))))
        .await
        .unwrap();
    let b = value_storage
        .store_value(&HashedValue::from(Arc::new(make_seconds(2))))
        .await
        .unwrap();
    let sum = value_storage
        .store_value(&HashedValue::from(Arc::new(make_sum(vec![a, b]))))
        .await
        .unwrap()
        .add_type(TYPE_ID_SUM);
    let result = reduce_expression_from_reference(&sum, &services, &value_storage, &value_storage)
        .await
        .unwrap();
    assert_eq!(make_seconds(3), *result.value);
}

pub fn make_delay(before: Reference, duration: Reference) -> TypedValue {
    TypedValue::new(
        TYPE_ID_DELAY,
        Value {
            blob: ValueBlob::empty(),
            references: vec![before, duration],
        },
    )
}

pub struct DelayService {}

impl ReduceExpression for DelayService {
    fn reduce<'t>(
        &'t self,
        mut argument: TypedValue,
        service_resolver: &'t dyn ResolveServiceId,
        loader: &'t dyn LoadValue,
        storage: &'t dyn StoreValue,
    ) -> Pin<Box<dyn std::future::Future<Output = Value> + 't>> {
        let mut arguments = argument.value.references.drain(0..2);
        let before_ref = arguments.next().unwrap();
        let duration_ref = arguments.next().unwrap();
        assert!(arguments.next().is_none());
        Box::pin(async move {
            let before_future = async move {
                reduce_expression_from_reference(
                    &TypedReference::new(TYPE_ID_EFFECT, before_ref),
                    service_resolver,
                    loader,
                    storage,
                )
                .await
            };
            let duration_future = async move {
                reduce_expression_from_reference(
                    &TypedReference::new(TYPE_ID_SECONDS, duration_ref),
                    service_resolver,
                    loader,
                    storage,
                )
                .await
            };
            let (before_result, duration_result) = join(before_future, duration_future).await;
            let duration = duration_result.unwrap().value;
            let seconds = to_seconds(&duration).unwrap();
            tokio::time::sleep(std::time::Duration::from_secs(seconds)).await;
            make_effect(before_result.unwrap().reference.reference)
        })
    }
}

pub struct ActualConsole {}

impl ReduceExpression for ActualConsole {
    fn reduce<'t>(
        &'t self,
        argument: TypedValue,
        service_resolver: &'t dyn ResolveServiceId,
        loader: &'t dyn LoadValue,
        storage: &'t dyn StoreValue,
    ) -> Pin<Box<dyn std::future::Future<Output = Value> + 't>> {
        Box::pin(async move {
            assert_eq!(2, argument.value.references.len());
            let past_ref = async {
                reduce_expression_from_reference(
                    &TypedReference::new(TYPE_ID_EFFECT, argument.value.references[0]),
                    service_resolver,
                    loader,
                    storage,
                )
                .await
            };
            let message_ref = async {
                reduce_expression_from_reference(
                    &TypedReference::new(TYPE_ID_STRING, argument.value.references[1]),
                    service_resolver,
                    loader,
                    storage,
                )
                .await
            };
            let (past_result, message_result) = join(past_ref, message_ref).await;
            let past = past_result.unwrap();
            let message_string = message_result.unwrap().value.to_string().unwrap();
            print!("{}", &message_string);
            std::io::stdout().flush().unwrap();
            make_effect(past.reference.reference)
        })
    }
}

pub struct Lambda {
    variable: Reference,
    body: Reference,
}

impl Lambda {
    pub fn new(variable: Reference, body: Reference) -> Self {
        Self {
            variable: variable,
            body: body,
        }
    }
}

pub fn make_lambda(lambda: Lambda) -> Value {
    Value {
        blob: ValueBlob::empty(),
        references: vec![lambda.variable, lambda.body],
    }
}

pub fn to_lambda(value: Value) -> Option<Lambda> {
    if value.references.len() != 2 {
        return None;
    }
    Some(Lambda::new(value.references[0], value.references[1]))
}

pub struct LambdaApplication {
    function: Reference,
    argument: Reference,
}

impl LambdaApplication {
    pub fn new(function: Reference, argument: Reference) -> Self {
        Self {
            function: function,
            argument: argument,
        }
    }
}

pub fn make_lambda_application(function: Reference, argument: Reference) -> Value {
    Value {
        blob: ValueBlob::empty(),
        references: vec![function, argument],
    }
}

pub fn to_lambda_application(value: Value) -> Option<LambdaApplication> {
    if value.references.len() != 2 {
        return None;
    }
    Some(LambdaApplication::new(
        value.references[0],
        value.references[1],
    ))
}

async fn replace_variable_recursively(
    body: &Reference,
    variable: &Reference,
    argument: &Reference,
    loader: &dyn LoadValue,
    storage: &dyn StoreValue,
) -> Option<Value> {
    let body_loaded = loader.load_value(&body).await.unwrap().hash().unwrap();
    let mut references: Vec<Reference> = Vec::new();
    let mut has_replaced_something = false;
    for child in &body_loaded.value().references {
        if child == variable {
            references.push(argument.clone());
            has_replaced_something = true;
        } else {
            if let Some(replaced) = Box::pin(replace_variable_recursively(
                child, variable, argument, loader, storage,
            ))
            .await
            {
                let stored = storage
                    .store_value(&HashedValue::from(Arc::new(replaced)))
                    .await
                    .unwrap(/*TODO*/);
                references.push(stored);
                has_replaced_something = true;
            } else {
                references.push(*child);
            }
        }
    }
    if !has_replaced_something {
        return None;
    }
    Some(Value::new(body_loaded.value().blob().clone(), references))
}

pub struct LambdaApplicationService {}

impl ReduceExpression for LambdaApplicationService {
    fn reduce<'t>(
        &'t self,
        argument: TypedValue,
        _service_resolver: &'t dyn ResolveServiceId,
        loader: &'t dyn LoadValue,
        storage: &'t dyn StoreValue,
    ) -> Pin<Box<dyn std::future::Future<Output = Value> + 't>> {
        let lambda_application = to_lambda_application(argument.value).unwrap();
        Box::pin(async move {
            let argument = &lambda_application.argument;
            let function = to_lambda(
                (**loader
                    .load_value(&lambda_application.function)
                    .await
                    .unwrap()
                    .hash()
                    .unwrap()
                    .value())
                .clone(),
            )
            .unwrap();
            let variable = &function.variable;
            match replace_variable_recursively(&function.body, &variable, argument, loader, storage)
                .await
            {
                Some(replaced) => replaced,
                None => (**loader
                    .load_value(&function.body)
                    .await
                    .unwrap()
                    .hash()
                    .unwrap()
                    .value())
                .clone(),
            }
        })
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Copy, Hash)]
pub struct SourceLocation {
    pub line: u64,
    pub column: u64,
}

impl SourceLocation {
    pub fn new(line: u64, column: u64) -> Self {
        Self { line, column }
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone)]
pub struct CompilerError {
    pub message: String,
    pub location: SourceLocation,
}

impl CompilerError {
    pub fn new(message: String, location: SourceLocation) -> Self {
        Self { message, location }
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone)]
pub struct CompilerOutput {
    pub entry_point: Reference,
    pub errors: Vec<CompilerError>,
}

impl CompilerOutput {
    pub fn new(entry_point: Reference, errors: Vec<CompilerError>) -> CompilerOutput {
        CompilerOutput {
            entry_point: entry_point,
            errors: errors,
        }
    }

    pub fn from_value(input: Value) -> Option<CompilerOutput> {
        if input.references.len() != 1 {
            return None;
        }
        let errors: Vec<CompilerError> = match postcard::from_bytes(input.blob.as_slice()) {
            Ok(parsed) => parsed,
            Err(_) => return None,
        };
        Some(CompilerOutput::new(input.references[0], errors))
    }

    pub fn to_value(self) -> Option<Value> {
        ValueBlob::try_from(postcard::to_allocvec(&self.errors).unwrap().into())
            .map(|value_blob| Value::new(value_blob, vec![self.entry_point]))
    }
}

pub struct CompiledReducer {}

impl ReduceExpression for CompiledReducer {
    fn reduce<'t>(
        &'t self,
        argument: TypedValue,
        _service_resolver: &'t dyn ResolveServiceId,
        loader: &'t dyn LoadValue,
        storage: &'t dyn StoreValue,
    ) -> Pin<Box<dyn std::future::Future<Output = Value> + 't>> {
        Box::pin(async move {
            let source_ref = argument.value.references[0];
            let source_value = loader
                .load_value(&source_ref)
                .await
                .unwrap()
                .hash()
                .unwrap();
            let source_string = source_value.value().to_string().unwrap();
            let compiler_output: CompilerOutput =
                crate::compiler::compile(&source_string, storage).await;
            compiler_output.to_value().unwrap(/*TODO*/)
        })
    }
}
