use crate::{
    builtins::LAMBDA_APPLY_METHOD_NAME,
    standard_library::Effect,
    types::{Name, Type},
};
use astraea::tree::{BlobDigest, HashedValue, Value};
use astraea::{
    storage::{LoadValue, StoreError, StoreValue},
    tree::ValueBlob,
};
use async_trait::async_trait;
use std::{
    collections::{BTreeMap, BTreeSet},
    pin::Pin,
    sync::Arc,
};

#[derive(Debug, Ord, Eq, PartialEq, PartialOrd, Hash, Clone)]
pub struct Application {
    pub callee: Expression,
    pub callee_interface: BlobDigest,
    pub method: Name,
    pub argument: Expression,
}

impl Application {
    pub fn new(
        callee: Expression,
        callee_interface: BlobDigest,
        method: Name,
        argument: Expression,
    ) -> Self {
        Self {
            callee,
            callee_interface,
            method,
            argument,
        }
    }
}

#[derive(Debug, Ord, Eq, PartialEq, PartialOrd, Hash, Clone)]
pub struct LambdaExpression {
    pub parameter_type: Type,
    pub parameter_name: Name,
    pub body: Expression,
}

impl LambdaExpression {
    pub fn new(parameter_type: Type, parameter_name: Name, body: Expression) -> Self {
        Self {
            parameter_type,
            parameter_name,
            body,
        }
    }

    pub fn find_captured_names(&self) -> BTreeSet<Name> {
        self.body.find_captured_names()
    }
}

#[derive(Debug, PartialEq, Eq, Ord, PartialOrd, Hash, Clone)]
pub enum Expression {
    Unit,
    Literal(Type, HashedValue),
    Apply(Box<Application>),
    ReadVariable(Name),
    Lambda(Box<LambdaExpression>),
    ConstructEffect(Type, Vec<Expression>),
}

impl Expression {
    pub fn print(&self, writer: &mut dyn std::fmt::Write, level: usize) -> std::fmt::Result {
        match self {
            Expression::Unit => write!(writer, "()"),
            Expression::Literal(literal_type, literal_value) => {
                write!(writer, "literal(")?;
                literal_type.print(writer, level)?;
                write!(writer, ", {})", literal_value.digest())
            }
            Expression::Apply(application) => {
                application.callee.print(writer, level)?;
                write!(writer, ".{}", &application.method.key)?;
                write!(writer, "(")?;
                application.argument.print(writer, level)?;
                write!(writer, ")")
            }
            Expression::ReadVariable(name) => {
                write!(writer, "{}", &name.key)
            }
            Expression::Lambda(lambda_expression) => {
                write!(writer, "({}) =>\n", &lambda_expression.parameter_name.key)?;
                let indented = level + 1;
                for _ in 0..(indented * 2) {
                    write!(writer, " ")?;
                }
                lambda_expression.body.print(writer, level + 1)
            }
            Expression::ConstructEffect(constructed_type, arguments) => {
                write!(writer, "construct(")?;
                constructed_type.print(writer, level)?;
                for argument in arguments {
                    write!(writer, ", ")?;
                    argument.print(writer, level)?;
                }
                write!(writer, ")")
            }
        }
    }

    pub fn to_string(&self) -> String {
        let mut result = String::new();
        self.print(&mut result, 0).unwrap();
        result
    }

    pub fn find_captured_names(&self) -> BTreeSet<Name> {
        match self {
            Expression::Unit => BTreeSet::new(),
            Expression::Literal(_, _blob_digest) => BTreeSet::new(),
            Expression::Apply(application) => {
                let mut result = application.argument.find_captured_names();
                result.append(&mut application.argument.find_captured_names());
                result
            }
            Expression::ReadVariable(name) => BTreeSet::from([name.clone()]),
            Expression::Lambda(lambda_expression) => {
                let mut result = lambda_expression.body.find_captured_names();
                result.remove(&lambda_expression.parameter_name);
                result
            }
            Expression::ConstructEffect(_constructed_type, arguments) => {
                let mut result = BTreeSet::new();
                for argument in arguments {
                    result.append(&mut argument.find_captured_names());
                }
                result
            }
        }
    }
}

#[async_trait]
pub trait Object: std::fmt::Debug + Send {
    async fn call_method(
        &self,
        interface: &BlobDigest,
        method: &Name,
        argument: &Pointer,
        load_value: &(dyn LoadValue + Sync),
        store_value: &(dyn StoreValue + Sync),
        read_variable: &Arc<ReadVariable>,
        read_literal: &ReadLiteral,
    ) -> std::result::Result<Pointer, StoreError>;

    async fn serialize(
        &self,
        storage: &(dyn StoreValue + Sync),
    ) -> std::result::Result<HashedValue, StoreError>;

    async fn serialize_to_flat_value(&self) -> Option<Arc<Value>>;
}

#[derive(Debug)]
pub struct Closure {
    lambda: LambdaExpression,
    captured_variables: BTreeMap<Name, Pointer>,
}

impl Closure {
    pub fn new(lambda: LambdaExpression, captured_variables: BTreeMap<Name, Pointer>) -> Self {
        Self {
            lambda,
            captured_variables,
        }
    }
}

#[async_trait]
impl Object for Closure {
    async fn call_method(
        &self,
        /*TODO: use the interface for something*/ _interface: &BlobDigest,
        method: &Name,
        argument: &Pointer,
        load_value: &(dyn LoadValue + Sync),
        store_value: &(dyn StoreValue + Sync),
        read_variable: &Arc<ReadVariable>,
        read_literal: &ReadLiteral,
    ) -> std::result::Result<Pointer, StoreError> {
        if method.key != LAMBDA_APPLY_METHOD_NAME {
            todo!()
        }
        let read_variable_in_body: Arc<ReadVariable> = Arc::new({
            let parameter_name = self.lambda.parameter_name.clone();
            let argument = argument.clone();
            let captured_variables = self.captured_variables.clone();
            let read_variable = read_variable.clone();
            move |name: &Name| -> Pin<Box<dyn core::future::Future<Output = Pointer> + Send>> {
                if name == &parameter_name {
                    let argument = argument.clone();
                    Box::pin(core::future::ready(argument))
                } else if let Some(found) = captured_variables.get(name) {
                    Box::pin(core::future::ready(found.clone()))
                } else {
                    read_variable(name)
                }
            }
        });
        evaluate(
            &self.lambda.body,
            load_value,
            store_value,
            &read_variable_in_body,
            read_literal,
        )
        .await
    }

    async fn serialize(
        &self,
        _storage: &(dyn StoreValue + Sync),
    ) -> std::result::Result<HashedValue, StoreError> {
        todo!()
    }

    async fn serialize_to_flat_value(&self) -> Option<Arc<Value>> {
        todo!()
    }
}

#[derive(Debug, Clone)]
pub struct InMemoryValue {
    pub blob: ValueBlob,
    pub references: Vec<Pointer>,
}

impl InMemoryValue {
    pub fn new(blob: ValueBlob, references: Vec<Pointer>) -> Self {
        Self { blob, references }
    }
}

#[derive(Debug, Clone)]
pub enum Pointer {
    Value(HashedValue),
    Object(Arc<(dyn Object + Sync)>),
    Reference(BlobDigest),
    InMemoryValue(InMemoryValue),
}

impl Pointer {
    pub async fn call_method(
        &self,
        interface: &BlobDigest,
        method: &Name,
        argument: &Pointer,
        load_value: &(dyn LoadValue + Sync),
        store_value: &(dyn StoreValue + Sync),
        read_variable: &Arc<ReadVariable>,
        read_literal: &ReadLiteral,
    ) -> std::result::Result<Pointer, StoreError> {
        match self {
            Pointer::Value(_hashed_value) => todo!(),
            Pointer::Object(arc) => {
                arc.call_method(
                    interface,
                    method,
                    argument,
                    load_value,
                    store_value,
                    read_variable,
                    read_literal,
                )
                .await
            }
            Pointer::Reference(_blob_digest) => todo!(),
            Pointer::InMemoryValue(_in_memory_value) => {
                todo!()
            }
        }
    }

    pub async fn serialize(
        self,
        storage: &(dyn StoreValue + Sync),
    ) -> std::result::Result<HashedValue, StoreError> {
        match self {
            Pointer::Value(hashed_value) => Ok(hashed_value),
            Pointer::Object(arc) => arc.serialize(storage).await,
            Pointer::Reference(_blob_digest) => todo!(),
            Pointer::InMemoryValue(_in_memory_value) => {
                todo!()
            }
        }
    }

    pub async fn serialize_to_flat_value(&self) -> Option<Arc<Value>> {
        match self {
            Pointer::Value(hashed_value) => {
                if hashed_value.value().references().is_empty() {
                    Some(hashed_value.value().clone())
                } else {
                    None
                }
            }
            Pointer::Object(arc) => arc.serialize_to_flat_value().await,
            Pointer::Reference(_blob_digest) => todo!(),
            Pointer::InMemoryValue(_in_memory_value) => {
                todo!()
            }
        }
    }
}

pub type ReadVariable =
    dyn Fn(&Name) -> Pin<Box<dyn core::future::Future<Output = Pointer> + Send>> + Send + Sync;

pub type ReadLiteral = dyn Fn(Type, HashedValue) -> Pin<Box<dyn core::future::Future<Output = Pointer> + Send>>
    + Send
    + Sync;

pub async fn evaluate(
    expression: &Expression,
    load_value: &(dyn LoadValue + Sync),
    store_value: &(dyn StoreValue + Sync),
    read_variable: &Arc<ReadVariable>,
    read_literal: &ReadLiteral,
) -> std::result::Result<Pointer, StoreError> {
    match expression {
        Expression::Unit => return Ok(Pointer::Value(HashedValue::from(Arc::new(Value::empty())))),
        Expression::Literal(literal_type, literal_value) => {
            let literal = read_literal(literal_type.clone(), literal_value.clone()).await;
            Ok(literal)
        }
        Expression::Apply(application) => {
            let evaluated_callee = Box::pin(evaluate(
                &application.callee,
                load_value,
                store_value,
                read_variable,
                read_literal,
            ))
            .await?;
            let evaluated_argument = Box::pin(evaluate(
                &application.argument,
                load_value,
                store_value,
                read_variable,
                read_literal,
            ))
            .await?;
            let call_result = evaluated_callee
                .call_method(
                    &application.callee_interface,
                    &application.method,
                    &evaluated_argument,
                    load_value,
                    store_value,
                    read_variable,
                    read_literal,
                )
                .await;
            call_result
        }
        Expression::ReadVariable(name) => Ok(read_variable(&name).await),
        Expression::Lambda(lambda_expression) => {
            let mut captured_variables = BTreeMap::new();
            for captured_name in lambda_expression.find_captured_names().into_iter() {
                let captured_value = read_variable(&captured_name).await;
                captured_variables.insert(captured_name, captured_value);
            }
            Ok(Pointer::Object(Arc::new(Closure::new(
                (**lambda_expression).clone(),
                captured_variables,
            ))))
        }
        Expression::ConstructEffect(constructed_type, arguments) => {
            let mut evaluated_arguments = Vec::new();
            for argument in arguments {
                let evaluated_argument = Box::pin(evaluate(
                    argument,
                    load_value,
                    store_value,
                    read_variable,
                    read_literal,
                ))
                .await?;
                evaluated_arguments
                    .push(*evaluated_argument.serialize(store_value).await?.digest());
            }
            let constructed_type_stored = store_value
                .store_value(&HashedValue::from(Arc::new(constructed_type.to_value())))
                .await?;
            let argument = store_value
                .store_value(&HashedValue::from(Arc::new(Value::new(
                    ValueBlob::empty(),
                    evaluated_arguments,
                ))))
                .await?;
            Ok(Pointer::Value(HashedValue::from(Arc::new(
                Effect::new(constructed_type_stored, argument).to_value(),
            ))))
        }
    }
}
