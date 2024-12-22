use crate::{
    storage::{LoadValue, StoreError, StoreValue},
    tree::{BlobDigest, HashedValue, Reference, Value},
    types::{Name, Type},
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
    Literal(Type, BlobDigest),
    Apply(Box<Application>),
    ReadVariable(Name),
    Lambda(Box<LambdaExpression>),
}

impl Expression {
    pub fn print(&self, writer: &mut dyn std::fmt::Write, level: usize) -> std::fmt::Result {
        match self {
            Expression::Unit => write!(writer, "()"),
            Expression::Literal(literal_type, blob_digest) => {
                write!(writer, "literal(")?;
                literal_type.print(writer, level)?;
                write!(writer, ", {})", blob_digest)
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
                write!(writer, "^{}", &lambda_expression.parameter_name.key)?;
                write!(writer, " .\n")?;
                let indented = level + 1;
                for _ in 0..(indented * 2) {
                    write!(writer, " ")?;
                }
                lambda_expression.body.print(writer, level + 1)
            }
        }
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
        storage: &(dyn LoadValue + Sync),
        read_variable: &Arc<ReadVariable>,
        read_literal: &ReadLiteral,
    ) -> std::result::Result<Pointer, ()>;

    async fn serialize(
        &self,
        storage: &dyn StoreValue,
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
        storage: &(dyn LoadValue + Sync),
        read_variable: &Arc<ReadVariable>,
        read_literal: &ReadLiteral,
    ) -> std::result::Result<Pointer, ()> {
        if method.key != "apply" {
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
        Ok(evaluate(
            &self.lambda.body,
            storage,
            &read_variable_in_body,
            read_literal,
        )
        .await)
    }

    async fn serialize(
        &self,
        _storage: &dyn StoreValue,
    ) -> std::result::Result<HashedValue, StoreError> {
        todo!()
    }

    async fn serialize_to_flat_value(&self) -> Option<Arc<Value>> {
        todo!()
    }
}

#[derive(Debug, Clone)]
pub enum Pointer {
    Value(HashedValue),
    Object(Arc<(dyn Object + Sync)>),
    Reference(BlobDigest),
}

impl Pointer {
    async fn call_method(
        &self,
        interface: &BlobDigest,
        method: &Name,
        argument: &Pointer,
        storage: &(dyn LoadValue + Sync),
        read_variable: &Arc<ReadVariable>,
        read_literal: &ReadLiteral,
    ) -> std::result::Result<Pointer, ()> {
        match self {
            Pointer::Value(_hashed_value) => todo!(),
            Pointer::Object(arc) => {
                arc.call_method(
                    interface,
                    method,
                    argument,
                    storage,
                    read_variable,
                    read_literal,
                )
                .await
            }
            Pointer::Reference(_blob_digest) => todo!(),
        }
    }

    pub async fn serialize(
        self,
        storage: &dyn StoreValue,
    ) -> std::result::Result<HashedValue, StoreError> {
        match self {
            Pointer::Value(hashed_value) => Ok(hashed_value),
            Pointer::Object(arc) => arc.serialize(storage).await,
            Pointer::Reference(_blob_digest) => todo!(),
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
    storage: &(dyn LoadValue + Sync),
    read_variable: &Arc<ReadVariable>,
    read_literal: &ReadLiteral,
) -> Pointer {
    match expression {
        Expression::Unit => return Pointer::Value(HashedValue::from(Arc::new(Value::from_unit()))),
        Expression::Literal(literal_type, blob_digest) => {
            let loaded: Option<crate::storage::DelayedHashedValue> =
                storage.load_value(&Reference::new(*blob_digest)).await;
            match loaded {
                Some(found) => match found.hash() {
                    Some(hashed) => {
                        let literal = read_literal(literal_type.clone(), hashed).await;
                        literal
                    }
                    None => todo!(),
                },
                None => todo!(),
            }
        }
        Expression::Apply(application) => {
            let evaluated_callee = Box::pin(evaluate(
                &application.callee,
                storage,
                read_variable,
                read_literal,
            ))
            .await;
            let evaluated_argument = Box::pin(evaluate(
                &application.argument,
                storage,
                read_variable,
                read_literal,
            ))
            .await;
            let call_result = evaluated_callee
                .call_method(
                    &application.callee_interface,
                    &application.method,
                    &evaluated_argument,
                    storage,read_variable, read_literal
                )
                .await
                .unwrap(/*TODO*/);
            call_result
        }
        Expression::ReadVariable(name) => read_variable(&name).await,
        Expression::Lambda(lambda_expression) => {
            let mut captured_variables = BTreeMap::new();
            for captured_name in lambda_expression.find_captured_names().into_iter() {
                let captured_value = read_variable(&captured_name).await;
                captured_variables.insert(captured_name, captured_value);
            }
            Pointer::Object(Arc::new(Closure::new(
                (**lambda_expression).clone(),
                captured_variables,
            )))
        }
    }
}
