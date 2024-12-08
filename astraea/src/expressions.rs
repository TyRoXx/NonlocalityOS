use crate::{
    storage::{LoadValue, StoreError, StoreValue},
    tree::{BlobDigest, HashedValue, Reference, Value},
    types::{Name, Type, TypedExpression},
};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::{pin::Pin, sync::Arc};

#[derive(Debug, PartialEq, PartialOrd, Hash, Clone, Serialize, Deserialize)]
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

#[derive(Debug, PartialEq, PartialOrd, Hash, Clone, Serialize, Deserialize)]
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
}

#[derive(Debug, PartialEq, PartialOrd, Hash, Clone, Serialize, Deserialize)]
pub enum Expression {
    Hole,
    Unit,
    Literal(Type, BlobDigest),
    Apply(Box<Application>),
    ReadVariable(Name),
    Lambda(Box<LambdaExpression>),
}

impl Expression {
    pub fn print(&self, writer: &mut dyn std::fmt::Write, level: usize) -> std::fmt::Result {
        match self {
            Expression::Hole => write!(writer, "?"),
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
}

#[async_trait]
pub trait Object: std::fmt::Debug + Send {
    async fn call_method(
        &self,
        interface: &BlobDigest,
        method: &Name,
        argument: &Pointer,
    ) -> std::result::Result<Pointer, ()>;

    async fn serialize(
        &self,
        storage: &dyn StoreValue,
    ) -> std::result::Result<HashedValue, StoreError>;

    async fn serialize_to_flat_value(&self) -> Option<Arc<Value>>;
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
    ) -> std::result::Result<Pointer, ()> {
        match self {
            Pointer::Value(_hashed_value) => todo!(),
            Pointer::Object(arc) => arc.call_method(interface, method, argument).await,
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

pub enum EvaluatedStep {
    Next(TypedExpression),
    Last(Pointer),
}

pub type ReadVariable =
    dyn Fn(&Name) -> Pin<Box<dyn core::future::Future<Output = Pointer> + Send>>;

pub type ReadLiteral =
    dyn Fn(&Type, &HashedValue) -> Pin<Box<dyn core::future::Future<Output = Pointer> + Send>>;

pub async fn evaluate_step(
    expression: &Expression,
    storage: &dyn LoadValue,
    read_variable: &ReadVariable,
    read_literal: &ReadLiteral,
) -> EvaluatedStep {
    match expression {
        Expression::Hole => todo!(),
        Expression::Unit => {
            return EvaluatedStep::Last(Pointer::Value(HashedValue::from(Arc::new(
                Value::from_unit(),
            ))))
        }
        Expression::Literal(literal_type, blob_digest) => {
            let loaded: Option<crate::storage::DelayedHashedValue> =
                storage.load_value(&Reference::new(*blob_digest)).await;
            match loaded {
                Some(found) => match found.hash() {
                    Some(hashed) => {
                        let literal = read_literal(literal_type, &hashed).await;
                        EvaluatedStep::Last(literal)
                    }
                    None => todo!(),
                },
                None => EvaluatedStep::Next(TypedExpression::hole()),
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
                )
                .await
                .unwrap(/*TODO*/);
            EvaluatedStep::Last(call_result)
        }
        Expression::ReadVariable(name) => EvaluatedStep::Last(read_variable(&name).await),
        Expression::Lambda(lambda_expression) => {
            // capture the environment
            todo!()
        }
    }
}

pub async fn evaluate(
    expression: &Expression,
    storage: &dyn LoadValue,
    read_variable: &ReadVariable,
    read_literal: &ReadLiteral,
) -> Pointer {
    let mut evaluated = evaluate_step(expression, storage, read_variable, read_literal).await;
    loop {
        match evaluated {
            EvaluatedStep::Next(next_expression) => {
                evaluated = evaluate_step(
                    &next_expression.expression,
                    storage,
                    read_variable,
                    read_literal,
                )
                .await;
            }
            EvaluatedStep::Last(result) => return result,
        }
    }
}
