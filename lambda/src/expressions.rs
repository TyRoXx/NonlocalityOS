use crate::types::Name;
use astraea::tree::{BlobDigest, HashedValue, Value};
use astraea::{
    storage::{LoadValue, StoreError, StoreValue},
    tree::ValueBlob,
};
use std::fmt::Display;
use std::{
    collections::{BTreeMap, BTreeSet},
    pin::Pin,
    sync::Arc,
};

#[derive(Debug, PartialEq, Eq, Ord, PartialOrd, Hash, Clone)]
pub enum Expression<E, V>
where
    E: Clone + Display,
    V: Clone + Display,
{
    Unit,
    Literal(V),
    Apply { callee: E, argument: E },
    ReadVariable(Name),
    Lambda { parameter_name: Name, body: E },
    Construct(Vec<E>),
}

impl<E, V> Expression<E, V>
where
    E: Clone + Display,
    V: Clone + Display,
{
    pub fn print(&self, writer: &mut dyn std::fmt::Write, level: usize) -> std::fmt::Result {
        match self {
            Expression::Unit => write!(writer, "()"),
            Expression::Literal(literal_value) => {
                write!(writer, "literal({})", literal_value)
            }
            Expression::Apply { callee, argument } => {
                write!(writer, "{}({})", callee, argument)
            }
            Expression::ReadVariable(name) => {
                write!(writer, "{}", &name.key)
            }
            Expression::Lambda {
                parameter_name,
                body,
            } => {
                write!(writer, "({}) =>\n", parameter_name)?;
                let indented = level + 1;
                for _ in 0..(indented * 2) {
                    write!(writer, " ")?;
                }
                write!(writer, "{}", body)
            }
            Expression::Construct(arguments) => {
                write!(writer, "construct(")?;
                for argument in arguments {
                    write!(writer, "{}, ", argument)?;
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

    pub fn make_unit() -> Self {
        Expression::Unit
    }

    pub fn make_literal(value: V) -> Self {
        Expression::Literal(value)
    }

    pub fn make_apply(callee: E, argument: E) -> Self {
        Expression::Apply { callee, argument }
    }

    pub fn make_lambda(parameter_name: Name, body: E) -> Self {
        Expression::Lambda {
            parameter_name,
            body,
        }
    }

    pub fn make_construct(arguments: Vec<E>) -> Self {
        Expression::Construct(arguments)
    }

    pub async fn deserialize(
        _value: &Value,
        _load_value: &(dyn LoadValue + Sync),
    ) -> Option<Expression<E, V>> {
        todo!()
    }

    pub async fn serialize(
        &self,
        _storage: &(dyn StoreValue + Sync),
    ) -> std::result::Result<HashedValue, StoreError> {
        todo!()
    }
}

impl<E, V> Display for Expression<E, V>
where
    E: Clone + Display,
    V: Clone + Display,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.print(f, 0)
    }
}

#[derive(Debug, PartialEq, Eq, Ord, PartialOrd, Hash, Clone)]
pub struct DeepExpression(pub Expression<Arc<DeepExpression>, HashedValue>);

impl Display for DeepExpression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

pub type ShallowExpression = Expression<BlobDigest, BlobDigest>;

async fn call_method(
    parameter_name: &Name,
    captured_variables: &BTreeMap<Name, Pointer>,
    body: &DeepExpression,
    argument: &Pointer,
    load_value: &(dyn LoadValue + Sync),
    store_value: &(dyn StoreValue + Sync),
    read_variable: &Arc<ReadVariable>,
) -> std::result::Result<Pointer, StoreError> {
    let read_variable_in_body: Arc<ReadVariable> = Arc::new({
        let parameter_name = parameter_name.clone();
        let argument = argument.clone();
        let captured_variables = captured_variables.clone();
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
    Box::pin(evaluate(
        &body,
        load_value,
        store_value,
        &read_variable_in_body,
    ))
    .await
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
    Reference(BlobDigest),
    InMemoryValue(InMemoryValue),
}

impl Pointer {
    pub fn serialize(self) -> HashedValue {
        match self {
            Pointer::Value(hashed_value) => hashed_value,
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
            Pointer::Reference(_blob_digest) => todo!(),
            Pointer::InMemoryValue(_in_memory_value) => {
                todo!()
            }
        }
    }
}

pub type ReadVariable =
    dyn Fn(&Name) -> Pin<Box<dyn core::future::Future<Output = Pointer> + Send>> + Send + Sync;

fn find_captured_names(expression: &DeepExpression) -> BTreeSet<Name> {
    match &expression.0 {
        Expression::Unit => BTreeSet::new(),
        Expression::Literal(_blob_digest) => BTreeSet::new(),
        Expression::Apply { callee, argument } => {
            let mut result = find_captured_names(callee);
            result.append(&mut find_captured_names(argument));
            result
        }
        Expression::ReadVariable(name) => BTreeSet::from([name.clone()]),
        Expression::Lambda {
            parameter_name,
            body,
        } => {
            let mut result = find_captured_names(body);
            result.remove(&parameter_name);
            result
        }
        Expression::Construct(arguments) => {
            let mut result = BTreeSet::new();
            for argument in arguments {
                result.append(&mut find_captured_names(argument));
            }
            result
        }
    }
}

pub async fn evaluate(
    expression: &DeepExpression,
    load_value: &(dyn LoadValue + Sync),
    store_value: &(dyn StoreValue + Sync),
    read_variable: &Arc<ReadVariable>,
) -> std::result::Result<Pointer, StoreError> {
    match &expression.0 {
        Expression::Unit => return Ok(Pointer::Value(HashedValue::from(Arc::new(Value::empty())))),
        Expression::Literal(literal_value) => Ok(Pointer::Value(literal_value.clone())),
        Expression::Apply { callee, argument } => {
            let evaluated_callee =
                Box::pin(evaluate(callee, load_value, store_value, read_variable)).await?;
            let evaluated_argument =
                Box::pin(evaluate(argument, load_value, store_value, read_variable)).await?;
            call_method(
                parameter_name,
                captured_variables,
                body,
                &evaluated_argument,
                load_value,
                store_value,
                read_variable,
            )
            .await
        }
        Expression::ReadVariable(name) => Ok(read_variable(&name).await),
        Expression::Lambda {
            parameter_name,
            body,
        } => {
            let mut captured_variables = BTreeMap::new();
            for captured_name in find_captured_names(body).into_iter() {
                let captured_value = read_variable(&captured_name).await;
                captured_variables.insert(captured_name, captured_value);
            }
            todo!()
        }
        Expression::Construct(arguments) => {
            let mut evaluated_arguments = Vec::new();
            for argument in arguments {
                let evaluated_argument =
                    Box::pin(evaluate(argument, load_value, store_value, read_variable)).await?;
                evaluated_arguments.push(*evaluated_argument.serialize().digest());
            }
            Ok(Pointer::Value(HashedValue::from(Arc::new(Value::new(
                ValueBlob::empty(),
                evaluated_arguments,
            )))))
        }
    }
}
