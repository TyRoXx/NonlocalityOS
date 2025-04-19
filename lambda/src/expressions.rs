use crate::types::Name;
use astraea::tree::{BlobDigest, HashedValue, ReferenceIndex, Value, ValueDeserializationError};
use astraea::{
    storage::{LoadValue, StoreError, StoreValue},
    tree::ValueBlob,
};
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::future::Future;
use std::hash::Hash;
use std::{
    collections::{BTreeMap, BTreeSet},
    pin::Pin,
    sync::Arc,
};

#[derive(Debug, PartialEq, Eq, Ord, PartialOrd, Hash, Clone, Serialize, Deserialize)]
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

    pub async fn map_child_expressions<
        't,
        Expr: Clone + Display,
        V2: Clone + Display,
        Error,
        F,
        G,
    >(
        &self,
        transform_expression: &'t F,
        transform_value: &'t G,
    ) -> Result<Expression<Expr, V2>, Error>
    where
        F: Fn(&E) -> Pin<Box<dyn Future<Output = Result<Expr, Error>> + 't>>,
        G: Fn(&V) -> Pin<Box<dyn Future<Output = Result<V2, Error>> + 't>>,
    {
        match self {
            Expression::Unit => Ok(Expression::Unit),
            Expression::Literal(value) => Ok(Expression::Literal(transform_value(value).await?)),
            Expression::Apply { callee, argument } => Ok(Expression::Apply {
                callee: transform_expression(callee).await?,
                argument: transform_expression(argument).await?,
            }),
            Expression::ReadVariable(name) => Ok(Expression::ReadVariable(name.clone())),
            Expression::Lambda {
                parameter_name,
                body,
            } => Ok(Expression::Lambda {
                parameter_name: parameter_name.clone(),
                body: transform_expression(body).await?,
            }),
            Expression::Construct(items) => {
                let mut transformed_items = Vec::new();
                for item in items.iter() {
                    transformed_items.push(transform_expression(item).await?);
                }
                Ok(Expression::Construct(transformed_items))
            }
        }
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
pub struct DeepExpression(pub Expression<Arc<DeepExpression>, BlobDigest>);

impl Display for DeepExpression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

pub type ShallowExpression = Expression<BlobDigest, BlobDigest>;

pub type ReferenceExpression = Expression<ReferenceIndex, ReferenceIndex>;

pub fn to_reference_expression(
    expression: &ShallowExpression,
) -> (ReferenceExpression, Vec<BlobDigest>) {
    match expression {
        Expression::Unit => (ReferenceExpression::Unit, vec![]),
        Expression::Literal(value) => (
            ReferenceExpression::Literal(ReferenceIndex(0)),
            vec![*value],
        ),
        Expression::Apply { callee, argument } => (
            ReferenceExpression::Apply {
                callee: ReferenceIndex(0),
                argument: ReferenceIndex(1),
            },
            // TODO: deduplicate?
            vec![*callee, *argument],
        ),
        Expression::ReadVariable(name) => (ReferenceExpression::ReadVariable(name.clone()), vec![]),
        Expression::Lambda {
            parameter_name,
            body,
        } => (
            ReferenceExpression::Lambda {
                parameter_name: parameter_name.clone(),
                body: ReferenceIndex(0),
            },
            vec![*body],
        ),
        Expression::Construct(items) => (
            ReferenceExpression::Construct(
                (0..items.len())
                    .map(|index| ReferenceIndex(index as u64))
                    .collect(),
            ),
            // TODO: deduplicate?
            items.clone(),
        ),
    }
}

pub async fn deserialize_shallow(
    _value: &Value,
    _load_value: &(dyn LoadValue + Sync),
) -> Option<ShallowExpression> {
    todo!()
}

pub async fn deserialize_recursively(
    _root: &BlobDigest,
    _load_value: &(dyn LoadValue + Sync),
) -> Option<DeepExpression> {
    todo!()
}

pub fn expression_to_value(expression: &ShallowExpression) -> Value {
    let (reference_expression, references) = to_reference_expression(expression);
    let blob = postcard::to_allocvec(&reference_expression).unwrap(/*TODO*/);
    Value::new(
        ValueBlob::try_from(bytes::Bytes::from_owner(blob)).unwrap(/*TODO*/),
        references,
    )
}

pub async fn serialize_shallow(
    expression: &ShallowExpression,
    storage: &(dyn StoreValue + Sync),
) -> std::result::Result<BlobDigest, StoreError> {
    let value = expression_to_value(expression);
    storage
        .store_value(&HashedValue::from(Arc::new(value)))
        .await
}

pub async fn serialize_recursively(
    expression: &DeepExpression,
    storage: &(dyn StoreValue + Sync),
) -> std::result::Result<BlobDigest, StoreError> {
    let shallow_expression: ShallowExpression = expression
        .0
        .map_child_expressions(&|child: &Arc<DeepExpression>| -> Pin<
            Box<dyn Future<Output = Result<BlobDigest, StoreError>>>,
        > {
            let child = child.clone();
            Box::pin(async move {
                serialize_recursively(&child, storage)
                    .await
            })
        },&|child: &BlobDigest| -> Pin<
        Box<dyn Future<Output = Result<BlobDigest, StoreError>>>,
        > {
            let child = child.clone();
            Box::pin(async move {
                Ok(child)
            })
        })
        .await?;
    serialize_shallow(&shallow_expression, storage).await
}

#[derive(Debug)]
pub struct Closure {
    parameter_name: Name,
    body: Arc<DeepExpression>,
    captured_variables: BTreeMap<Name, BlobDigest>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ClosureBlob {
    parameter_name: Name,
    captured_variables: BTreeMap<Name, ReferenceIndex>,
}

impl ClosureBlob {
    pub fn new(parameter_name: Name, captured_variables: BTreeMap<Name, ReferenceIndex>) -> Self {
        Self {
            parameter_name,
            captured_variables,
        }
    }
}

impl Closure {
    pub fn new(
        parameter_name: Name,
        body: Arc<DeepExpression>,
        captured_variables: BTreeMap<Name, BlobDigest>,
    ) -> Self {
        Self {
            parameter_name,
            body,
            captured_variables,
        }
    }

    pub async fn serialize(
        &self,
        store_value: &(dyn StoreValue + Sync),
    ) -> Result<BlobDigest, StoreError> {
        let mut references = vec![serialize_recursively(&self.body, store_value).await?];
        let mut captured_variables = BTreeMap::new();
        for (name, reference) in self.captured_variables.iter() {
            let index = ReferenceIndex(references.len() as u64);
            captured_variables.insert(name.clone(), index);
            references.push(reference.clone());
        }
        let closure_blob = ClosureBlob::new(self.parameter_name.clone(), captured_variables);
        store_value
            .store_value(&HashedValue::from(Arc::new(Value::new(
                ValueBlob::try_from(bytes::Bytes::from_owner(
                  postcard::to_allocvec(&closure_blob).unwrap(/*TODO*/))).unwrap(/*TODO*/),
                references,
            ))))
            .await
    }

    pub async fn deserialize(
        root: &BlobDigest,
        load_value: &(dyn LoadValue + Sync),
    ) -> Result<Closure, ValueDeserializationError> {
        let root_value = match load_value.load_value(root).await {
            Some(success) => success,
            None => return Err(ValueDeserializationError::BlobUnavailable(root.clone())),
        };
        let closure_blob: ClosureBlob = match root_value.value().to_object() {
            Ok(success) => success,
            Err(error) => return Err(error),
        };
        let body_reference = &root_value.value().references()[0];
        let body = deserialize_recursively(body_reference, load_value).await?;
        let mut captured_variables = BTreeMap::new();
        for (name, index) in closure_blob.captured_variables {
            let reference = &root_value.value().references()[index.0 as usize];
            captured_variables.insert(name, reference.clone());
        }
        Ok(Closure::new(
            closure_blob.parameter_name,
            Arc::new(body),
            captured_variables,
        ))
    }
}

async fn call_method(
    parameter_name: &Name,
    captured_variables: &BTreeMap<Name, BlobDigest>,
    body: &DeepExpression,
    argument: &BlobDigest,
    load_value: &(dyn LoadValue + Sync),
    store_value: &(dyn StoreValue + Sync),
    read_variable: &Arc<ReadVariable>,
) -> std::result::Result<BlobDigest, StoreError> {
    let read_variable_in_body: Arc<ReadVariable> = Arc::new({
        let parameter_name = parameter_name.clone();
        let argument = argument.clone();
        let captured_variables = captured_variables.clone();
        let read_variable = read_variable.clone();
        move |name: &Name| -> Pin<Box<dyn core::future::Future<Output = BlobDigest> + Send>> {
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
    dyn Fn(&Name) -> Pin<Box<dyn core::future::Future<Output = BlobDigest> + Send>> + Send + Sync;

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
) -> std::result::Result<BlobDigest, StoreError> {
    match &expression.0 {
        Expression::Unit => {
            return Ok(store_value
                .store_value(&HashedValue::from(Arc::new(Value::empty())))
                .await?)
        }
        Expression::Literal(literal_value) => Ok(literal_value.clone()),
        Expression::Apply { callee, argument } => {
            let evaluated_callee =
                Box::pin(evaluate(callee, load_value, store_value, read_variable)).await?;
            let evaluated_argument =
                Box::pin(evaluate(argument, load_value, store_value, read_variable)).await?;
            let closure = match Closure::deserialize(&evaluated_callee, load_value).await {
                Some(success) => success,
                None => todo!(),
            };
            call_method(
                &closure.parameter_name,
                &closure.captured_variables,
                &closure.body,
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
            let closure = Closure::new(parameter_name.clone(), body.clone(), captured_variables);
            let serialized = closure.serialize(store_value).await?;
            if Closure::deserialize(&serialized, load_value)
                .await
                .is_none()
            {
                panic!()
            }
            Ok(serialized)
        }
        Expression::Construct(arguments) => {
            let mut evaluated_arguments = Vec::new();
            for argument in arguments {
                let evaluated_argument =
                    Box::pin(evaluate(argument, load_value, store_value, read_variable)).await?;
                evaluated_arguments.push(evaluated_argument);
            }
            Ok(HashedValue::from(Arc::new(Value::new(
                ValueBlob::empty(),
                evaluated_arguments,
            )))
            .digest()
            .clone())
        }
    }
}
