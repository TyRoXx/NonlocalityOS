use crate::name::Name;
use astraea::tree::{BlobDigest, HashedTree, ReferenceIndex, Tree, TreeDeserializationError};
use astraea::{
    storage::{LoadValue, StoreError, StoreTree},
    tree::TreeBlob,
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

pub trait PrintExpression {
    fn print(&self, writer: &mut dyn std::fmt::Write, level: usize) -> std::fmt::Result;
}

#[derive(Debug, PartialEq, Eq, Ord, PartialOrd, Hash, Clone, Serialize, Deserialize)]
pub enum Expression<E, V>
where
    E: Clone + Display + PrintExpression,
    V: Clone + Display,
{
    Literal(V),
    Apply { callee: E, argument: E },
    ReadVariable(Name),
    Lambda { parameter_name: Name, body: E },
    Construct(Vec<E>),
}

impl<E, V> PrintExpression for Expression<E, V>
where
    E: Clone + Display + PrintExpression,
    V: Clone + Display,
{
    fn print(&self, writer: &mut dyn std::fmt::Write, level: usize) -> std::fmt::Result {
        match self {
            Expression::Literal(literal_value) => {
                write!(writer, "literal({})", literal_value)
            }
            Expression::Apply { callee, argument } => {
                callee.print(writer, level)?;
                write!(writer, "(")?;
                argument.print(writer, level)?;
                write!(writer, ")")
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
                body.print(writer, indented)
            }
            Expression::Construct(arguments) => {
                write!(writer, "construct(")?;
                for argument in arguments {
                    argument.print(writer, level)?;
                    write!(writer, ", ")?;
                }
                write!(writer, ")")
            }
        }
    }
}

impl<E, V> Expression<E, V>
where
    E: Clone + Display + PrintExpression,
    V: Clone + Display,
{
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

    pub fn make_read_variable(name: Name) -> Self {
        Expression::ReadVariable(name)
    }

    pub async fn map_child_expressions<
        't,
        Expr: Clone + Display + PrintExpression,
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
    E: Clone + Display + PrintExpression,
    V: Clone + Display,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.print(f, 0)
    }
}

#[derive(Debug, PartialEq, Eq, Ord, PartialOrd, Hash, Clone)]
pub struct DeepExpression(pub Expression<Arc<DeepExpression>, BlobDigest>);

impl PrintExpression for DeepExpression {
    fn print(&self, writer: &mut dyn std::fmt::Write, level: usize) -> std::fmt::Result {
        self.0.print(writer, level)
    }
}

impl PrintExpression for Arc<DeepExpression> {
    fn print(&self, writer: &mut dyn std::fmt::Write, level: usize) -> std::fmt::Result {
        self.0.print(writer, level)
    }
}

impl Display for DeepExpression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

pub type ShallowExpression = Expression<BlobDigest, BlobDigest>;

impl PrintExpression for BlobDigest {
    fn print(&self, writer: &mut dyn std::fmt::Write, _level: usize) -> std::fmt::Result {
        write!(writer, "{}", self)
    }
}

pub type ReferenceExpression = Expression<ReferenceIndex, ReferenceIndex>;

impl PrintExpression for ReferenceIndex {
    fn print(&self, writer: &mut dyn std::fmt::Write, _level: usize) -> std::fmt::Result {
        write!(writer, "{}", self)
    }
}

pub fn to_reference_expression(
    expression: &ShallowExpression,
) -> (ReferenceExpression, Vec<BlobDigest>) {
    match expression {
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

pub async fn deserialize_shallow(value: &Tree) -> Result<ShallowExpression, ()> {
    let reference_expression: ReferenceExpression = postcard::from_bytes(value.blob().as_slice())
        .unwrap(/*TODO*/);
    reference_expression
        .map_child_expressions(
            &|child: &ReferenceIndex| -> Pin<Box<dyn Future<Output = Result<BlobDigest, ()>>>> {
                let child = value.references()[child.0 as usize].clone();
                Box::pin(async move { Ok(child) })
            },
            &|child: &ReferenceIndex| -> Pin<Box<dyn Future<Output = Result<BlobDigest, ()>>>> {
                let child = value.references()[child.0 as usize].clone();
                Box::pin(async move { Ok(child) })
            },
        )
        .await
}

pub async fn deserialize_recursively(
    root: &BlobDigest,
    load_value: &(dyn LoadValue + Sync),
) -> Result<DeepExpression, ()> {
    let root_loaded = load_value.load_value(root).await.unwrap(/*TODO*/).hash().unwrap(/*TODO*/);
    let shallow = deserialize_shallow(&root_loaded.tree()).await?;
    let deep = shallow
        .map_child_expressions(
            &|child: &BlobDigest| -> Pin<Box<dyn Future<Output = Result<Arc<DeepExpression>, ()>>>> {
                let child = child.clone();
                Box::pin(async move { deserialize_recursively(&child, load_value)
                    .await
                    .map(|success| Arc::new(success)) })
            },
            &|child: &BlobDigest| -> Pin<Box<dyn Future<Output = Result<BlobDigest, ()>>>> {
                let child = child.clone();
                Box::pin(async move { Ok(child) })
            },
        )
        .await?;
    Ok(DeepExpression(deep))
}

pub fn expression_to_value(expression: &ShallowExpression) -> Tree {
    let (reference_expression, references) = to_reference_expression(expression);
    let blob = postcard::to_allocvec(&reference_expression).unwrap(/*TODO*/);
    Tree::new(
        TreeBlob::try_from(bytes::Bytes::from_owner(blob)).unwrap(/*TODO*/),
        references,
    )
}

pub async fn serialize_shallow(
    expression: &ShallowExpression,
    storage: &(dyn StoreTree + Sync),
) -> std::result::Result<BlobDigest, StoreError> {
    let value = expression_to_value(expression);
    storage
        .store_tree(&HashedTree::from(Arc::new(value)))
        .await
}

pub async fn serialize_recursively(
    expression: &DeepExpression,
    storage: &(dyn StoreTree + Sync),
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
        store_value: &(dyn StoreTree + Sync),
    ) -> Result<BlobDigest, StoreError> {
        let mut references = vec![serialize_recursively(&self.body, store_value).await?];
        let mut captured_variables = BTreeMap::new();
        for (name, reference) in self.captured_variables.iter() {
            let index = ReferenceIndex(references.len() as u64);
            captured_variables.insert(name.clone(), index);
            references.push(reference.clone());
        }
        let closure_blob = ClosureBlob::new(self.parameter_name.clone(), captured_variables);
        let closure_blob_bytes = postcard::to_allocvec(&closure_blob).unwrap(/*TODO*/);
        store_value
            .store_tree(&HashedTree::from(Arc::new(Tree::new(
                TreeBlob::try_from(bytes::Bytes::from_owner(closure_blob_bytes)).unwrap(/*TODO*/),
                references,
            ))))
            .await
    }

    pub async fn deserialize(
        root: &BlobDigest,
        load_value: &(dyn LoadValue + Sync),
    ) -> Result<Closure, TreeDeserializationError> {
        let loaded_root = match load_value.load_value(root).await {
            Some(success) => success,
            None => return Err(TreeDeserializationError::BlobUnavailable(root.clone())),
        };
        let root_tree = loaded_root.hash().unwrap(/*TODO*/).tree().clone();
        let closure_blob: ClosureBlob = match postcard::from_bytes(&root_tree.blob().as_slice()) {
            Ok(success) => success,
            Err(error) => return Err(TreeDeserializationError::Postcard(error)),
        };
        let body_reference = &root_tree.references()[0];
        let body = deserialize_recursively(body_reference, load_value).await.unwrap(/*TODO*/);
        let mut captured_variables = BTreeMap::new();
        for (name, index) in closure_blob.captured_variables {
            let reference = &root_tree.references()[index.0 as usize];
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
    store_value: &(dyn StoreTree + Sync),
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

pub type ReadVariable =
    dyn Fn(&Name) -> Pin<Box<dyn core::future::Future<Output = BlobDigest> + Send>> + Send + Sync;

fn find_captured_names(expression: &DeepExpression) -> BTreeSet<Name> {
    match &expression.0 {
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
    store_value: &(dyn StoreTree + Sync),
    read_variable: &Arc<ReadVariable>,
) -> std::result::Result<BlobDigest, StoreError> {
    match &expression.0 {
        Expression::Literal(literal_value) => Ok(literal_value.clone()),
        Expression::Apply { callee, argument } => {
            let evaluated_callee =
                Box::pin(evaluate(callee, load_value, store_value, read_variable)).await?;
            let evaluated_argument =
                Box::pin(evaluate(argument, load_value, store_value, read_variable)).await?;
            let closure = match Closure::deserialize(&evaluated_callee, load_value).await {
                Ok(success) => success,
                Err(_) => todo!(),
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
            Ok(serialized)
        }
        Expression::Construct(arguments) => {
            let mut evaluated_arguments = Vec::new();
            for argument in arguments {
                let evaluated_argument =
                    Box::pin(evaluate(argument, load_value, store_value, read_variable)).await?;
                evaluated_arguments.push(evaluated_argument);
            }
            Ok(
                HashedTree::from(Arc::new(Tree::new(TreeBlob::empty(), evaluated_arguments)))
                    .digest()
                    .clone(),
            )
        }
    }
}
