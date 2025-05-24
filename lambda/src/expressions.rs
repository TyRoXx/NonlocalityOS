use crate::name::Name;
use astraea::tree::{BlobDigest, HashedTree, ReferenceIndex, Tree, TreeDeserializationError};
use astraea::{
    storage::{LoadTree, StoreError, StoreTree},
    tree::TreeBlob,
};
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::future::Future;
use std::hash::Hash;
use std::{pin::Pin, sync::Arc};

pub trait PrintExpression {
    fn print(&self, writer: &mut dyn std::fmt::Write, level: usize) -> std::fmt::Result;
}

#[derive(Debug, PartialEq, Eq, Ord, PartialOrd, Hash, Clone, Serialize, Deserialize)]
pub enum Expression<E, TreeLike>
where
    E: Clone + Display + PrintExpression,
    TreeLike: Clone + std::fmt::Debug,
{
    Literal(TreeLike),
    Apply { callee: E, argument: E },
    Argument,
    Environment,
    Lambda { environment: E, body: E },
    ConstructTree(Vec<E>),
    GetChild { parent: E, index: u16 },
}

impl<E, V> PrintExpression for Expression<E, V>
where
    E: Clone + Display + PrintExpression,
    V: Clone + std::fmt::Debug,
{
    fn print(&self, writer: &mut dyn std::fmt::Write, level: usize) -> std::fmt::Result {
        match self {
            Expression::Literal(literal_value) => {
                write!(writer, "literal({literal_value:?})")
            }
            Expression::Apply { callee, argument } => {
                callee.print(writer, level)?;
                write!(writer, "(")?;
                argument.print(writer, level)?;
                write!(writer, ")")
            }
            Expression::Argument => {
                write!(writer, "$arg")
            }
            Expression::Environment => {
                write!(writer, "$env")
            }
            Expression::Lambda { environment, body } => {
                write!(writer, "$env={{")?;
                let indented = level + 1;
                environment.print(writer, indented)?;
                writeln!(writer, "}}($arg) =>")?;
                for _ in 0..(indented * 2) {
                    write!(writer, " ")?;
                }
                body.print(writer, indented)
            }
            Expression::ConstructTree(arguments) => {
                write!(writer, "[")?;
                for argument in arguments {
                    argument.print(writer, level)?;
                    write!(writer, ", ")?;
                }
                write!(writer, "]")
            }
            Expression::GetChild { parent, index } => {
                parent.print(writer, level)?;
                write!(writer, ".{index}")
            }
        }
    }
}

impl<E, TreeLike> Expression<E, TreeLike>
where
    E: Clone + Display + PrintExpression,
    TreeLike: Clone + std::fmt::Debug,
{
    pub fn make_literal(value: TreeLike) -> Self {
        Expression::Literal(value)
    }

    pub fn make_apply(callee: E, argument: E) -> Self {
        Expression::Apply { callee, argument }
    }

    pub fn make_argument() -> Self {
        Expression::Argument
    }

    pub fn make_environment() -> Self {
        Expression::Environment
    }

    pub fn make_lambda(environment: E, body: E) -> Self {
        Expression::Lambda { environment, body }
    }

    pub fn make_construct_tree(arguments: Vec<E>) -> Self {
        Expression::ConstructTree(arguments)
    }

    pub fn make_get_child(parent: E, index: u16) -> Self {
        Expression::GetChild { parent, index }
    }

    pub async fn map_child_expressions<
        't,
        Expr: Clone + Display + PrintExpression,
        TreeLike2: Clone + std::fmt::Debug,
        Error,
        F,
        G,
    >(
        &self,
        transform_expression: &'t F,
        transform_tree: &'t G,
    ) -> Result<Expression<Expr, TreeLike2>, Error>
    where
        F: Fn(&E) -> Pin<Box<dyn Future<Output = Result<Expr, Error>> + 't>>,
        G: Fn(&TreeLike) -> Pin<Box<dyn Future<Output = Result<TreeLike2, Error>> + 't>>,
    {
        match self {
            Expression::Literal(value) => Ok(Expression::Literal(transform_tree(value).await?)),
            Expression::Apply { callee, argument } => Ok(Expression::Apply {
                callee: transform_expression(callee).await?,
                argument: transform_expression(argument).await?,
            }),
            Expression::Argument => Ok(Expression::Argument),
            Expression::Environment => Ok(Expression::Environment),
            Expression::Lambda { environment, body } => Ok(Expression::Lambda {
                environment: transform_expression(environment).await?,
                body: transform_expression(body).await?,
            }),
            Expression::ConstructTree(items) => {
                let mut transformed_items = Vec::new();
                for item in items.iter() {
                    transformed_items.push(transform_expression(item).await?);
                }
                Ok(Expression::ConstructTree(transformed_items))
            }
            Expression::GetChild { parent, index } => Ok(Expression::GetChild {
                parent: transform_expression(parent).await?,
                index: *index,
            }),
        }
    }
}

impl<E, V> Display for Expression<E, V>
where
    E: Clone + Display + PrintExpression,
    V: Clone + std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.print(f, 0)
    }
}

#[derive(Debug, PartialEq, Eq, Ord, PartialOrd, Clone)]
pub struct DeepExpression(pub Expression<Arc<DeepExpression>, Tree>);

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
        write!(writer, "{self}")
    }
}

pub type ReferenceExpression = Expression<ReferenceIndex, ReferenceIndex>;

impl PrintExpression for ReferenceIndex {
    fn print(&self, writer: &mut dyn std::fmt::Write, _level: usize) -> std::fmt::Result {
        write!(writer, "{self}")
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
        Expression::Argument => (ReferenceExpression::Argument, vec![]),
        Expression::Environment => (ReferenceExpression::Environment, vec![]),
        Expression::Lambda { environment, body } => (
            ReferenceExpression::Lambda {
                environment: ReferenceIndex(0),
                body: ReferenceIndex(1),
            },
            vec![*environment, *body],
        ),
        Expression::ConstructTree(items) => (
            ReferenceExpression::ConstructTree(
                (0..items.len())
                    .map(|index| ReferenceIndex(index as u64))
                    .collect(),
            ),
            // TODO: deduplicate?
            items.clone(),
        ),
        Expression::GetChild { parent, index } => (
            ReferenceExpression::GetChild {
                parent: ReferenceIndex(0),
                index: *index,
            },
            vec![*parent],
        ),
    }
}

pub async fn deserialize_shallow(tree: &Tree) -> Result<ShallowExpression, ()> {
    let reference_expression: ReferenceExpression = postcard::from_bytes(tree.blob().as_slice())
        .unwrap(/*TODO*/);
    reference_expression
        .map_child_expressions(
            &|child: &ReferenceIndex| -> Pin<Box<dyn Future<Output = Result<BlobDigest, ()>>>> {
                let child = tree.references()[child.0 as usize];
                Box::pin(async move { Ok(child) })
            },
            &|child: &ReferenceIndex| -> Pin<Box<dyn Future<Output = Result<BlobDigest, ()>>>> {
                let child = tree.references()[child.0 as usize];
                Box::pin(async move { Ok(child) })
            },
        )
        .await
}

pub async fn deserialize_recursively(
    root: &BlobDigest,
    load_tree: &(dyn LoadTree + Sync),
) -> Result<DeepExpression, ()> {
    let root_loaded = load_tree.load_tree(root).await.unwrap(/*TODO*/).hash().unwrap(/*TODO*/);
    let shallow = deserialize_shallow(root_loaded.tree()).await?;
    let deep = shallow
        .map_child_expressions(
            &|child: &BlobDigest| -> Pin<Box<dyn Future<Output = Result<Arc<DeepExpression>, ()>>>> {
                let child = *child;
                Box::pin(async move { deserialize_recursively(&child, load_tree)
                    .await
                    .map(Arc::new) })
            },
            &|child: &BlobDigest| -> Pin<Box<dyn Future<Output = Result<Tree, ()>>>> {
                let child = *child;
                Box::pin(async move { Ok((**load_tree.load_tree(&child).await
                    .map(|tree| tree.hash().unwrap(/*TODO*/) ).unwrap(/*TODO*/).tree()).clone())})
            },
        )
        .await?;
    Ok(DeepExpression(deep))
}

pub fn expression_to_tree(expression: &ShallowExpression) -> Tree {
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
    let tree = expression_to_tree(expression);
    storage.store_tree(&HashedTree::from(Arc::new(tree))).await
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
        },&|child: &Tree| -> Pin<
        Box<dyn Future<Output = Result<BlobDigest, StoreError>>>,
        > {
            let child = child.clone();
            Box::pin(async move {
                storage.store_tree(&HashedTree::from(Arc::new(child))).await
            })
        })
        .await?;
    serialize_shallow(&shallow_expression, storage).await
}

#[derive(Debug)]
pub struct Closure {
    environment: BlobDigest,
    body: Arc<DeepExpression>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ClosureBlob {}

impl Default for ClosureBlob {
    fn default() -> Self {
        Self::new()
    }
}

impl ClosureBlob {
    pub fn new() -> Self {
        Self {}
    }
}

impl Closure {
    pub fn new(environment: BlobDigest, body: Arc<DeepExpression>) -> Self {
        Self { environment, body }
    }

    pub async fn serialize(
        &self,
        store_tree: &(dyn StoreTree + Sync),
    ) -> Result<BlobDigest, StoreError> {
        let references = vec![
            self.environment,
            serialize_recursively(&self.body, store_tree).await?,
        ];
        let closure_blob = ClosureBlob::new();
        let closure_blob_bytes = postcard::to_allocvec(&closure_blob).unwrap(/*TODO*/);
        store_tree
            .store_tree(&HashedTree::from(Arc::new(Tree::new(
                TreeBlob::try_from(bytes::Bytes::from_owner(closure_blob_bytes)).unwrap(/*TODO*/),
                references,
            ))))
            .await
    }

    pub async fn deserialize(
        root: &BlobDigest,
        load_tree: &(dyn LoadTree + Sync),
    ) -> Result<Closure, TreeDeserializationError> {
        let loaded_root = match load_tree.load_tree(root).await {
            Some(success) => success,
            None => return Err(TreeDeserializationError::BlobUnavailable(*root)),
        };
        let root_tree = loaded_root.hash().unwrap(/*TODO*/).tree().clone();
        let _closure_blob: ClosureBlob = match postcard::from_bytes(root_tree.blob().as_slice()) {
            Ok(success) => success,
            Err(error) => return Err(TreeDeserializationError::Postcard(error)),
        };
        let environment_reference = &root_tree.references()[0];
        let body_reference = &root_tree.references()[1];
        let body = deserialize_recursively(body_reference, load_tree).await.unwrap(/*TODO*/);
        Ok(Closure::new(*environment_reference, Arc::new(body)))
    }
}

async fn call_method(
    body: &DeepExpression,
    argument: &BlobDigest,
    environment: &BlobDigest,
    load_tree: &(dyn LoadTree + Sync),
    store_tree: &(dyn StoreTree + Sync),
) -> std::result::Result<BlobDigest, StoreError> {
    Box::pin(evaluate(
        body,
        load_tree,
        store_tree,
        &Some(*argument),
        &Some(*environment),
    ))
    .await
}

pub type ReadVariable =
    dyn Fn(&Name) -> Pin<Box<dyn core::future::Future<Output = BlobDigest> + Send>> + Send + Sync;

pub async fn apply_evaluated_argument(
    callee: &DeepExpression,
    evaluated_argument: &BlobDigest,
    load_tree: &(dyn LoadTree + Sync),
    store_tree: &(dyn StoreTree + Sync),
    current_lambda_argument: &Option<BlobDigest>,
    current_lambda_environment: &Option<BlobDigest>,
) -> std::result::Result<BlobDigest, StoreError> {
    let evaluated_callee = Box::pin(evaluate(
        callee,
        load_tree,
        store_tree,
        current_lambda_argument,
        current_lambda_environment,
    ))
    .await?;
    let closure = match Closure::deserialize(&evaluated_callee, load_tree).await {
        Ok(success) => success,
        Err(_) => todo!(),
    };
    call_method(
        &closure.body,
        evaluated_argument,
        &closure.environment,
        load_tree,
        store_tree,
    )
    .await
}

pub async fn evaluate_apply(
    callee: &DeepExpression,
    argument: &DeepExpression,
    load_tree: &(dyn LoadTree + Sync),
    store_tree: &(dyn StoreTree + Sync),
    current_lambda_argument: &Option<BlobDigest>,
    current_lambda_environment: &Option<BlobDigest>,
) -> std::result::Result<BlobDigest, StoreError> {
    let evaluated_argument = Box::pin(evaluate(
        argument,
        load_tree,
        store_tree,
        current_lambda_argument,
        current_lambda_environment,
    ))
    .await?;
    apply_evaluated_argument(
        callee,
        &evaluated_argument,
        load_tree,
        store_tree,
        current_lambda_argument,
        current_lambda_environment,
    )
    .await
}

pub async fn evaluate(
    expression: &DeepExpression,
    load_tree: &(dyn LoadTree + Sync),
    store_tree: &(dyn StoreTree + Sync),
    current_lambda_argument: &Option<BlobDigest>,
    current_lambda_environment: &Option<BlobDigest>,
) -> std::result::Result<BlobDigest, StoreError> {
    match &expression.0 {
        Expression::Literal(literal_value) => {
            store_tree
                .store_tree(&HashedTree::from(Arc::new(literal_value.clone())))
                .await
        }
        Expression::Apply { callee, argument } => {
            evaluate_apply(
                callee,
                argument,
                load_tree,
                store_tree,
                current_lambda_argument,
                current_lambda_environment,
            )
            .await
        }
        Expression::Argument => {
            if let Some(argument) = current_lambda_argument {
                Ok(*argument)
            } else {
                todo!("We are not in a lambda context; argument is not available")
            }
        }
        Expression::Environment => {
            if let Some(environment) = current_lambda_environment {
                Ok(*environment)
            } else {
                todo!("We are not in a lambda context; environment is not available")
            }
        }
        Expression::Lambda { environment, body } => {
            let evaluated_environment = Box::pin(evaluate(
                environment,
                load_tree,
                store_tree,
                current_lambda_argument,
                current_lambda_environment,
            ))
            .await?;
            let closure = Closure::new(evaluated_environment, body.clone());
            let serialized = closure.serialize(store_tree).await?;
            Ok(serialized)
        }
        Expression::ConstructTree(arguments) => {
            let mut evaluated_arguments = Vec::new();
            for argument in arguments {
                let evaluated_argument = Box::pin(evaluate(
                    argument,
                    load_tree,
                    store_tree,
                    current_lambda_argument,
                    current_lambda_environment,
                ))
                .await?;
                evaluated_arguments.push(evaluated_argument);
            }
            store_tree
                .store_tree(&HashedTree::from(Arc::new(Tree::new(
                    TreeBlob::empty(),
                    evaluated_arguments,
                ))))
                .await
        }
        Expression::GetChild { parent, index } => {
            let evaluated_parent = Box::pin(evaluate(
                parent,
                load_tree,
                store_tree,
                current_lambda_argument,
                current_lambda_environment,
            ))
            .await?;
            let loaded_parent = load_tree.load_tree(&evaluated_parent).await.unwrap(/*TODO*/);
            let hashed_tree = loaded_parent
                .hash()
                .unwrap(/*TODO*/);
            let child = hashed_tree
                .tree()
                .references()
                .get(*index as usize)
                .expect("TODO handle out of range error");
            Ok(*child)
        }
    }
}
