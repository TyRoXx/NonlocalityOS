use crate::{
    ast::{self, LambdaParameter},
    compilation::{CompilerOutput, SourceLocation},
};
use astraea::{
    deep_tree::DeepTree,
    storage::StoreError,
    tree::{ReferenceIndex, TreeBlob},
};
use lambda::{
    expressions::{evaluate, DeepExpression, Expression},
    name::Name,
};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, sync::Arc};

#[derive(Debug, PartialEq, Eq, Ord, PartialOrd, Hash, Clone, Serialize, Deserialize)]
pub enum GenericType<T>
where
    T: Clone,
{
    Any,
    String,
    TreeWithKnownChildTypes(Vec<T>),
    Function {
        parameters: Vec<T>,
        return_type: Box<T>,
    },
    Type,
}

#[derive(Debug, PartialEq, Eq, Ord, PartialOrd, Hash, Clone)]
pub struct DeepType(pub GenericType<DeepType>);

pub fn to_reference_type(deep_type: &DeepType) -> (GenericType<ReferenceIndex>, Vec<DeepType>) {
    match deep_type.0 {
        GenericType::Any => (GenericType::Any, Vec::new()),
        GenericType::String => (GenericType::String, Vec::new()),
        GenericType::TreeWithKnownChildTypes(ref children) => (
            GenericType::TreeWithKnownChildTypes(
                (0u64..(children.len() as u64))
                    .map(ReferenceIndex)
                    .collect(),
            ),
            children.clone(),
        ),
        GenericType::Function {
            ref parameters,
            ref return_type,
        } => {
            let mut parameters_references = Vec::new();
            let mut children = Vec::new();
            for (index, parameter) in parameters.iter().enumerate() {
                parameters_references.push(ReferenceIndex(index as u64));
                children.push(parameter.clone());
            }
            let return_type_reference = ReferenceIndex(children.len() as u64);
            children.push(return_type.as_ref().clone());
            (
                GenericType::Function {
                    parameters: parameters_references,
                    return_type: Box::new(return_type_reference),
                },
                children,
            )
        }
        GenericType::Type => (GenericType::Type, Vec::new()),
    }
}

pub fn type_to_deep_tree(deep_type: &DeepType) -> DeepTree {
    let (body, children) = to_reference_type(deep_type);
    let body_serialized = postcard::to_allocvec(&body).unwrap(/*TODO*/);
    DeepTree::new(
        TreeBlob::try_from(bytes::Bytes::from( body_serialized)).unwrap(/*TODO*/),
        children.iter().map(type_to_deep_tree).collect(),
    )
}

pub fn from_reference_type(body: &GenericType<ReferenceIndex>, children: &[DeepType]) -> DeepType {
    match body {
        GenericType::Any => DeepType(GenericType::Any),
        GenericType::String => DeepType(GenericType::String),
        GenericType::TreeWithKnownChildTypes(ref children_references) => {
            let mut resulting_children = Vec::new();
            for reference in children_references {
                let index = reference.0 as usize;
                if index < children.len() {
                    resulting_children.push(children[index].clone());
                } else {
                    // TODO error handling
                    // This should not happen if the tree is well-formed.
                    panic!("Reference index out of bounds: {index}");
                }
            }
            DeepType(GenericType::TreeWithKnownChildTypes(resulting_children))
        }
        GenericType::Function {
            ref parameters,
            ref return_type,
        } => {
            let mut resulting_parameters = Vec::new();
            for reference in parameters {
                let index: usize = reference.0.try_into().expect("TODO");
                if index < children.len() {
                    resulting_parameters.push(children[index].clone());
                } else {
                    // TODO error handling
                    // This should not happen if the tree is well-formed.
                    panic!("Reference index out of bounds: {index}");
                }
            }
            let resulting_return_type = {
                let index: usize = return_type.0.try_into().expect("TODO");
                if index < children.len() {
                    children[index].clone()
                } else {
                    // TODO error handling
                    panic!("Reference index out of bounds: {index}");
                }
            };
            DeepType(GenericType::Function {
                parameters: resulting_parameters,
                return_type: Box::new(resulting_return_type),
            })
        }
        GenericType::Type => DeepType(GenericType::Type),
    }
}

pub fn type_from_deep_tree(deep_tree: &DeepTree) -> DeepType {
    let body: GenericType<ReferenceIndex> =
        postcard::from_bytes(deep_tree.blob().as_slice()).unwrap(/*TODO*/);
    let children: Vec<_> = deep_tree
        .references()
        .iter()
        .map(type_from_deep_tree)
        .collect();
    from_reference_type(&body, &children)
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone)]
pub struct TypedExpression {
    pub expression: DeepExpression,
    pub type_: DeepType,
}

impl TypedExpression {
    pub fn new(expression: DeepExpression, type_: DeepType) -> Self {
        Self { expression, type_ }
    }
}

async fn check_tree_construction_or_argument_list(
    arguments: &[ast::Expression],
    environment_builder: &mut EnvironmentBuilder,
) -> Result<CompilerOutput, StoreError> {
    let mut errors = Vec::new();
    let mut checked_arguments = Vec::new();
    let mut argument_types = Vec::new();
    for argument in arguments {
        let output = check_types(argument, environment_builder).await?;
        errors.extend(output.errors);
        if let Some(checked) = output.entry_point {
            checked_arguments.push(Arc::new(checked.expression));
            argument_types.push(checked.type_);
        } else {
            return Ok(CompilerOutput::new(None, errors));
        }
    }
    Ok(CompilerOutput {
        entry_point: Some(TypedExpression::new(
            lambda::expressions::DeepExpression(lambda::expressions::Expression::ConstructTree(
                checked_arguments,
            )),
            DeepType(GenericType::TreeWithKnownChildTypes(argument_types)),
        )),
        errors,
    })
}

#[derive(Debug, Clone, Copy)]
enum ParameterIndex {
    SingleParameter,
    GetChild(u16),
}

impl ParameterIndex {
    pub fn create_deep_expression(&self) -> lambda::expressions::DeepExpression {
        match self {
            ParameterIndex::SingleParameter => {
                lambda::expressions::DeepExpression(lambda::expressions::Expression::make_argument())
            }
            ParameterIndex::GetChild(index) => lambda::expressions::DeepExpression(
                lambda::expressions::Expression::make_get_child(
                    Arc::new(lambda::expressions::DeepExpression(
                        lambda::expressions::Expression::make_argument(),
                    )),
                    *index,
                ),
            ),
        }
    }
}

struct LocalVariable {
    parameter_index: ParameterIndex,
    type_: DeepType,
    compile_time_value: Option<DeepTree>,
}

impl LocalVariable {
    pub fn new(
        parameter_index: ParameterIndex,
        type_: DeepType,
        compile_time_value: Option<DeepTree>,
    ) -> Self {
        Self {
            parameter_index,
            type_,
            compile_time_value,
        }
    }
}

struct LambdaScope {
    names: BTreeMap<Name, LocalVariable>,
    captures: BTreeMap<TypedExpression, u16>,
}

impl LambdaScope {
    pub fn new_lambda_scope(parameters: &[TypeCheckedLambdaParameter]) -> Self {
        let mut names = BTreeMap::new();
        if parameters.len() == 1 {
            names.insert(
                parameters[0].name.clone(),
                LocalVariable::new(
                    ParameterIndex::SingleParameter,
                    parameters[0].type_.clone(),
                    parameters[0].compile_time_value.clone(),
                ),
            );
        } else {
            for (index, parameter) in parameters.iter().enumerate() {
                let checked_index: u16 = index.try_into().expect("TODO handle too many parameters");
                names.insert(
                    parameter.name.clone(),
                    LocalVariable::new(
                        ParameterIndex::GetChild(checked_index),
                        parameter.type_.clone(),
                        parameter.compile_time_value.clone(),
                    ),
                );
            }
        }
        Self {
            names,
            captures: BTreeMap::new(),
        }
    }

    pub fn new_constant_scope(name: Name, type_: DeepType, compile_time_value: DeepTree) -> Self {
        let mut names = BTreeMap::new();
        names.insert(
            name,
            LocalVariable::new(
                ParameterIndex::SingleParameter,
                type_.clone(),
                Some(compile_time_value),
            ),
        );
        Self {
            names,
            captures: BTreeMap::new(),
        }
    }

    pub fn find_parameter_index(
        &self,
        parameter_name: &Name,
    ) -> Option<(ParameterIndex, DeepType, Option<DeepTree>)> {
        self.names.get(parameter_name).map(|variable| {
            (
                variable.parameter_index,
                variable.type_.clone(),
                variable.compile_time_value.clone(),
            )
        })
    }

    pub fn capture(&mut self, expression: TypedExpression) -> CompilerOutput {
        let type_ = expression.type_.clone();
        let index = match self.captures.get(&expression) {
            Some(&already_exists) => already_exists,
            None => {
                let new_index = self
                    .captures
                    .len()
                    .try_into()
                    .expect("TODO handle too many captures");
                self.captures.insert(expression, new_index);
                new_index
            }
        };
        CompilerOutput::new(
            Some(TypedExpression::new(
                lambda::expressions::DeepExpression(
                    lambda::expressions::Expression::make_get_child(
                        Arc::new(DeepExpression(
                            lambda::expressions::Expression::make_environment(),
                        )),
                        index,
                    ),
                ),
                type_,
            )),
            Vec::new(),
        )
    }

    pub fn leave(self) -> Vec<TypedExpression> {
        let mut as_vec: Vec<(TypedExpression, u16)> = self.captures.into_iter().collect();
        as_vec.sort_by_key(|(_, index)| *index);
        // sanity check:
        for (expected_index, (_, actual_index)) in as_vec.iter().enumerate() {
            assert_eq!(expected_index, *actual_index as usize);
        }
        as_vec
            .into_iter()
            .map(|(expression, _)| expression)
            .collect()
    }
}

pub struct EnvironmentBuilder {
    lambda_layers: Vec<LambdaScope>,
}

impl Default for EnvironmentBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl EnvironmentBuilder {
    pub fn new() -> Self {
        Self {
            lambda_layers: Vec::new(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.lambda_layers.is_empty()
    }

    pub fn enter_lambda_body(&mut self, parameters: &[TypeCheckedLambdaParameter]) {
        self.lambda_layers
            .push(LambdaScope::new_lambda_scope(parameters));
    }

    pub fn leave_lambda_body(&mut self) -> Vec<TypedExpression> {
        let top_scope = self.lambda_layers.pop().unwrap();
        top_scope.leave()
    }

    pub fn define_constant(&mut self, name: Name, type_: DeepType, compile_time_value: DeepTree) {
        self.lambda_layers.push(LambdaScope::new_constant_scope(
            name,
            type_,
            compile_time_value,
        ));
    }

    pub fn undefine_constant(&mut self) {
        let captures = self.leave_lambda_body();
        if !captures.is_empty() {
            todo!()
        }
    }

    pub fn read(&mut self, identifier: &Name, location: &SourceLocation) -> CompilerOutput {
        Self::read_down(&mut self.lambda_layers, identifier, location)
    }

    fn read_down(
        layers: &mut [LambdaScope],
        identifier: &Name,
        location: &SourceLocation,
    ) -> CompilerOutput {
        let layer_count = layers.len();
        if let Some(last) = layers.last_mut() {
            if let Some((parameter_index, parameter_type, compile_time_value)) =
                last.find_parameter_index(identifier)
            {
                return match compile_time_value {
                    Some(value) => CompilerOutput::new(
                        Some(TypedExpression::new(
                            DeepExpression(lambda::expressions::Expression::make_literal(value)),
                            parameter_type,
                        )),
                        Vec::new(),
                    ),
                    None => CompilerOutput::new(
                        Some(TypedExpression::new(
                            parameter_index.create_deep_expression(),
                            parameter_type,
                        )),
                        Vec::new(),
                    ),
                };
            } else if layer_count > 1 {
                let result = Self::read_down(&mut layers[..layer_count - 1], identifier, location);
                if result.entry_point.is_some() {
                    return layers
                        .last_mut()
                        .unwrap()
                        .capture(result.entry_point.unwrap());
                }
                return result;
            }
        }
        CompilerOutput::new(
            None,
            vec![crate::compilation::CompilerError::new(
                format!("Identifier {identifier} not found"),
                *location,
            )],
        )
    }
}

pub async fn evaluate_type_at_compile_time(expression: &DeepExpression) -> DeepType {
    let storage = astraea::storage::InMemoryTreeStorage::empty();
    let digest = evaluate(expression, &storage, &storage, &None, &None).await.unwrap(/*TODO*/);
    let deep_tree = DeepTree::deserialize(&digest, &storage).await.unwrap(/*TODO*/);
    type_from_deep_tree(&deep_tree)
}

pub struct TypeCheckedLambdaParameter {
    pub name: Name,
    pub source_location: SourceLocation,
    pub type_: DeepType,
    pub compile_time_value: Option<DeepTree>,
}

impl TypeCheckedLambdaParameter {
    pub fn new(
        name: Name,
        source_location: SourceLocation,
        type_: DeepType,
        compile_time_value: Option<DeepTree>,
    ) -> Self {
        Self {
            name,
            source_location,
            type_,
            compile_time_value,
        }
    }
}

pub async fn check_lambda_parameters(
    parameters: &[LambdaParameter],
    environment_builder: &mut EnvironmentBuilder,
) -> Result<Vec<TypeCheckedLambdaParameter>, StoreError> {
    let mut checked_parameters = Vec::new();
    for parameter in parameters {
        let parameter_type: DeepType = match &parameter.type_annotation {
            Some(type_annotation) => {
                let checked_type = check_types(type_annotation, environment_builder).await?;
                if let Some(checked) = checked_type.entry_point {
                    evaluate_type_at_compile_time(&checked.expression).await
                } else {
                    todo!()
                }
            }
            None => {
                // If no type annotation is provided, we assume the type is `Any`.
                DeepType(GenericType::Any)
            }
        };
        checked_parameters.push(TypeCheckedLambdaParameter {
            name: parameter.name.clone(),
            source_location: parameter.source_location,
            type_: parameter_type,
            compile_time_value: None, // TODO?
        });
    }
    Ok(checked_parameters)
}

pub async fn check_lambda(
    parameters: &[LambdaParameter],
    body: &ast::Expression,
    environment_builder: &mut EnvironmentBuilder,
) -> Result<CompilerOutput, StoreError> {
    let checked_parameters = check_lambda_parameters(parameters, environment_builder).await?;
    environment_builder.enter_lambda_body(&checked_parameters[..]);
    let body_result = check_types(body, environment_builder).await;
    // TODO: use RAII or something?
    let environment = environment_builder.leave_lambda_body();
    let environment_expressions = environment
        .into_iter()
        .map(|typed_expression| Arc::new(typed_expression.expression))
        .collect();
    let body_output = body_result?;
    match body_output.entry_point {
        Some(body_checked) => Ok(CompilerOutput {
            entry_point: Some(TypedExpression::new(
                lambda::expressions::DeepExpression(lambda::expressions::Expression::Lambda {
                    environment: Arc::new(DeepExpression(Expression::make_construct_tree(
                        environment_expressions,
                    ))),
                    body: Arc::new(body_checked.expression),
                }),
                DeepType(GenericType::Function {
                    parameters: checked_parameters
                        .into_iter()
                        .map(|parameter| parameter.type_)
                        .collect(),
                    return_type: Box::new(body_checked.type_),
                }),
            )),
            errors: body_output.errors,
        }),
        None => Ok(CompilerOutput::new(None, body_output.errors)),
    }
}

pub async fn check_braces(
    expression: &[ast::Expression],
    environment_builder: &mut EnvironmentBuilder,
) -> Result<CompilerOutput, StoreError> {
    if expression.len() != 1 {
        todo!()
    }
    check_types(&expression[0], environment_builder).await
}

pub async fn check_let(
    name: &Name,
    location: &SourceLocation,
    value: &ast::Expression,
    body: &ast::Expression,
    environment_builder: &mut EnvironmentBuilder,
) -> Result<CompilerOutput, StoreError> {
    let value_checked = check_types(value, environment_builder).await?;
    if !value_checked.errors.is_empty() {
        todo!()
    }
    let value_checked_unwrapped = value_checked.entry_point.unwrap();
    let checked_parameters = [TypeCheckedLambdaParameter::new(
        name.clone(),
        *location,
        value_checked_unwrapped.type_.clone(),
        // TODO: add const keyword or something
        None,
    )];
    environment_builder.enter_lambda_body(&checked_parameters[..]);
    let body_result = check_types(body, environment_builder).await;
    // TODO: use RAII or something?
    let environment = environment_builder.leave_lambda_body();
    let environment_expressions = environment
        .into_iter()
        .map(|typed_expression| Arc::new(typed_expression.expression))
        .collect();
    let body_output = body_result?;
    match body_output.entry_point {
        Some(body_checked) => Ok(CompilerOutput {
            entry_point: Some(TypedExpression::new(
                lambda::expressions::DeepExpression(lambda::expressions::Expression::make_apply(
                    Arc::new(lambda::expressions::DeepExpression(
                        lambda::expressions::Expression::Lambda {
                            environment: Arc::new(DeepExpression(Expression::make_construct_tree(
                                environment_expressions,
                            ))),
                            body: Arc::new(body_checked.expression),
                        },
                    )),
                    Arc::new(value_checked_unwrapped.expression),
                )),
                body_checked.type_,
            )),
            errors: body_output.errors,
        }),
        None => Ok(CompilerOutput::new(None, body_output.errors)),
    }
}

pub async fn check_types(
    syntax_tree: &ast::Expression,
    environment_builder: &mut EnvironmentBuilder,
) -> Result<CompilerOutput, StoreError> {
    Box::pin(async move {
        match syntax_tree {
            ast::Expression::Identifier(name, location) => {
                Ok(environment_builder.read(name, location))
            }
            ast::Expression::StringLiteral(value) => Ok(CompilerOutput::new(
                Some(TypedExpression::new(
                    lambda::expressions::DeepExpression(lambda::expressions::Expression::Literal(
                        DeepTree::try_from_string(value).unwrap(/*TODO*/),
                    )),
                    DeepType(GenericType::String),
                )),
                Vec::new(),
            )),
            ast::Expression::Apply { callee, arguments } => {
                let callee_output = check_types(callee, environment_builder).await?;
                let argument_output = if arguments.len() == 1 {
                    // For N=1 we don't need an indirection.
                    check_types(&arguments[0], environment_builder).await?
                } else {
                    check_tree_construction_or_argument_list(&arguments[..], environment_builder)
                        .await?
                };
                let errors = callee_output
                    .errors
                    .into_iter()
                    .chain(argument_output.errors)
                    .collect();
                match (callee_output.entry_point, argument_output.entry_point) {
                    (Some(callee_checked), Some(argument_checked)) => {
                        let return_type = match &callee_checked.type_.0 {
                            GenericType::Function { return_type, .. } => {
                                return_type.as_ref().clone()
                            }
                            _ => {
                                return Ok(CompilerOutput::new(
                                    None,
                                    vec![crate::compilation::CompilerError::new(
                                        "Callee is not a function".to_string(),
                                        callee.source_location(),
                                    )],
                                ))
                            }
                        };
                        // TODO: check argument types against callee parameter types
                        Ok(CompilerOutput {
                            entry_point: Some(TypedExpression::new(
                                lambda::expressions::DeepExpression(
                                    lambda::expressions::Expression::Apply {
                                        callee: Arc::new(callee_checked.expression),
                                        argument: Arc::new(argument_checked.expression),
                                    },
                                ),
                                return_type,
                            )),
                            errors,
                        })
                    }
                    (None, _) | (_, None) => Ok(CompilerOutput::new(None, errors)),
                }
            }
            ast::Expression::Lambda { parameters, body } => {
                check_lambda(&parameters[..], body, environment_builder).await
            }
            ast::Expression::ConstructTree(arguments) => {
                check_tree_construction_or_argument_list(&arguments[..], environment_builder).await
            }
            ast::Expression::Braces(expression) => {
                check_types(expression, environment_builder).await
            }
            ast::Expression::Let {
                name,
                location,
                value,
                body,
            } => check_let(name, location, value, body, environment_builder).await,
        }
    })
    .await
}
