use crate::{
    ast::{self, LambdaParameter},
    compilation::{CompilerOutput, SourceLocation},
};
use astraea::{storage::StoreError, tree::Tree};
use lambda::{
    expressions::{DeepExpression, Expression},
    name::Name,
};
use std::{collections::BTreeMap, sync::Arc};

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone)]
pub enum Type {
    Any,
    String,
    TreeWithKnownChildTypes(Vec<Type>),
    Function {
        parameters: Vec<Type>,
        return_type: Box<Type>,
    },
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone)]
pub struct TypedExpression {
    pub expression: DeepExpression,
    pub type_: Type,
}

impl TypedExpression {
    pub fn new(expression: DeepExpression, type_: Type) -> Self {
        Self { expression, type_ }
    }
}

fn check_tree_construction_or_argument_list(
    arguments: &[ast::Expression],
    environment_builder: &mut EnvironmentBuilder,
) -> Result<CompilerOutput, StoreError> {
    let mut errors = Vec::new();
    let mut checked_arguments = Vec::new();
    let mut argument_types = Vec::new();
    for argument in arguments {
        let output = check_types(argument, environment_builder)?;
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
            Type::TreeWithKnownChildTypes(argument_types),
        )),
        errors,
    })
}

pub struct LocalVariable {
    parameter_index: u16,
    type_: Type,
}

impl LocalVariable {
    pub fn new(parameter_index: u16, type_: Type) -> Self {
        Self {
            parameter_index,
            type_,
        }
    }
}

pub struct LambdaScope {
    names: BTreeMap<Name, LocalVariable>,
    captures: Vec<TypedExpression>,
}

impl LambdaScope {
    pub fn new(parameters: &[TypeCheckedLambdaParameter]) -> Self {
        let mut names = BTreeMap::new();
        for (index, parameter) in parameters.iter().enumerate() {
            let checked_index: u16 = index.try_into().expect("TODO handle too many parameters");
            names.insert(
                parameter.name.clone(),
                LocalVariable::new(checked_index, parameter.type_.clone()),
            );
        }
        Self {
            names,
            captures: Vec::new(),
        }
    }

    pub fn find_parameter_index(&self, parameter_name: &Name) -> Option<(u16, Type)> {
        self.names
            .get(parameter_name)
            .map(|variable| (variable.parameter_index, variable.type_.clone()))
    }

    pub fn capture(&mut self, expression: TypedExpression) -> CompilerOutput {
        let index = self
            .captures
            .len()
            .try_into()
            .expect("TODO handle too many captures");
        self.captures.push(expression);
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
                self.captures.last().unwrap().type_.clone(),
            )),
            Vec::new(),
        )
    }

    pub fn leave(self) -> Vec<TypedExpression> {
        self.captures
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
        self.lambda_layers.push(LambdaScope::new(parameters));
    }

    pub fn leave_lambda_body(&mut self) -> Vec<TypedExpression> {
        let top_scope = self.lambda_layers.pop().unwrap();
        top_scope.leave()
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
            if let Some((parameter_index, parameter_type)) = last.find_parameter_index(identifier) {
                return CompilerOutput::new(
                    Some(TypedExpression::new(
                        lambda::expressions::DeepExpression(
                            lambda::expressions::Expression::make_get_child(
                                Arc::new(lambda::expressions::DeepExpression(
                                    lambda::expressions::Expression::make_argument(),
                                )),
                                parameter_index,
                            ),
                        ),
                        parameter_type,
                    )),
                    Vec::new(),
                );
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

pub fn evaluate_type_at_compile_time(_expression: &DeepExpression) -> Type {
    todo!()
}

pub struct TypeCheckedLambdaParameter {
    pub name: Name,
    pub source_location: SourceLocation,
    pub type_: Type,
}

pub fn check_lambda_parameters(
    parameters: &[LambdaParameter],
) -> Result<Vec<TypeCheckedLambdaParameter>, StoreError> {
    let mut checked_parameters = Vec::new();
    for parameter in parameters {
        let mut environment_builder = EnvironmentBuilder::new();
        let parameter_type: Type = match &parameter.type_annotation {
            Some(type_annotation) => {
                let checked_type = check_types(type_annotation, &mut environment_builder)?;
                assert!(environment_builder.is_empty());
                if let Some(checked) = checked_type.entry_point {
                    evaluate_type_at_compile_time(&checked.expression)
                } else {
                    todo!()
                }
            }
            None => {
                // If no type annotation is provided, we assume the type is `Any`.
                Type::Any
            }
        };
        checked_parameters.push(TypeCheckedLambdaParameter {
            name: parameter.name.clone(),
            source_location: parameter.source_location,
            type_: parameter_type,
        });
    }
    Ok(checked_parameters)
}

pub fn check_lambda(
    parameters: &[LambdaParameter],
    body: &ast::Expression,
    environment_builder: &mut EnvironmentBuilder,
) -> Result<CompilerOutput, StoreError> {
    let checked_parameters = check_lambda_parameters(parameters)?;
    environment_builder.enter_lambda_body(&checked_parameters[..]);
    let body_result = check_types(body, environment_builder);
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
                Type::Function {
                    parameters: checked_parameters
                        .into_iter()
                        .map(|parameter| parameter.type_)
                        .collect(),
                    return_type: Box::new(body_checked.type_),
                },
            )),
            errors: body_output.errors,
        }),
        None => Ok(CompilerOutput::new(None, body_output.errors)),
    }
}

pub fn check_types(
    syntax_tree: &ast::Expression,
    environment_builder: &mut EnvironmentBuilder,
) -> Result<CompilerOutput, StoreError> {
    match syntax_tree {
        ast::Expression::Identifier(name, location) => Ok(environment_builder.read(name, location)),
        ast::Expression::StringLiteral(value) => Ok(CompilerOutput::new(
            Some(TypedExpression::new(
                lambda::expressions::DeepExpression(lambda::expressions::Expression::Literal(
                    Tree::from_string(value).unwrap(/*TODO*/),
                )),
                Type::String,
            )),
            Vec::new(),
        )),
        ast::Expression::Apply { callee, arguments } => {
            let callee_output = check_types(callee, environment_builder)?;
            let argument_output = if arguments.len() == 1 {
                // For N=1 we don't need an indirection.
                check_types(&arguments[0], environment_builder)?
            } else {
                check_tree_construction_or_argument_list(&arguments[..], environment_builder)?
            };
            let errors = callee_output
                .errors
                .into_iter()
                .chain(argument_output.errors)
                .collect();
            match (callee_output.entry_point, argument_output.entry_point) {
                (Some(callee_checked), Some(argument_checked)) => {
                    let return_type = match &callee_checked.type_ {
                        Type::Function { return_type, .. } => return_type.as_ref().clone(),
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
            check_lambda(&parameters[..], body, environment_builder)
        }
        ast::Expression::ConstructTree(arguments) => {
            check_tree_construction_or_argument_list(&arguments[..], environment_builder)
        }
        ast::Expression::Braces(expression) => {
            let output = check_types(expression, environment_builder)?;
            Ok(output)
        }
    }
}
