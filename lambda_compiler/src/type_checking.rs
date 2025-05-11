use crate::{
    ast,
    compilation::{CompilerOutput, SourceLocation},
};
use astraea::{storage::StoreError, tree::Tree};
use lambda::{
    expressions::{DeepExpression, Expression},
    name::Name,
};
use std::{collections::BTreeMap, sync::Arc};

fn check_tree_construction_or_argument_list(
    arguments: &[ast::Expression],
    environment_builder: &mut EnvironmentBuilder,
) -> Result<CompilerOutput, StoreError> {
    let mut errors = Vec::new();
    let mut checked_arguments = Vec::new();
    for argument in arguments {
        let output = check_types(argument, environment_builder)?;
        errors.extend(output.errors);
        if let Some(checked) = output.entry_point {
            checked_arguments.push(Arc::new(checked));
        } else {
            return Ok(CompilerOutput::new(None, errors));
        }
    }
    Ok(CompilerOutput {
        entry_point: Some(lambda::expressions::DeepExpression(
            lambda::expressions::Expression::ConstructTree(checked_arguments),
        )),
        errors,
    })
}

pub struct LocalVariable {
    parameter_index: usize,
}

impl LocalVariable {
    pub fn new(parameter_index: usize) -> Self {
        Self { parameter_index }
    }
}

pub struct LambdaScope {
    names: BTreeMap<Name, LocalVariable>,
    captures: Vec<Arc<DeepExpression>>,
}

impl LambdaScope {
    pub fn new(parameter_names: &[Name]) -> Self {
        let mut names = BTreeMap::new();
        for (index, name) in parameter_names.iter().enumerate() {
            names.insert(name.clone(), LocalVariable::new(index));
        }
        Self {
            names,
            captures: Vec::new(),
        }
    }

    pub fn find_parameter_index(&self, parameter_name: &Name) -> Option<usize> {
        self.names
            .get(parameter_name)
            .map(|variable| variable.parameter_index)
    }

    pub fn capture(&mut self, expression: Arc<DeepExpression>) -> CompilerOutput {
        self.captures.push(expression);
        CompilerOutput::new(
            Some(lambda::expressions::DeepExpression(
                lambda::expressions::Expression::make_environment(),
                /*TODO: get nth element*/
            )),
            Vec::new(),
        )
    }

    pub fn leave(self) -> Vec<Arc<DeepExpression>> {
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

    pub fn enter_lambda_body(&mut self, parameter_names: &[Name]) {
        self.lambda_layers.push(LambdaScope::new(parameter_names));
    }

    pub fn leave_lambda_body(&mut self) -> Vec<Arc<DeepExpression>> {
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
            if let Some(_parameter_index) = last.find_parameter_index(identifier) {
                return CompilerOutput::new(
                    Some(lambda::expressions::DeepExpression(
                        lambda::expressions::Expression::make_argument(), /*TODO _parameter_index*/
                    )),
                    Vec::new(),
                );
            } else if layer_count > 1 {
                let result = Self::read_down(&mut layers[..layer_count - 1], identifier, location);
                if result.entry_point.is_some() {
                    return layers
                        .last_mut()
                        .unwrap()
                        .capture(Arc::new(result.entry_point.unwrap()));
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

pub fn check_lambda(
    parameter_names: &[Name],
    body: &ast::Expression,
    environment_builder: &mut EnvironmentBuilder,
) -> Result<CompilerOutput, StoreError> {
    environment_builder.enter_lambda_body(parameter_names);
    let body_result = check_types(body, environment_builder);
    // TODO: use RAII or something?
    let environment = environment_builder.leave_lambda_body();
    let body_output = body_result?;
    match body_output.entry_point {
        Some(body_checked) => Ok(CompilerOutput {
            entry_point: Some(lambda::expressions::DeepExpression(
                lambda::expressions::Expression::Lambda {
                    environment: Arc::new(DeepExpression(Expression::make_construct_tree(
                        environment,
                    ))),
                    body: Arc::new(body_checked),
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
            Some(lambda::expressions::DeepExpression(
                lambda::expressions::Expression::Literal(Tree::from_string(value).unwrap(/*TODO*/)),
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
                (Some(callee_checked), Some(argument_checked)) => Ok(CompilerOutput {
                    entry_point: Some(lambda::expressions::DeepExpression(
                        lambda::expressions::Expression::Apply {
                            callee: Arc::new(callee_checked),
                            argument: Arc::new(argument_checked),
                        },
                    )),
                    errors,
                }),
                (None, _) | (_, None) => Ok(CompilerOutput::new(None, errors)),
            }
        }
        ast::Expression::Lambda {
            parameter_names,
            body,
        } => check_lambda(&parameter_names[..], body, environment_builder),
        ast::Expression::ConstructTree(arguments) => {
            check_tree_construction_or_argument_list(&arguments[..], environment_builder)
        }
        ast::Expression::Braces(expression) => {
            let output = check_types(expression, environment_builder)?;
            Ok(output)
        }
    }
}
