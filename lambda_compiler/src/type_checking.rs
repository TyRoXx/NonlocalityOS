use crate::{ast, compilation::CompilerOutput};
use astraea::{
    storage::{StoreError, StoreTree},
    tree::{HashedTree, Tree},
};
use lambda::name::{Name, NamespaceId};
use std::sync::Arc;

pub fn combine_parameter_names(parameter_names: &[Name], namespace_id: &NamespaceId) -> Name {
    let mut combined = String::new();
    for name in parameter_names {
        if !combined.is_empty() {
            combined.push('_');
        }
        combined.push_str(&name.key);
    }
    Name::new(namespace_id.clone(), combined)
}

pub async fn check_types(
    syntax_tree: &ast::Expression,
    generated_name_namespace: &NamespaceId,
    storage: &dyn StoreTree,
) -> Result<CompilerOutput, StoreError> {
    match syntax_tree {
        ast::Expression::Identifier(name) => Ok(CompilerOutput::new(
            Some(lambda::expressions::DeepExpression(
                lambda::expressions::Expression::ReadVariable(name.clone()),
            )),
            Vec::new(),
        )),
        ast::Expression::StringLiteral(value) => Ok(CompilerOutput::new(
            Some(lambda::expressions::DeepExpression(
                lambda::expressions::Expression::Literal(
                    storage
                        .store_tree(&HashedTree::from(Arc::new(
                            Tree::from_string(value).unwrap(/*TODO*/),
                        )))
                        .await?,
                ),
            )),
            Vec::new(),
        )),
        ast::Expression::Apply { callee, argument } => {
            let callee_output =
                Box::pin(check_types(callee, generated_name_namespace, storage)).await?;
            let argument_output =
                Box::pin(check_types(argument, generated_name_namespace, storage)).await?;
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
                    errors: errors,
                }),
                (None, _) | (_, None) => return Ok(CompilerOutput::new(None, errors)),
            }
        }
        ast::Expression::Lambda {
            parameter_names,
            body,
        } => {
            let body_output =
                Box::pin(check_types(body, generated_name_namespace, storage)).await?;
            match body_output.entry_point {
                Some(body_checked) => Ok(CompilerOutput {
                    entry_point: Some(lambda::expressions::DeepExpression(
                        lambda::expressions::Expression::Lambda {
                            parameter_name: combine_parameter_names(
                                &parameter_names[..],
                                generated_name_namespace,
                            ),
                            body: Arc::new(body_checked),
                        },
                    )),
                    errors: body_output.errors,
                }),
                None => return Ok(CompilerOutput::new(None, body_output.errors)),
            }
        }
        ast::Expression::ConstructTree(expressions) => {
            let mut errors = Vec::new();
            let mut children = Vec::new();
            for expression in expressions {
                let output =
                    Box::pin(check_types(expression, generated_name_namespace, storage)).await?;
                errors.extend(output.errors);
                if let Some(checked) = output.entry_point {
                    children.push(Arc::new(checked));
                } else {
                    return Ok(CompilerOutput::new(None, errors));
                }
            }
            Ok(CompilerOutput {
                entry_point: Some(lambda::expressions::DeepExpression(
                    lambda::expressions::Expression::ConstructTree(children),
                )),
                errors: errors,
            })
        }
    }
}
