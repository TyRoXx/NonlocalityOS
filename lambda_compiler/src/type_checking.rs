use crate::{ast, compilation::CompilerOutput};
use astraea::{
    storage::{StoreError, StoreTree},
    tree::{HashedTree, Tree},
};
use std::sync::Arc;

pub async fn check_types(
    syntax_tree: &ast::Expression,
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
            let callee_output = Box::pin(check_types(callee, storage)).await?;
            let argument_output = Box::pin(check_types(argument, storage)).await?;
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
            parameter_name,
            body,
        } => {
            let body_output = Box::pin(check_types(body, storage)).await?;
            match body_output.entry_point {
                Some(body_checked) => Ok(CompilerOutput {
                    entry_point: Some(lambda::expressions::DeepExpression(
                        lambda::expressions::Expression::Lambda {
                            parameter_name: parameter_name.clone(),
                            body: Arc::new(body_checked),
                        },
                    )),
                    errors: body_output.errors,
                }),
                None => return Ok(CompilerOutput::new(None, body_output.errors)),
            }
        }
    }
}
