use astraea::tree::BlobDigest;

use crate::{
    expressions::{Expression, LambdaExpression},
    types::{Interface, Name, Signature, Type, TypedExpression},
};
use std::{pin::Pin, sync::Arc};

pub type FindInterface<'t, 'u> =
    dyn Fn(
        &'u BlobDigest,
        Arc<Type>,
    ) -> Pin<Box<dyn core::future::Future<Output = Option<Arc<Interface>>> + Send + 't>>;

async fn type_of_lambda_expression<'t, 'u, 'v, 'w>(
    unchecked: &'w LambdaExpression,
    find_variable: &dyn Fn(&Name) -> Option<Type>,
    find_interface: &'u FindInterface<'t, 'w>,
) -> std::result::Result<Type, TypeCheckingError>
where
    'w: 't,
    'w: 'v,
{
    let find_lambda_variable = {
        let parameter_type = unchecked.parameter_type.clone();
        let parameter_name = unchecked.parameter_name.clone();
        move |name: &Name| -> Option<Type> {
            if name == &parameter_name {
                let parameter_type = parameter_type.clone();
                Some(parameter_type)
            } else {
                find_variable(name)
            }
        }
    };
    let result_type =
        Box::pin(
            async move { type_of(&unchecked.body, &find_lambda_variable, find_interface).await },
        )
        .await?;
    Ok(Type::Function(Box::new(Signature::new(
        unchecked.parameter_type.clone(),
        result_type,
    ))))
}

pub async fn type_of<'t, 'u, 'v, 'w>(
    unchecked: &'w Expression,
    find_variable: &dyn Fn(&Name) -> Option<Type>,
    find_interface: &'u FindInterface<'t, 'w>,
) -> std::result::Result<Type, TypeCheckingError>
where
    'w: 't,
    'w: 'v,
{
    match &unchecked {
        Expression::Unit => Ok(Type::Unit),
        Expression::Literal(literal_type, _blob_digest) => {
            /*TODO: check if the value behind _blob_digest is valid for this type*/
            Ok(literal_type.clone())
        }
        Expression::Apply(application) => {
            let callee_type = Arc::new(
                Box::pin(type_of(&application.callee, find_variable, find_interface)).await?,
            );
            let argument_type = Box::pin(type_of(
                &application.argument,
                find_variable,
                find_interface,
            ))
            .await?;
            let interface =
                match find_interface(&application.callee_interface, callee_type.clone()).await {
                    Some(found) => found,
                    None => {
                        return Err(TypeCheckingError {
                            kind: TypeCheckingErrorKind::CouldNotFindInterface {
                                callee_interface: application.callee_interface,
                                callee_type: callee_type,
                            },
                        })
                    }
                };
            let signature = match interface.methods.get(&application.method) {
                Some(found) => found,
                None => todo!(),
            };
            if is_convertible(&argument_type, &signature.argument) {
                Ok(signature.result.clone())
            } else {
                Err(TypeCheckingError {
                    kind: TypeCheckingErrorKind::NoConversionPossible {
                        from: argument_type.clone(),
                        to: signature.argument.clone(),
                    },
                })
            }
        }
        Expression::ReadVariable(name) => match find_variable(name) {
            Some(found) => Ok(found),
            None => Err(TypeCheckingError {
                kind: TypeCheckingErrorKind::UnknownIdentifier(name.clone()),
            }),
        },
        Expression::Lambda(lambda_expression) => {
            type_of_lambda_expression(&lambda_expression, find_variable, find_interface).await
        }
    }
}

pub fn is_convertible(from: &Type, into: &Type) -> bool {
    // TODO
    from == into
}

#[derive(Debug, PartialEq)]
pub enum TypeCheckingErrorKind {
    NoConversionPossible {
        from: Type,
        to: Type,
    },
    UnknownIdentifier(Name),
    CouldNotFindInterface {
        callee_interface: BlobDigest,
        callee_type: Arc<Type>,
    },
}

#[derive(Debug, PartialEq)]
pub struct TypeCheckingError {
    pub kind: TypeCheckingErrorKind,
}

#[derive(Debug, PartialEq, PartialOrd, Hash, Clone)]
pub struct TypeCheckedExpression {
    correct: TypedExpression,
}

impl TypeCheckedExpression {
    pub async fn check<'t, 'u, 'v>(
        unchecked: &'v TypedExpression,
        find_variable: &dyn Fn(&Name) -> Option<Type>,
        find_interface: &'u FindInterface<'t, 'v>,
    ) -> std::result::Result<TypeCheckedExpression, TypeCheckingError>
    where
        'v: 't,
    {
        let actual_type = type_of(&unchecked.expression, find_variable, find_interface).await?;
        if is_convertible(&actual_type, &unchecked.type_) {
            Ok(TypeCheckedExpression {
                correct: unchecked.clone(),
            })
        } else {
            Err(TypeCheckingError {
                kind: TypeCheckingErrorKind::NoConversionPossible {
                    from: actual_type,
                    to: unchecked.type_.clone(),
                },
            })
        }
    }

    pub fn correct(&self) -> &TypedExpression {
        &self.correct
    }
}
