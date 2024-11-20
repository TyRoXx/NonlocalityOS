#[cfg(test)]
mod tests {
    use crate::{
        expressions::{Expression, LambdaExpression},
        tree::BlobDigest,
        type_checking::TypeCheckedExpression,
        types::{Interface, Name, NamespaceId, Signature, Type, TypedExpression},
    };
    use std::{pin::Pin, sync::Arc};

    fn no_named_variables(_name: &Name) -> Option<Type> {
        assert!(false);
        None
    }

    fn no_interfaces<'t>(
        _digest: &BlobDigest,
        _callee: Arc<Type>,
    ) -> Pin<Box<dyn core::future::Future<Output = Option<Arc<Interface>>> + Send + 't>> {
        assert!(false);
        Box::pin(core::future::ready(None))
    }

    #[test_log::test(tokio::test)]
    async fn no_conversion_possible() {
        assert_eq!(
            Err(crate::type_checking::TypeCheckingError {
                kind: crate::type_checking::TypeCheckingErrorKind::NoConversionPossible {
                    from: Type::Reference,
                    to: Type::Unit
                }
            }),
            TypeCheckedExpression::check(
                &TypedExpression::new(
                    Expression::Literal(Type::Reference, BlobDigest::hash(b"")),
                    Type::Unit
                ),
                &no_named_variables,
                &no_interfaces
            )
            .await
        );
        assert_eq!(
            Err(crate::type_checking::TypeCheckingError {
                kind: crate::type_checking::TypeCheckingErrorKind::NoConversionPossible {
                    from: Type::Unit,
                    to: Type::Reference
                }
            }),
            TypeCheckedExpression::check(
                &TypedExpression::new(Expression::Unit, Type::Reference),
                &no_named_variables,
                &no_interfaces
            )
            .await
        );
        let test_namespace = NamespaceId([0; 16]);
        assert_eq!(
            Err(crate::type_checking::TypeCheckingError {
                kind: crate::type_checking::TypeCheckingErrorKind::NoConversionPossible {
                    from: Type::Function(Box::new(Signature::new(Type::Unit, Type::Unit))),
                    to: Type::Unit
                }
            }),
            TypeCheckedExpression::check(
                &TypedExpression::new(
                    Expression::Lambda(Box::new(LambdaExpression::new(
                        Type::Unit,
                        Name::new(test_namespace, "a".to_string()),
                        Expression::Unit,
                    ))),
                    Type::Unit
                ),
                &no_named_variables,
                &no_interfaces
            )
            .await
        );
    }

    #[test_log::test(tokio::test)]
    async fn check_unit() {
        let unchecked = TypedExpression::unit();
        assert_eq!(
            Ok(&unchecked),
            TypeCheckedExpression::check(&unchecked, &no_named_variables, &no_interfaces)
                .await
                .as_ref()
                .map(|checked| checked.correct())
        );
    }

    #[test_log::test(tokio::test)]
    async fn check_reference() {
        let unchecked = TypedExpression::new(
            Expression::Literal(Type::Reference, BlobDigest::hash(b"")),
            Type::Reference,
        );
        assert_eq!(
            Ok(&unchecked),
            TypeCheckedExpression::check(&unchecked, &no_named_variables, &no_interfaces)
                .await
                .as_ref()
                .map(|checked| checked.correct())
        );
    }
}
