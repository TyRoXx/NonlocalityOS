use crate::expressions::{Application, Expression};
use astraea::tree::BlobDigest;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use uuid::Uuid;

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Copy, Serialize, Deserialize)]
pub struct NamespaceId(pub [u8; 16]);

impl NamespaceId {
    pub fn random() -> Self {
        Self(Uuid::new_v4().into_bytes())
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Serialize, Deserialize)]
pub struct Name {
    pub namespace: NamespaceId,
    pub key: String,
}

impl Name {
    pub fn new(namespace: NamespaceId, key: String) -> Self {
        Self { namespace, key }
    }
}

#[derive(Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Clone, Serialize, Deserialize)]
pub struct Signature {
    pub argument: Type,
    pub result: Type,
}

impl Signature {
    pub fn new(argument: Type, result: Type) -> Self {
        Self { argument, result }
    }
}

#[derive(Debug, PartialEq, PartialOrd, Hash, Clone, Serialize, Deserialize)]
pub struct Interface {
    pub methods: BTreeMap<Name, Signature>,
}

impl Interface {
    pub fn new(methods: BTreeMap<Name, Signature>) -> Self {
        Self { methods }
    }
}

#[derive(Debug, PartialEq, PartialOrd, Hash, Clone)]
pub struct TypedExpression {
    pub expression: Expression,
    pub type_: Type,
}

impl TypedExpression {
    pub fn new(expression: Expression, type_: Type) -> Self {
        Self { expression, type_ }
    }

    pub fn unit() -> Self {
        Self::new(Expression::Unit, Type::Unit)
    }

    pub fn convert_into(self, type_: &Type) -> Expression {
        if &self.type_ == type_ {
            self.expression
        } else {
            todo!()
        }
    }

    pub fn apply(
        self,
        interface: &Interface,
        interface_reference: &BlobDigest,
        method: Name,
        argument: TypedExpression,
    ) -> Option<TypedExpression> {
        self.type_.apply(
            self.expression,
            interface,
            interface_reference,
            method,
            argument,
        )
    }
}

#[derive(Debug, PartialEq, Eq, Ord, PartialOrd, Hash, Clone, Serialize, Deserialize)]
pub enum Type {
    Named(Name),
    Unit,
    Option(BlobDigest),
    Function(Box<Signature>),
    Reference,
}

impl Type {
    pub fn apply(
        &self,
        callee: Expression,
        interface: &Interface,
        interface_reference: &BlobDigest,
        method: Name,
        argument: TypedExpression,
    ) -> Option<TypedExpression> {
        interface.methods.get(&method).map(|signature| {
            let converted_argument = argument.convert_into(&signature.argument);
            TypedExpression::new(
                Expression::Apply(Box::new(Application::new(
                    callee,
                    *interface_reference,
                    method,
                    converted_argument,
                ))),
                signature.result.clone(),
            )
        })
    }

    pub fn print(&self, writer: &mut dyn std::fmt::Write, level: usize) -> std::fmt::Result {
        match self {
            Type::Named(name) => write!(writer, "{}", &name.key),
            Type::Unit => write!(writer, "()"),
            Type::Option(blob_digest) => write!(writer, "Option<{}>", blob_digest),
            Type::Function(signature) => {
                signature.argument.print(writer, level)?;
                write!(writer, " -> ")?;
                signature.result.print(writer, level)
            }
            Type::Reference => write!(writer, "Reference"),
        }
    }
}
