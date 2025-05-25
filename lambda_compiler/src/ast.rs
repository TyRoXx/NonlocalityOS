use crate::compilation::SourceLocation;
use lambda::name::Name;

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone)]
pub struct LambdaParameter {
    pub name: Name,
    pub source_location: SourceLocation,
    pub type_annotation: Option<Expression>,
}

impl LambdaParameter {
    pub fn new(
        name: Name,
        source_location: SourceLocation,
        type_annotation: Option<Expression>,
    ) -> Self {
        Self {
            name,
            source_location,
            type_annotation,
        }
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone)]
pub enum Expression {
    Identifier(Name, SourceLocation),
    StringLiteral(String),
    Apply {
        callee: Box<Expression>,
        arguments: Vec<Expression>,
    },
    Lambda {
        parameters: Vec<LambdaParameter>,
        body: Box<Expression>,
    },
    ConstructTree(Vec<Expression>),
    Braces(Box<Expression>),
}
