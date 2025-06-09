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
    StringLiteral(String, SourceLocation),
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
    Let {
        name: Name,
        location: SourceLocation,
        value: Box<Expression>,
        body: Box<Expression>,
    },
    TypeOf(Box<Expression>),
}

impl Expression {
    pub fn source_location(&self) -> SourceLocation {
        match self {
            Expression::Identifier(_, location) => *location,
            Expression::StringLiteral(_, location) => *location,
            Expression::Apply { callee, .. } => callee.source_location(),
            Expression::Lambda { body, .. } => body.source_location(),
            Expression::ConstructTree(_) => todo!(),
            Expression::Braces(expression) => expression.source_location(),
            Expression::Let {
                name: _,
                location,
                value: _,
                body: _,
            } => *location,
            Expression::TypeOf(expression) => expression.source_location(),
        }
    }
}
