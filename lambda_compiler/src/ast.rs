use lambda::name::Name;

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone)]
pub enum Expression {
    Identifier(Name),
    StringLiteral(String),
    Apply {
        callee: Box<Expression>,
        arguments: Vec<Expression>,
    },
    Lambda {
        parameter_names: Vec<Name>,
        body: Box<Expression>,
    },
    ConstructTree(Vec<Expression>),
    Braces(Box<Expression>),
}
