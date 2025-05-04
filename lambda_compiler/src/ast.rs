use lambda::name::Name;

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone)]
pub enum Expression {
    Identifier(Name),
    StringLiteral(String),
    Apply {
        callee: Box<Expression>,
        argument: Box<Expression>,
    },
    Lambda {
        parameter_name: Name,
        body: Box<Expression>,
    },
}

impl Expression {
    pub fn to_string(&self) -> String {
        format!("{:?}", self)
    }
}
