use crate::{
    builtins::builtins_namespace,
    compilation::{CompilerError, CompilerOutput, SourceLocation},
    tokenization::{Token, TokenContent},
};
use astraea::{
    expressions::{Application, Expression, LambdaExpression},
    tree::{BlobDigest, HashedValue, Value},
    types::{Name, NamespaceId, Type},
};
use std::sync::Arc;

#[derive(Debug)]
pub struct ParserError {
    pub message: String,
}

impl ParserError {
    pub fn new(message: String) -> Self {
        Self { message }
    }
}

impl std::fmt::Display for ParserError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", &self.message)
    }
}

pub type ParserResult<T> = std::result::Result<T, ParserError>;

pub fn pop_next_non_whitespace_token<'t>(
    tokens: &mut std::iter::Peekable<std::slice::Iter<'t, Token>>,
) -> Option<&'t Token> {
    let token = peek_next_non_whitespace_token(tokens);
    if token.is_some() {
        tokens.next();
    }

    return token;
}

pub fn peek_next_non_whitespace_token<'t>(
    tokens: &mut std::iter::Peekable<std::slice::Iter<'t, Token>>,
) -> Option<&'t Token> {
    loop {
        let next = tokens.peek();
        match next {
            Some(token) => match token.content {
                TokenContent::Whitespace => {
                    tokens.next();
                    continue;
                }
                TokenContent::Identifier(_)
                | TokenContent::Assign
                | TokenContent::LeftParenthesis
                | TokenContent::RightParenthesis
                | TokenContent::Dot
                | TokenContent::Quotes(_)
                | TokenContent::FatArrow => return Some(token),
            },
            None => return None,
        }
    }
}

fn expect_right_parenthesis(tokens: &mut std::iter::Peekable<std::slice::Iter<'_, Token>>) {
    match pop_next_non_whitespace_token(tokens) {
        Some(non_whitespace) => match &non_whitespace.content {
            TokenContent::Whitespace => todo!(),
            TokenContent::Identifier(_) => todo!(),
            TokenContent::Assign => todo!(),
            TokenContent::LeftParenthesis => todo!(),
            TokenContent::RightParenthesis => {}
            TokenContent::Dot => todo!(),
            TokenContent::Quotes(_) => todo!(),
            TokenContent::FatArrow => todo!(),
        },
        None => todo!(),
    }
}

fn expect_fat_arrow(tokens: &mut std::iter::Peekable<std::slice::Iter<'_, Token>>) {
    match pop_next_non_whitespace_token(tokens) {
        Some(non_whitespace) => match &non_whitespace.content {
            TokenContent::Whitespace => todo!(),
            TokenContent::Identifier(_) => todo!(),
            TokenContent::Assign => todo!(),
            TokenContent::LeftParenthesis => todo!(),
            TokenContent::RightParenthesis => todo!(),
            TokenContent::Dot => todo!(),
            TokenContent::Quotes(_) => todo!(),
            TokenContent::FatArrow => {}
        },
        None => todo!(),
    }
}

async fn parse_expression_start<'t>(
    tokens: &mut std::iter::Peekable<std::slice::Iter<'t, Token>>,
) -> ParserResult<Expression> {
    match pop_next_non_whitespace_token(tokens) {
        Some(non_whitespace) => match &non_whitespace.content {
            TokenContent::Whitespace => todo!(),
            TokenContent::Identifier(identifier) => {
                Ok(Expression::ReadVariable(Name::new(
                    /*TODO: use local namespace*/ builtins_namespace(),
                    identifier.clone(),
                )))
            }
            TokenContent::Assign => todo!(),
            TokenContent::LeftParenthesis => Box::pin(parse_lambda(tokens)).await,
            TokenContent::RightParenthesis => todo!(),
            TokenContent::Dot => todo!(),
            TokenContent::Quotes(content) => Ok(Expression::Literal(
                Type::Named(Name::new(builtins_namespace(), "utf8-string".to_string())),
                HashedValue::from(Arc::new(
                    Value::from_string(&content).expect("It's too long. That's what she said."),
                )),
            )),
            TokenContent::FatArrow => todo!(),
        },
        None => Err(ParserError::new(
            "Expected expression, got EOF.".to_string(),
        )),
    }
}

pub async fn parse_expression<'t>(
    tokens: &mut std::iter::Peekable<std::slice::Iter<'t, Token>>,
) -> ParserResult<Expression> {
    let start = parse_expression_start(tokens).await?;
    match peek_next_non_whitespace_token(tokens) {
        Some(more) => match &more.content {
            TokenContent::Whitespace => unreachable!(),
            TokenContent::Identifier(_) => Ok(start),
            TokenContent::Assign => Ok(start),
            TokenContent::LeftParenthesis => {
                tokens.next();
                let argument = Box::pin(parse_expression(tokens)).await?;
                expect_right_parenthesis(tokens);
                Ok(Expression::Apply(Box::new(Application::new(
                    start,
                    BlobDigest::hash(b"todo"),
                    Name::new(builtins_namespace(), "apply".to_string()),
                    argument,
                ))))
            }
            TokenContent::RightParenthesis => Ok(start),
            TokenContent::Dot => todo!(),
            TokenContent::Quotes(_) => todo!(),
            TokenContent::FatArrow => todo!(),
        },
        None => Ok(start),
    }
}

async fn parse_lambda<'t>(
    tokens: &mut std::iter::Peekable<std::slice::Iter<'t, Token>>,
) -> ParserResult<Expression> {
    let namespace = NamespaceId([0; 16]); // todo define son ding
    let parameter_name = Name::new(
        namespace,
        match pop_next_non_whitespace_token(tokens) {
            Some(non_whitespace) => match &non_whitespace.content {
                TokenContent::Whitespace => todo!(),
                TokenContent::Identifier(identifier) => identifier.clone(),
                TokenContent::Assign => todo!(),
                TokenContent::LeftParenthesis => todo!(),
                TokenContent::RightParenthesis => todo!(),
                TokenContent::Dot => todo!(),
                TokenContent::Quotes(_) => todo!(),
                TokenContent::FatArrow => todo!(),
            },
            None => todo!(),
        },
    );
    expect_right_parenthesis(tokens);
    expect_fat_arrow(tokens);
    let body = parse_expression(tokens).await?;
    Ok(Expression::Lambda(Box::new(LambdaExpression::new(
        Type::Unit, // todo: do propper typechecking
        parameter_name,
        body,
    ))))
}

pub async fn parse_entry_point_lambda<'t>(
    tokens: &mut std::iter::Peekable<std::slice::Iter<'t, Token>>,
) -> CompilerOutput {
    let mut errors = Vec::new();
    let entry_point_result = parse_expression(tokens).await;
    match entry_point_result {
        Ok(entry_point) => match &entry_point {
            Expression::Unit
            | Expression::Literal(_, _)
            | Expression::Apply(_)
            | Expression::ReadVariable(_) => {
                errors.push(CompilerError::new(
                    "The entry point is expected to be a lambda expression.".to_string(),
                    SourceLocation::new(0, 0),
                ));
                CompilerOutput::new(Expression::Unit, errors)
            }
            Expression::Lambda(_) => CompilerOutput::new(entry_point, errors),
        },
        Err(error) => {
            errors.push(CompilerError::new(
                format!("Parser error: {}", &error),
                SourceLocation::new(0, 0),
            ));
            CompilerOutput::new(Expression::Unit, errors)
        }
    }
}
