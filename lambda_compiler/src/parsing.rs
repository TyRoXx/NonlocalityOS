use crate::{
    compilation::{CompilerError, CompilerOutput, SourceLocation},
    tokenization::{Token, TokenContent},
};
use astraea::tree::{HashedValue, Value};
use lambda::expressions::{DeepExpression, Expression};
use lambda::types::{Name, NamespaceId};
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
    local_namespace: &NamespaceId,
) -> ParserResult<DeepExpression> {
    match pop_next_non_whitespace_token(tokens) {
        Some(non_whitespace) => match &non_whitespace.content {
            TokenContent::Whitespace => todo!(),
            TokenContent::Identifier(identifier) => Ok(DeepExpression(Expression::ReadVariable(
                Name::new(*local_namespace, identifier.clone()),
            ))),
            TokenContent::Assign => todo!(),
            TokenContent::LeftParenthesis => Box::pin(parse_lambda(tokens, local_namespace)).await,
            TokenContent::RightParenthesis => todo!(),
            TokenContent::Dot => todo!(),
            TokenContent::Quotes(content) => Ok(DeepExpression(Expression::Literal(
                HashedValue::from(Arc::new(
                    Value::from_string(&content).expect("It's too long. That's what she said."),
                ))
                .digest()
                .clone(),
            ))),
            TokenContent::FatArrow => todo!(),
        },
        None => Err(ParserError::new(
            "Expected expression, got EOF.".to_string(),
        )),
    }
}

pub async fn parse_expression<'t>(
    tokens: &mut std::iter::Peekable<std::slice::Iter<'t, Token>>,
    local_namespace: &NamespaceId,
) -> ParserResult<DeepExpression> {
    let start = parse_expression_start(tokens, local_namespace).await?;
    match peek_next_non_whitespace_token(tokens) {
        Some(more) => match &more.content {
            TokenContent::Whitespace => unreachable!(),
            TokenContent::Identifier(_) => Ok(start),
            TokenContent::Assign => Ok(start),
            TokenContent::LeftParenthesis => {
                tokens.next();
                let argument = Box::pin(parse_expression(tokens, local_namespace)).await?;
                expect_right_parenthesis(tokens);
                Ok(DeepExpression(Expression::make_apply(
                    Arc::new(start),
                    Arc::new(argument),
                )))
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
    local_namespace: &NamespaceId,
) -> ParserResult<DeepExpression> {
    let parameter_name: Name = Name::new(
        *local_namespace,
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
    let body = parse_expression(tokens, local_namespace).await?;
    Ok(DeepExpression(Expression::make_lambda(
        parameter_name,
        Arc::new(body),
    )))
}

pub async fn parse_entry_point_lambda<'t>(
    tokens: &mut std::iter::Peekable<std::slice::Iter<'t, Token>>,
    local_namespace: &NamespaceId,
) -> CompilerOutput {
    let mut errors = Vec::new();
    let entry_point_result = parse_expression(tokens, local_namespace).await;
    match entry_point_result {
        Ok(entry_point) => match &entry_point.0 {
            Expression::Unit
            | Expression::Literal(_)
            | Expression::Apply {
                callee: _,
                argument: _,
            }
            | Expression::ReadVariable(_) => {
                errors.push(CompilerError::new(
                    "The entry point is expected to be a lambda expression.".to_string(),
                    SourceLocation::new(0, 0),
                ));
                CompilerOutput::new(DeepExpression(Expression::Unit), errors)
            }
            Expression::Lambda {
                parameter_name: _,
                body: _,
            } => CompilerOutput::new(entry_point, errors),
            Expression::Construct(_arguments) => {
                todo!()
            }
        },
        Err(error) => {
            errors.push(CompilerError::new(
                format!("Parser error: {}", &error),
                SourceLocation::new(0, 0),
            ));
            CompilerOutput::new(DeepExpression(Expression::Unit), errors)
        }
    }
}
