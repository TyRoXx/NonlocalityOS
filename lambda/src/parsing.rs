use std::sync::Arc;

use crate::{
    compilation::{CompilerError, CompilerOutput, SourceLocation},
    tokenization::{Token, TokenContent},
};
use astraea::{
    expressions::{Application, Expression, LambdaExpression},
    tree::{BlobDigest, HashedValue, Value},
    types::{Name, NamespaceId, Type},
};

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
                TokenContent::Identifier(_) => return Some(token),
                TokenContent::Assign => return Some(token),
                TokenContent::Caret => return Some(token),
                TokenContent::LeftParenthesis => return Some(token),
                TokenContent::RightParenthesis => return Some(token),
                TokenContent::Dot => return Some(token),
                TokenContent::Quotes(_) => return Some(token),
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
            TokenContent::Caret => todo!(),
            TokenContent::LeftParenthesis => todo!(),
            TokenContent::RightParenthesis => {}
            TokenContent::Dot => todo!(),
            TokenContent::Quotes(_) => todo!(),
        },
        None => todo!(),
    }
}

fn expect_dot(tokens: &mut std::iter::Peekable<std::slice::Iter<'_, Token>>) {
    match pop_next_non_whitespace_token(tokens) {
        Some(non_whitespace) => match &non_whitespace.content {
            TokenContent::Whitespace => todo!(),
            TokenContent::Identifier(_) => todo!(),
            TokenContent::Assign => todo!(),
            TokenContent::Caret => todo!(),
            TokenContent::LeftParenthesis => todo!(),
            TokenContent::RightParenthesis => todo!(),
            TokenContent::Dot => {}
            TokenContent::Quotes(_) => todo!(),
        },
        None => todo!(),
    }
}

async fn parse_expression_start<'t>(
    tokens: &mut std::iter::Peekable<std::slice::Iter<'t, Token>>,
) -> Expression {
    match pop_next_non_whitespace_token(tokens) {
        Some(non_whitespace) => match &non_whitespace.content {
            TokenContent::Whitespace => todo!(),
            TokenContent::Identifier(identifier) => {
                Expression::ReadVariable(Name::new(
                    /*TODO: use local namespace*/ NamespaceId::builtins(),
                    identifier.clone(),
                ))
            }
            TokenContent::Assign => todo!(),
            TokenContent::Caret => Box::pin(parse_lambda(tokens)).await,
            TokenContent::LeftParenthesis => todo!(),
            TokenContent::RightParenthesis => todo!(),
            TokenContent::Dot => todo!(),
            TokenContent::Quotes(_) => todo!(),
        },
        None => todo!(),
    }
}

pub async fn parse_expression<'t>(
    tokens: &mut std::iter::Peekable<std::slice::Iter<'t, Token>>,
) -> Expression {
    let start = parse_expression_start(tokens).await;
    match peek_next_non_whitespace_token(tokens) {
        Some(more) => match &more.content {
            TokenContent::Whitespace => unreachable!(),
            TokenContent::Identifier(_) => start,
            TokenContent::Assign => start,
            TokenContent::Caret => start,
            TokenContent::LeftParenthesis => {
                tokens.next();
                let argument = Box::pin(parse_expression(tokens)).await;
                expect_right_parenthesis(tokens);
                return Expression::Apply(Box::new(Application::new(
                    start,
                    BlobDigest::hash(b"todo"),
                    Name::new(NamespaceId::builtins(), "apply".to_string()),
                    argument,
                )));
            }
            TokenContent::RightParenthesis => start,
            TokenContent::Dot => todo!(),
            TokenContent::Quotes(content) => {
                return Expression::Literal(
                    Type::Named(Name::new(
                        NamespaceId::builtins(),
                        "utf8-string".to_string(),
                    )),
                    HashedValue::from(Arc::new(
                        Value::from_string(&content).expect("It's too long. That's what she said."),
                    )),
                );
            }
        },
        None => start,
    }
}

async fn parse_lambda<'t>(
    tokens: &mut std::iter::Peekable<std::slice::Iter<'t, Token>>,
) -> Expression {
    let namespace = NamespaceId([0; 16]); // todo define son ding
    let parameter_name = Name::new(
        namespace,
        match pop_next_non_whitespace_token(tokens) {
            Some(non_whitespace) => match &non_whitespace.content {
                TokenContent::Whitespace => todo!(),
                TokenContent::Identifier(identifier) => identifier.clone(),
                TokenContent::Assign => todo!(),
                TokenContent::Caret => todo!(),
                TokenContent::LeftParenthesis => todo!(),
                TokenContent::RightParenthesis => todo!(),
                TokenContent::Dot => todo!(),
                TokenContent::Quotes(_) => todo!(),
            },
            None => todo!(),
        },
    );
    expect_dot(tokens);
    let body = parse_expression(tokens).await;
    return Expression::Lambda(Box::new(LambdaExpression::new(
        Type::Unit, // todo: do propper typechecking
        parameter_name,
        body,
    )));
}

pub async fn parse_entry_point_lambda<'t>(
    tokens: &mut std::iter::Peekable<std::slice::Iter<'t, Token>>,
) -> CompilerOutput {
    let mut errors = Vec::new();
    match pop_next_non_whitespace_token(tokens) {
        Some(non_whitespace) => match non_whitespace.content {
            TokenContent::Whitespace => unreachable!(),
            TokenContent::Identifier(_) => todo!(),
            TokenContent::Assign => todo!(),
            TokenContent::Caret => {
                let entry_point = parse_lambda(tokens).await;
                CompilerOutput::new(entry_point, errors)
            }
            TokenContent::LeftParenthesis => todo!(),
            TokenContent::RightParenthesis => todo!(),
            TokenContent::Dot => todo!(),
            TokenContent::Quotes(_) => todo!(),
        },
        None => {
            errors.push(CompilerError::new(
                "Expected entry point lambda".to_string(),
                SourceLocation::new(0, 0),
            ));
            CompilerOutput::new(Expression::Unit, errors)
        }
    }
}
