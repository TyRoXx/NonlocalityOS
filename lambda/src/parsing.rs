use crate::{
    compilation::{CompilerError, CompilerOutput, SourceLocation},
    tokenization::{Token, TokenContent},
};
use astraea::{
    expressions::{Expression, LambdaExpression},
    types::{Name, NamespaceId, Type},
};

pub fn pop_next_non_whitespace_token<'t>(
    tokens: &'t mut std::slice::Iter<Token>,
) -> Option<&'t Token> {
    loop {
        let next = tokens.next();
        match next {
            Some(token) => match token.content {
                TokenContent::Whitespace => continue,
                TokenContent::Identifier(_) => return next,
                TokenContent::Assign => return next,
                TokenContent::Caret => return next,
                TokenContent::LeftParenthesis => return next,
                TokenContent::RightParenthesis => return next,
                TokenContent::Dot => return next,
            },
            None => return None,
        }
    }
}

fn expect_dot(tokens: &mut std::slice::Iter<Token>) {
    match pop_next_non_whitespace_token(tokens) {
        Some(non_whitespace) => match &non_whitespace.content {
            TokenContent::Whitespace => todo!(),
            TokenContent::Identifier(_) => todo!(),
            TokenContent::Assign => todo!(),
            TokenContent::Caret => todo!(),
            TokenContent::LeftParenthesis => todo!(),
            TokenContent::RightParenthesis => todo!(),
            TokenContent::Dot => {}
        },
        None => todo!(),
    }
}

async fn parse_expression<'t>(tokens: &mut std::slice::Iter<'t, Token>) -> Expression {
    match pop_next_non_whitespace_token(tokens) {
        Some(non_whitespace) => match &non_whitespace.content {
            TokenContent::Whitespace => todo!(),
            TokenContent::Identifier(identifier) => {
                Expression::ReadVariable(Name::new(NamespaceId([0; 16]), identifier.clone()))
            }
            TokenContent::Assign => todo!(),
            TokenContent::Caret => todo!(),
            TokenContent::LeftParenthesis => todo!(),
            TokenContent::RightParenthesis => todo!(),
            TokenContent::Dot => todo!(),
        },
        None => todo!(),
    }
}

async fn parse_lambda<'t>(tokens: &mut std::slice::Iter<'t, Token>) -> Expression {
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
    tokens: &mut std::slice::Iter<'t, Token>,
) -> CompilerOutput {
    let mut errors = Vec::new();
    match pop_next_non_whitespace_token(tokens) {
        Some(non_whitespace) => match non_whitespace.content {
            TokenContent::Whitespace => todo!(),
            TokenContent::Identifier(_) => todo!(),
            TokenContent::Assign => todo!(),
            TokenContent::Caret => {
                let entry_point = parse_lambda(tokens).await;
                CompilerOutput::new(entry_point, errors)
            }
            TokenContent::LeftParenthesis => todo!(),
            TokenContent::RightParenthesis => todo!(),
            TokenContent::Dot => todo!(),
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
