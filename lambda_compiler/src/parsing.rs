use crate::{
    ast,
    compilation::{CompilerError, SourceLocation},
    tokenization::{Token, TokenContent},
};
use lambda::name::{Name, NamespaceId};

#[derive(Debug)]
pub struct ParserError {
    pub message: String,
    pub location: SourceLocation,
}

impl ParserError {
    pub fn new(message: String, location: SourceLocation) -> Self {
        Self { message, location }
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

    token
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
                | TokenContent::LeftBracket
                | TokenContent::RightBracket
                | TokenContent::Dot
                | TokenContent::Quotes(_)
                | TokenContent::FatArrow
                | TokenContent::Comma
                | TokenContent::EndOfFile => return Some(token),
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
            TokenContent::LeftBracket => todo!(),
            TokenContent::RightBracket => todo!(),
            TokenContent::Dot => todo!(),
            TokenContent::Quotes(_) => todo!(),
            TokenContent::FatArrow => todo!(),
            TokenContent::Comma => todo!(),
            TokenContent::EndOfFile => todo!(),
        },
        None => todo!(),
    }
}

fn try_skip_right_parenthesis(
    tokens: &mut std::iter::Peekable<std::slice::Iter<'_, Token>>,
) -> bool {
    match peek_next_non_whitespace_token(tokens) {
        Some(non_whitespace) => match &non_whitespace.content {
            TokenContent::Whitespace => todo!(),
            TokenContent::Identifier(_) => false,
            TokenContent::Assign => todo!(),
            TokenContent::LeftParenthesis => todo!(),
            TokenContent::RightParenthesis => {
                pop_next_non_whitespace_token(tokens);
                true
            }
            TokenContent::LeftBracket => todo!(),
            TokenContent::RightBracket => todo!(),
            TokenContent::Dot => todo!(),
            TokenContent::Quotes(_) => todo!(),
            TokenContent::FatArrow => todo!(),
            TokenContent::Comma => todo!(),
            TokenContent::EndOfFile => todo!(),
        },
        None => todo!(),
    }
}

fn expect_fat_arrow(tokens: &mut std::iter::Peekable<std::slice::Iter<'_, Token>>) {
    match pop_next_non_whitespace_token(tokens) {
        Some(non_whitespace) => match &non_whitespace.content {
            TokenContent::Whitespace => todo!(),
            TokenContent::Identifier(_identifier) => todo!(),
            TokenContent::Assign => todo!(),
            TokenContent::LeftParenthesis => todo!(),
            TokenContent::RightParenthesis => todo!(),
            TokenContent::LeftBracket => todo!(),
            TokenContent::RightBracket => todo!(),
            TokenContent::Dot => todo!(),
            TokenContent::Quotes(_) => todo!(),
            TokenContent::FatArrow => {}
            TokenContent::Comma => todo!(),
            TokenContent::EndOfFile => todo!(),
        },
        None => todo!(),
    }
}

fn expect_comma(tokens: &mut std::iter::Peekable<std::slice::Iter<'_, Token>>) {
    match pop_next_non_whitespace_token(tokens) {
        Some(non_whitespace) => match &non_whitespace.content {
            TokenContent::Whitespace => todo!(),
            TokenContent::Identifier(_) => todo!(),
            TokenContent::Assign => todo!(),
            TokenContent::LeftParenthesis => todo!(),
            TokenContent::RightParenthesis => todo!(),
            TokenContent::LeftBracket => todo!(),
            TokenContent::RightBracket => todo!(),
            TokenContent::Dot => todo!(),
            TokenContent::Quotes(_) => todo!(),
            TokenContent::FatArrow => todo!(),
            TokenContent::Comma => {}
            TokenContent::EndOfFile => todo!(),
        },
        None => todo!(),
    }
}

fn skip_right_bracket(tokens: &mut std::iter::Peekable<std::slice::Iter<'_, Token>>) -> bool {
    let maybe_right_bracket = peek_next_non_whitespace_token(tokens);
    match maybe_right_bracket {
        Some(token) => match &token.content {
            TokenContent::Whitespace => unreachable!(),
            TokenContent::Identifier(_) => false,
            TokenContent::Assign => false,
            TokenContent::LeftParenthesis => false,
            TokenContent::RightParenthesis => false,
            TokenContent::LeftBracket => false,
            TokenContent::RightBracket => {
                tokens.next();
                true
            }
            TokenContent::Dot => false,
            TokenContent::Quotes(_) => false,
            TokenContent::FatArrow => false,
            TokenContent::Comma => false,
            TokenContent::EndOfFile => todo!(),
        },
        None => false,
    }
}

fn parse_tree_construction(
    tokens: &mut std::iter::Peekable<std::slice::Iter<'_, Token>>,
    local_namespace: &NamespaceId,
) -> ParserResult<ast::Expression> {
    let mut elements = Vec::new();
    loop {
        if skip_right_bracket(tokens) {
            break;
        }
        if !elements.is_empty() {
            expect_comma(tokens);
        }
        if skip_right_bracket(tokens) {
            break;
        }
        let element = parse_expression(tokens, local_namespace)?;
        elements.push(element);
    }
    Ok(ast::Expression::ConstructTree(elements))
}

fn parse_expression_start<'t>(
    tokens: &mut std::iter::Peekable<std::slice::Iter<'t, Token>>,
    local_namespace: &NamespaceId,
) -> ParserResult<ast::Expression> {
    match peek_next_non_whitespace_token(tokens) {
        Some(non_whitespace) => match &non_whitespace.content {
            TokenContent::Whitespace => todo!(),
            TokenContent::Identifier(identifier) => {
                pop_next_non_whitespace_token(tokens);
                Ok(ast::Expression::Identifier(Name::new(
                    *local_namespace,
                    identifier.clone(),
                )))
            }
            TokenContent::Assign => todo!(),
            TokenContent::LeftParenthesis => {
                pop_next_non_whitespace_token(tokens);
                parse_lambda(tokens, local_namespace)
            }
            TokenContent::RightParenthesis => Err(ParserError::new(
                "Expected expression, found right parenthesis.".to_string(),
                non_whitespace.location,
            )),
            TokenContent::LeftBracket => {
                pop_next_non_whitespace_token(tokens);
                parse_tree_construction(tokens, local_namespace)
            }
            TokenContent::RightBracket => Err(ParserError::new(
                "Expected expression, found right bracket.".to_string(),
                non_whitespace.location,
            )),
            TokenContent::Dot => todo!(),
            TokenContent::Quotes(content) => {
                pop_next_non_whitespace_token(tokens);
                Ok(ast::Expression::StringLiteral(content.clone()))
            }
            TokenContent::FatArrow => todo!(),
            TokenContent::Comma => todo!(),
            TokenContent::EndOfFile => Err(ParserError::new(
                "Expected expression, got end of file.".to_string(),
                non_whitespace.location,
            )),
        },
        None => todo!(),
    }
}

pub fn parse_expression<'t>(
    tokens: &mut std::iter::Peekable<std::slice::Iter<'t, Token>>,
    local_namespace: &NamespaceId,
) -> ParserResult<ast::Expression> {
    let start = parse_expression_start(tokens, local_namespace)?;
    match peek_next_non_whitespace_token(tokens) {
        Some(more) => match &more.content {
            TokenContent::Whitespace => unreachable!(),
            TokenContent::Identifier(_) => Ok(start),
            TokenContent::Assign => Ok(start),
            TokenContent::LeftParenthesis => {
                tokens.next();
                let argument = parse_expression(tokens, local_namespace)?;
                expect_right_parenthesis(tokens);
                Ok(ast::Expression::Apply {
                    callee: Box::new(start),
                    argument: Box::new(argument),
                })
            }
            TokenContent::RightParenthesis => Ok(start),
            TokenContent::LeftBracket => todo!(),
            TokenContent::RightBracket => Ok(start),
            TokenContent::Dot => todo!(),
            TokenContent::Quotes(_) => todo!(),
            TokenContent::FatArrow => todo!(),
            TokenContent::Comma => Ok(start),
            TokenContent::EndOfFile => Ok(start),
        },
        None => todo!(),
    }
}

fn try_pop_identifier(
    tokens: &mut std::iter::Peekable<std::slice::Iter<'_, Token>>,
) -> Option<String> {
    match peek_next_non_whitespace_token(tokens) {
        Some(non_whitespace) => match &non_whitespace.content {
            TokenContent::Whitespace => todo!(),
            TokenContent::Identifier(identifier) => {
                pop_next_non_whitespace_token(tokens);
                Some(identifier.clone())
            }
            TokenContent::Assign => todo!(),
            TokenContent::LeftParenthesis => todo!(),
            TokenContent::RightParenthesis => None,
            TokenContent::LeftBracket => todo!(),
            TokenContent::RightBracket => todo!(),
            TokenContent::Dot => todo!(),
            TokenContent::Quotes(_) => todo!(),
            TokenContent::FatArrow => todo!(),
            TokenContent::Comma => None,
            TokenContent::EndOfFile => None,
        },
        None => None,
    }
}

fn try_skip_comma(tokens: &mut std::iter::Peekable<std::slice::Iter<'_, Token>>) -> bool {
    match peek_next_non_whitespace_token(tokens) {
        Some(non_whitespace) => match &non_whitespace.content {
            TokenContent::Whitespace => todo!(),
            TokenContent::Identifier(_identifier) => false,
            TokenContent::Assign => todo!(),
            TokenContent::LeftParenthesis => todo!(),
            TokenContent::RightParenthesis => false,
            TokenContent::LeftBracket => todo!(),
            TokenContent::RightBracket => todo!(),
            TokenContent::Dot => todo!(),
            TokenContent::Quotes(_) => todo!(),
            TokenContent::FatArrow => todo!(),
            TokenContent::Comma => {
                pop_next_non_whitespace_token(tokens);
                true
            }
            TokenContent::EndOfFile => false,
        },
        None => false,
    }
}

fn parse_lambda<'t>(
    tokens: &mut std::iter::Peekable<std::slice::Iter<'t, Token>>,
    local_namespace: &NamespaceId,
) -> ParserResult<ast::Expression> {
    let mut parameter_names = Vec::new();
    while let Some(name) = try_pop_identifier(tokens) {
        let parameter_name: Name = Name::new(*local_namespace, name);
        parameter_names.push(parameter_name);
        if !try_skip_comma(tokens) {
            break;
        }
    }
    if !try_skip_right_parenthesis(tokens) {
        let next_token = peek_next_non_whitespace_token(tokens).unwrap();
        return Err(ParserError::new(
            "Expected comma or right parenthesis in lambda parameter list.".to_string(),
            next_token.location,
        ));
    }
    expect_fat_arrow(tokens);
    let body = parse_expression(tokens, local_namespace)?;
    Ok(ast::Expression::Lambda {
        parameter_names,
        body: Box::new(body),
    })
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone)]
pub struct ParserOutput {
    pub entry_point: Option<ast::Expression>,
    pub errors: Vec<CompilerError>,
}

impl ParserOutput {
    pub fn new(entry_point: Option<ast::Expression>, errors: Vec<CompilerError>) -> ParserOutput {
        ParserOutput {
            entry_point,
            errors,
        }
    }
}

pub fn parse_expression_tolerantly<'t>(
    tokens: &mut std::iter::Peekable<std::slice::Iter<'t, Token>>,
    local_namespace: &NamespaceId,
) -> ParserOutput {
    let mut errors = Vec::new();
    let entry_point_result = parse_expression(tokens, local_namespace);
    match entry_point_result {
        Ok(entry_point) => ParserOutput::new(Some(entry_point), errors),
        Err(error) => {
            errors.push(CompilerError::new(
                format!("Parser error: {}", &error),
                error.location,
            ));
            ParserOutput::new(None, errors)
        }
    }
}
