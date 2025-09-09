use crate::{
    ast::{self, LambdaParameter},
    compilation::{CompilerError, SourceLocation},
    tokenization::{IntegerBase, Token, TokenContent},
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
                | TokenContent::LeftBrace
                | TokenContent::RightBrace
                | TokenContent::Dot
                | TokenContent::Colon
                | TokenContent::Quotes(_)
                | TokenContent::FatArrow
                | TokenContent::Comma
                | TokenContent::Comment(_)
                | TokenContent::Integer(_, _)
                | TokenContent::EndOfFile => return Some(token),
            },
            None => return None,
        }
    }
}

fn expect_right_brace(
    tokens: &mut std::iter::Peekable<std::slice::Iter<'_, Token>>,
) -> ParserResult<()> {
    match peek_next_non_whitespace_token(tokens) {
        Some(non_whitespace) => {
            match &non_whitespace.content {
                TokenContent::Comment(_) => todo!(),
                TokenContent::Whitespace => unreachable!(),
                TokenContent::Identifier(_) => {}
                TokenContent::Assign => {}
                TokenContent::LeftParenthesis => {}
                TokenContent::RightParenthesis => {}
                TokenContent::LeftBracket => {}
                TokenContent::RightBracket => {}
                TokenContent::LeftBrace => {}
                TokenContent::RightBrace => {
                    pop_next_non_whitespace_token(tokens);
                    return Ok(());
                }
                TokenContent::Dot => {}
                TokenContent::Colon => {}
                TokenContent::Quotes(_) => {}
                TokenContent::FatArrow => {}
                TokenContent::Comma => {}
                TokenContent::Integer(_, _) => {}
                TokenContent::EndOfFile => {}
            }
            Err(ParserError::new(
                "Expected right brace.".to_string(),
                non_whitespace.location,
            ))
        }
        None => todo!(),
    }
}

fn try_skip_left_parenthesis(
    tokens: &mut std::iter::Peekable<std::slice::Iter<'_, Token>>,
) -> bool {
    match peek_next_non_whitespace_token(tokens) {
        Some(non_whitespace) => match &non_whitespace.content {
            TokenContent::Comment(_) => todo!(),
            TokenContent::Whitespace => unreachable!(),
            TokenContent::Identifier(_) => false,
            TokenContent::Assign => todo!(),
            TokenContent::LeftParenthesis => {
                pop_next_non_whitespace_token(tokens);
                true
            }
            TokenContent::RightParenthesis => todo!(),
            TokenContent::LeftBracket => false,
            TokenContent::RightBracket => todo!(),
            TokenContent::LeftBrace => false,
            TokenContent::RightBrace => todo!(),
            TokenContent::Dot => todo!(),
            TokenContent::Colon => todo!(),
            TokenContent::Quotes(_) => false,
            TokenContent::FatArrow => todo!(),
            TokenContent::Comma => false,
            TokenContent::Integer(_, _) => todo!(),
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
            TokenContent::Comment(_) => false,
            TokenContent::Whitespace => unreachable!(),
            TokenContent::Identifier(_) => false,
            TokenContent::Assign => false,
            TokenContent::LeftParenthesis => false,
            TokenContent::RightParenthesis => {
                pop_next_non_whitespace_token(tokens);
                true
            }
            TokenContent::LeftBracket => false,
            TokenContent::RightBracket => false,
            TokenContent::LeftBrace => false,
            TokenContent::RightBrace => false,
            TokenContent::Dot => false,
            TokenContent::Colon => false,
            TokenContent::Quotes(_) => false,
            TokenContent::FatArrow => false,
            TokenContent::Comma => false,
            TokenContent::Integer(_, _) => false,
            TokenContent::EndOfFile => false,
        },
        None => todo!(),
    }
}

fn try_skip_assign(tokens: &mut std::iter::Peekable<std::slice::Iter<'_, Token>>) -> bool {
    match peek_next_non_whitespace_token(tokens) {
        Some(non_whitespace) => match &non_whitespace.content {
            TokenContent::Comment(_) => todo!(),
            TokenContent::Whitespace => unreachable!(),
            TokenContent::Identifier(_) => false,
            TokenContent::Assign => {
                pop_next_non_whitespace_token(tokens);
                true
            }
            TokenContent::LeftParenthesis => todo!(),
            TokenContent::RightParenthesis => todo!(),
            TokenContent::LeftBracket => todo!(),
            TokenContent::RightBracket => todo!(),
            TokenContent::LeftBrace => false,
            TokenContent::RightBrace => todo!(),
            TokenContent::Dot => todo!(),
            TokenContent::Colon => todo!(),
            TokenContent::Quotes(_) => false,
            TokenContent::FatArrow => todo!(),
            TokenContent::Comma => false,
            TokenContent::Integer(_, _) => todo!(),
            TokenContent::EndOfFile => todo!(),
        },
        None => todo!(),
    }
}

fn expect_fat_arrow(tokens: &mut std::iter::Peekable<std::slice::Iter<'_, Token>>) {
    match pop_next_non_whitespace_token(tokens) {
        Some(non_whitespace) => match &non_whitespace.content {
            TokenContent::Comment(_) => todo!(),
            TokenContent::Whitespace => unreachable!(),
            TokenContent::Identifier(_identifier) => todo!(),
            TokenContent::Assign => todo!(),
            TokenContent::LeftParenthesis => todo!(),
            TokenContent::RightParenthesis => todo!(),
            TokenContent::LeftBracket => todo!(),
            TokenContent::RightBracket => todo!(),
            TokenContent::LeftBrace => todo!(),
            TokenContent::RightBrace => todo!(),
            TokenContent::Dot => todo!(),
            TokenContent::Colon => todo!(),
            TokenContent::Quotes(_) => todo!(),
            TokenContent::FatArrow => {}
            TokenContent::Comma => todo!(),
            TokenContent::Integer(_, _) => todo!(),
            TokenContent::EndOfFile => todo!(),
        },
        None => todo!(),
    }
}

fn expect_comma(tokens: &mut std::iter::Peekable<std::slice::Iter<'_, Token>>) -> ParserResult<()> {
    match pop_next_non_whitespace_token(tokens) {
        Some(non_whitespace) => {
            match &non_whitespace.content {
                TokenContent::Comment(_) => todo!(),
                TokenContent::Whitespace => unreachable!(),
                TokenContent::Identifier(_) => {}
                TokenContent::Assign => {}
                TokenContent::LeftParenthesis => {}
                TokenContent::RightParenthesis => {}
                TokenContent::LeftBracket => {}
                TokenContent::RightBracket => {}
                TokenContent::LeftBrace => {}
                TokenContent::RightBrace => {}
                TokenContent::Dot => {}
                TokenContent::Colon => {}
                TokenContent::Quotes(_) => {}
                TokenContent::FatArrow => {}
                TokenContent::Comma => {
                    return Ok(());
                }
                TokenContent::Integer(_, _) => {}
                TokenContent::EndOfFile => {}
            }
            Err(ParserError::new(
                "Expected comma.".to_string(),
                non_whitespace.location,
            ))
        }
        None => todo!(),
    }
}

fn skip_right_bracket(tokens: &mut std::iter::Peekable<std::slice::Iter<'_, Token>>) -> bool {
    let maybe_right_bracket = peek_next_non_whitespace_token(tokens);
    match maybe_right_bracket {
        Some(token) => match &token.content {
            TokenContent::Comment(_) => todo!(),
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
            TokenContent::LeftBrace => false,
            TokenContent::RightBrace => false,
            TokenContent::Dot => false,
            TokenContent::Colon => false,
            TokenContent::Quotes(_) => false,
            TokenContent::FatArrow => false,
            TokenContent::Comma => false,
            TokenContent::Integer(_, _) => false,
            TokenContent::EndOfFile => false,
        },
        None => false,
    }
}

fn parse_tree_construction(
    tokens: &mut std::iter::Peekable<std::slice::Iter<'_, Token>>,
    local_namespace: &NamespaceId,
    location: &SourceLocation,
) -> ParserResult<ast::Expression> {
    let mut elements = Vec::new();
    loop {
        if skip_right_bracket(tokens) {
            break;
        }
        if !elements.is_empty() {
            expect_comma(tokens)?;
        }
        if skip_right_bracket(tokens) {
            break;
        }
        let element = parse_expression(tokens, local_namespace)?;
        elements.push(element);
    }
    Ok(ast::Expression::ConstructTree(elements, *location))
}

fn parse_braces(
    tokens: &mut std::iter::Peekable<std::slice::Iter<'_, Token>>,
    local_namespace: &NamespaceId,
) -> ParserResult<ast::Expression> {
    let content = parse_expression(tokens, local_namespace)?;
    expect_right_brace(tokens)?;
    Ok(ast::Expression::Braces(Box::new(content)))
}

fn parse_let(
    tokens: &mut std::iter::Peekable<std::slice::Iter<'_, Token>>,
    local_namespace: &NamespaceId,
    let_location: &SourceLocation,
) -> ParserResult<ast::Expression> {
    let (name, location) = match try_pop_identifier(tokens) {
        Some((name, location)) => (name, location),
        None => {
            return Err(ParserError::new(
                "Expected identifier after 'let' keyword.".to_string(),
                *let_location,
            ))
        }
    };
    if !try_skip_assign(tokens) {
        return Err(ParserError::new(
            "Expected '=' after 'let' identifier.".to_string(),
            *let_location,
        ));
    }
    let value = parse_expression(tokens, local_namespace)?;
    let body = parse_expression(tokens, local_namespace)?;
    Ok(ast::Expression::Let {
        name: Name::new(*local_namespace, name),
        location,
        value: Box::new(value),
        body: Box::new(body),
    })
}

fn parse_type_of(
    tokens: &mut std::iter::Peekable<std::slice::Iter<'_, Token>>,
    local_namespace: &NamespaceId,
    type_of_location: &SourceLocation,
) -> ParserResult<ast::Expression> {
    if !try_skip_left_parenthesis(tokens) {
        return Err(ParserError::new(
            "Expected '(' after 'type_of' keyword.".to_string(),
            *type_of_location,
        ));
    }
    let expression = parse_expression(tokens, local_namespace)?;
    if !try_skip_right_parenthesis(tokens) {
        return Err(ParserError::new(
            "Expected ')' after expression in 'type_of'.".to_string(),
            expression.source_location(),
        ));
    }
    Ok(ast::Expression::TypeOf(Box::new(expression)))
}

fn parse_comment(
    tokens: &mut std::iter::Peekable<std::slice::Iter<'_, Token>>,
    content: &str,
    local_namespace: &NamespaceId,
    comment_location: &SourceLocation,
) -> ParserResult<ast::Expression> {
    let expression = parse_expression(tokens, local_namespace)?;
    Ok(ast::Expression::Comment(
        content.to_string(),
        Box::new(expression),
        *comment_location,
    ))
}

fn parse_integer(
    value: i64,
    base: IntegerBase,
    integer_location: &SourceLocation,
) -> ParserResult<ast::Expression> {
    Ok(ast::Expression::IntegerLiteral(
        value,
        base,
        *integer_location,
    ))
}

fn parse_expression_start<'t>(
    tokens: &mut std::iter::Peekable<std::slice::Iter<'t, Token>>,
    local_namespace: &NamespaceId,
) -> ParserResult<ast::Expression> {
    match peek_next_non_whitespace_token(tokens) {
        Some(non_whitespace) => match &non_whitespace.content {
            TokenContent::Whitespace => unreachable!(),
            TokenContent::Identifier(identifier) => {
                pop_next_non_whitespace_token(tokens);
                if identifier.as_str() == "let" {
                    parse_let(tokens, local_namespace, &non_whitespace.location)
                } else if identifier.as_str() == "type_of" {
                    parse_type_of(tokens, local_namespace, &non_whitespace.location)
                } else {
                    Ok(ast::Expression::Identifier(
                        Name::new(*local_namespace, identifier.clone()),
                        non_whitespace.location,
                    ))
                }
            }
            TokenContent::Assign => Err(ParserError::new(
                "Expected expression, found assignment operator.".to_string(),
                non_whitespace.location,
            )),
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
                parse_tree_construction(tokens, local_namespace, &non_whitespace.location)
            }
            TokenContent::RightBracket => Err(ParserError::new(
                "Expected expression, found right bracket.".to_string(),
                non_whitespace.location,
            )),
            TokenContent::LeftBrace => {
                pop_next_non_whitespace_token(tokens);
                parse_braces(tokens, local_namespace)
            }
            TokenContent::RightBrace => Err(ParserError::new(
                "Expected expression, found right brace.".to_string(),
                non_whitespace.location,
            )),
            TokenContent::Dot => Err(ParserError::new(
                "Expected expression, found dot.".to_string(),
                non_whitespace.location,
            )),
            TokenContent::Colon => Err(ParserError::new(
                "Expected expression, found colon.".to_string(),
                non_whitespace.location,
            )),
            TokenContent::Quotes(content) => {
                pop_next_non_whitespace_token(tokens);
                Ok(ast::Expression::StringLiteral(
                    content.clone(),
                    non_whitespace.location,
                ))
            }
            TokenContent::FatArrow => Err(ParserError::new(
                "Expected expression, found fat arrow.".to_string(),
                non_whitespace.location,
            )),
            TokenContent::Comma => Err(ParserError::new(
                "Expected expression, found comma.".to_string(),
                non_whitespace.location,
            )),
            TokenContent::EndOfFile => Err(ParserError::new(
                "Expected expression, got end of file.".to_string(),
                non_whitespace.location,
            )),
            TokenContent::Comment(content) => {
                pop_next_non_whitespace_token(tokens);
                parse_comment(tokens, content, local_namespace, &non_whitespace.location)
            }
            TokenContent::Integer(value, base) => {
                pop_next_non_whitespace_token(tokens);
                parse_integer(*value, *base, &non_whitespace.location)
            }
        },
        None => todo!(),
    }
}

fn parse_apply(
    callee: ast::Expression,
    tokens: &mut std::iter::Peekable<std::slice::Iter<'_, Token>>,
    local_namespace: &NamespaceId,
) -> ParserResult<ast::Expression> {
    let mut arguments = Vec::new();
    loop {
        if try_skip_right_parenthesis(tokens) {
            break;
        }
        if !arguments.is_empty() {
            expect_comma(tokens)?;
        }
        if try_skip_right_parenthesis(tokens) {
            break;
        }
        let argument = parse_expression(tokens, local_namespace)?;
        arguments.push(argument);
    }
    Ok(ast::Expression::Apply {
        callee: Box::new(callee),
        arguments,
    })
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
                parse_apply(start, tokens, local_namespace)
            }
            TokenContent::RightParenthesis => Ok(start),
            TokenContent::LeftBracket => Ok(start),
            TokenContent::RightBracket => Ok(start),
            TokenContent::LeftBrace => Ok(start),
            TokenContent::RightBrace => Ok(start),
            TokenContent::Dot => todo!(),
            TokenContent::Colon => Ok(start),
            TokenContent::Quotes(_) => Ok(start),
            TokenContent::FatArrow => Ok(start),
            TokenContent::Comma => Ok(start),
            TokenContent::EndOfFile => Ok(start),
            TokenContent::Integer(_, _) => Ok(start),
            TokenContent::Comment(_) => Ok(start),
        },
        None => todo!(),
    }
}

fn try_pop_identifier(
    tokens: &mut std::iter::Peekable<std::slice::Iter<'_, Token>>,
) -> Option<(String, SourceLocation)> {
    match peek_next_non_whitespace_token(tokens) {
        Some(non_whitespace) => match &non_whitespace.content {
            TokenContent::Comment(_) => todo!(),
            TokenContent::Whitespace => unreachable!(),
            TokenContent::Identifier(identifier) => {
                pop_next_non_whitespace_token(tokens);
                Some((identifier.clone(), non_whitespace.location))
            }
            TokenContent::Assign => None,
            TokenContent::LeftParenthesis => None,
            TokenContent::RightParenthesis => None,
            TokenContent::LeftBracket => None,
            TokenContent::RightBracket => None,
            TokenContent::LeftBrace => None,
            TokenContent::RightBrace => None,
            TokenContent::Dot => None,
            TokenContent::Colon => None,
            TokenContent::Quotes(_) => None,
            TokenContent::FatArrow => None,
            TokenContent::Comma => None,
            TokenContent::Integer(_, _) => None,
            TokenContent::EndOfFile => None,
        },
        None => None,
    }
}

fn try_skip_comma(tokens: &mut std::iter::Peekable<std::slice::Iter<'_, Token>>) -> bool {
    match peek_next_non_whitespace_token(tokens) {
        Some(non_whitespace) => match &non_whitespace.content {
            TokenContent::Comment(_) => todo!(),
            TokenContent::Whitespace => unreachable!(),
            TokenContent::Identifier(_identifier) => false,
            TokenContent::Assign => todo!(),
            TokenContent::LeftParenthesis => todo!(),
            TokenContent::RightParenthesis => false,
            TokenContent::LeftBracket => todo!(),
            TokenContent::RightBracket => todo!(),
            TokenContent::LeftBrace => todo!(),
            TokenContent::RightBrace => todo!(),
            TokenContent::Dot => todo!(),
            TokenContent::Colon => todo!(),
            TokenContent::Quotes(_) => todo!(),
            TokenContent::FatArrow => todo!(),
            TokenContent::Comma => {
                pop_next_non_whitespace_token(tokens);
                true
            }
            TokenContent::Integer(_, _) => todo!(),
            TokenContent::EndOfFile => false,
        },
        None => false,
    }
}

fn try_skip_colon(tokens: &mut std::iter::Peekable<std::slice::Iter<'_, Token>>) -> bool {
    match peek_next_non_whitespace_token(tokens) {
        Some(non_whitespace) => match &non_whitespace.content {
            TokenContent::Comment(_) => todo!(),
            TokenContent::Whitespace => unreachable!(),
            TokenContent::Identifier(_identifier) => false,
            TokenContent::Assign => todo!(),
            TokenContent::LeftParenthesis => todo!(),
            TokenContent::RightParenthesis => false,
            TokenContent::LeftBracket => todo!(),
            TokenContent::RightBracket => todo!(),
            TokenContent::LeftBrace => todo!(),
            TokenContent::RightBrace => todo!(),
            TokenContent::Dot => todo!(),
            TokenContent::Colon => {
                pop_next_non_whitespace_token(tokens);
                true
            }
            TokenContent::Quotes(_) => todo!(),
            TokenContent::FatArrow => todo!(),
            TokenContent::Comma => false,
            TokenContent::Integer(_, _) => todo!(),
            TokenContent::EndOfFile => false,
        },
        None => false,
    }
}

fn parse_lambda<'t>(
    tokens: &mut std::iter::Peekable<std::slice::Iter<'t, Token>>,
    local_namespace: &NamespaceId,
) -> ParserResult<ast::Expression> {
    let mut parameters = Vec::new();
    while let Some((parameter_name, parameter_location)) = try_pop_identifier(tokens) {
        let namespaced_name = Name::new(*local_namespace, parameter_name);
        let mut type_annotation = None;
        if try_skip_colon(tokens) {
            type_annotation = Some(parse_expression(tokens, local_namespace)?);
        }
        parameters.push(LambdaParameter::new(
            namespaced_name,
            parameter_location,
            type_annotation,
        ));
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
        parameters,
        body: Box::new(body),
    })
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone)]
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
