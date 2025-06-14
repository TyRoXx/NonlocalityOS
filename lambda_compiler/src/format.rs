use crate::{ast::{Expression, LambdaParameter}, tokenization::IntegerBase};

fn format_string_literal<W>(content: &str, writer: &mut W) -> std::fmt::Result
where
    W: std::fmt::Write,
{
    write!(writer, "\"")?;
    for character in content.chars() {
        match character {
            '"' | '\'' | '\\' => write!(writer, "\\{character}")?,
            '\n' => write!(writer, "\\n")?,
            '\r' => write!(writer, "\\r")?,
            '\t' => write!(writer, "\\t")?,
            _ => write!(writer, "{character}")?,
        }
    }
    write!(writer, "\"")
}

fn format_apply<W>(
    callee: &Expression,
    arguments: &[Expression],
    indentation_level: usize,
    writer: &mut W,
) -> std::fmt::Result
where
    W: std::fmt::Write,
{
    format_expression(callee, indentation_level, writer)?;
    write!(writer, "(")?;
    for (index, argument) in arguments.iter().enumerate() {
        if index > 0 {
            write!(writer, ", ")?;
        }
        format_expression(argument, indentation_level, writer)?;
    }
    write!(writer, ")")
}

fn format_lambda<W>(
    parameters: &[LambdaParameter],
    body: &Expression,
    indentation_level: usize,
    writer: &mut W,
) -> std::fmt::Result
where
    W: std::fmt::Write,
{
    write!(writer, "(")?;
    for (index, parameter) in parameters.iter().enumerate() {
        if index > 0 {
            write!(writer, ", ")?;
        }
        write!(writer, "{}", parameter.name.key)?;
        if let Some(type_annotation) = &parameter.type_annotation {
            write!(writer, ": ")?;
            format_expression(type_annotation, indentation_level, writer)?;
        }
    }
    write!(writer, ") => ")?;
    format_expression(body, indentation_level, writer)
}

fn break_line<W>(indentation_level: usize, writer: &mut W) -> std::fmt::Result
where
    W: std::fmt::Write,
{
    writeln!(writer)?;
    for _ in 0..indentation_level {
        write!(writer, "    ")?;
    }
    Ok(())
}

pub fn format_braces<W>(
    content: &Expression,
    indentation_level: usize,
    writer: &mut W,
) -> std::fmt::Result
where
    W: std::fmt::Write,
{
    write!(writer, "{{")?;
    let mut content_formatted = String::new();
    format_expression(content, indentation_level, &mut content_formatted)?;
    if content_formatted.contains('\n') {
        let inner_indentation_level = indentation_level + 1;
        break_line(inner_indentation_level, writer)?;
        format_expression(content, inner_indentation_level, writer)?;
        break_line(indentation_level, writer)?;
    } else {
        write!(writer, "{content_formatted}")?;
    }
    write!(writer, "}}")
}

pub fn format_expression<W>(
    expression: &Expression,
    indentation_level: usize,
    writer: &mut W,
) -> std::fmt::Result
where
    W: std::fmt::Write,
{
    match expression {
        Expression::Identifier(name, _source_location) => write!(writer, "{}", &name.key),
        Expression::StringLiteral(content, _source_location) => {
            format_string_literal(content, writer)
        }
        Expression::Apply { callee, arguments } => {
            format_apply(callee, arguments, indentation_level, writer)
        }
        Expression::Lambda { parameters, body } => {
            format_lambda(parameters, body, indentation_level, writer)
        }
        Expression::ConstructTree(children, _) => {
            write!(writer, "[")?;
            for (index, child) in children.iter().enumerate() {
                if index > 0 {
                    write!(writer, ", ")?;
                }
                format_expression(child, indentation_level, writer)?;
            }
            write!(writer, "]")
        }
        Expression::Braces(content) => format_braces(content, indentation_level, writer),
        Expression::Let {
            name,
            location: _,
            value,
            body,
        } => {
            write!(writer, "let {} = ", &name.key)?;
            format_expression(value, indentation_level, writer)?;
            break_line(indentation_level, writer)?;
            format_expression(body, indentation_level, writer)
        }
        Expression::TypeOf(expression) => {
            write!(writer, "type_of(")?;
            format_expression(expression, indentation_level, writer)?;
            write!(writer, ")")
        }
        Expression::Comment(comment, expression, _source_location) => {
            write!(writer, "#{comment}")?;
            break_line(indentation_level, writer)?;
            format_expression(expression, indentation_level, writer)
        }
        Expression::IntegerLiteral(value, base, _source_location) => match base {
            IntegerBase::Decimal => write!(writer, "{value}"),
            IntegerBase::Hexadecimal => write!(writer, "0x{value:x}"),
        },
    }
}

pub fn format_file<W>(entry_point: &Expression, writer: &mut W) -> std::fmt::Result
where
    W: std::fmt::Write,
{
    format_expression(entry_point, 0, writer)?;
    writeln!(writer)
}
