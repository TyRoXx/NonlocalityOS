use crate::ast::{Expression, LambdaParameter};

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
    for argument in arguments.iter() {
        format_expression(argument, indentation_level, writer)?;
        write!(writer, ", ")?;
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
    for parameter in parameters.iter() {
        write!(writer, "{}", parameter.name.key)?;
        if let Some(type_annotation) = &parameter.type_annotation {
            write!(writer, ": ")?;
            format_expression(type_annotation, indentation_level, writer)?;
        }
        write!(writer, ", ")?;
    }
    write!(writer, ") => ")?;
    format_expression(body, indentation_level + 1, writer)
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
        Expression::ConstructTree(children) => {
            write!(writer, "[")?;
            for child in children.iter() {
                format_expression(child, indentation_level, writer)?;
                write!(writer, ", ")?;
            }
            write!(writer, "]")
        }
        Expression::Braces(expression) => {
            write!(writer, "{{")?;
            format_expression(expression, indentation_level, writer)?;
            write!(writer, "}}")
        }
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
    }
}
