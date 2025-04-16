use crate::types::{Name, Type};
use astraea::tree::{BlobDigest, HashedValue, Value};
use astraea::{
    storage::{LoadValue, StoreError, StoreValue},
    tree::ValueBlob,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    pin::Pin,
    sync::Arc,
};

#[derive(Debug, Ord, Eq, PartialEq, PartialOrd, Hash, Clone)]
pub struct Application {
    pub callee: Expression,
    pub argument: Expression,
}

impl Application {
    pub fn new(callee: Expression, argument: Expression) -> Self {
        Self { callee, argument }
    }
}

#[derive(Debug, Ord, Eq, PartialEq, PartialOrd, Hash, Clone)]
pub struct LambdaExpression {
    pub parameter_name: Name,
    pub body: Expression,
}

impl LambdaExpression {
    pub fn new(parameter_name: Name, body: Expression) -> Self {
        Self {
            parameter_name,
            body,
        }
    }

    pub async fn deserialize(
        value: &Value,
        load_value: &(dyn LoadValue + Sync),
    ) -> Option<LambdaExpression> {
        if value.references().len() != 2 {
            return None;
        }
        let parameter_name = match postcard::from_bytes(value.blob().as_slice()) {
            Ok(name) => name,
            Err(_) => return None,
        };
        let body = Expression::deserialize(
            load_value
                .load_value(&value.references()[1])
                .await?
                .hash()?
                .value(),
            load_value,
        )
        .await?;
        Some(LambdaExpression::new(parameter_name, body))
    }

    pub async fn serialize(
        &self,
        storage: &(dyn StoreValue + Sync),
    ) -> std::result::Result<HashedValue, StoreError> {
        let parameter_name: Vec<u8> = match postcard::to_allocvec(&self.parameter_name) {
            Ok(success) => success,
            Err(_) => todo!(),
        };
        let blob = ValueBlob::try_from(bytes::Bytes::from_owner(parameter_name)).unwrap();
        let body = self.body.serialize(storage).await?;
        let references = vec![*body.digest()];
        Ok(HashedValue::from(Arc::new(Value::new(blob, references))))
    }

    pub fn find_captured_names(&self) -> BTreeSet<Name> {
        self.body.find_captured_names()
    }
}

#[derive(Debug, PartialEq, Eq, Ord, PartialOrd, Hash, Clone)]
pub enum Expression {
    Unit,
    Literal(Type, HashedValue),
    Apply(Box<Application>),
    ReadVariable(Name),
    Lambda(Box<LambdaExpression>),
    MakeValue(Vec<Expression>),
}

impl Expression {
    pub fn print(&self, writer: &mut dyn std::fmt::Write, level: usize) -> std::fmt::Result {
        match self {
            Expression::Unit => write!(writer, "()"),
            Expression::Literal(literal_type, literal_value) => {
                write!(writer, "literal(")?;
                literal_type.print(writer)?;
                write!(writer, ", {})", literal_value.digest())
            }
            Expression::Apply(application) => {
                application.callee.print(writer, level)?;
                write!(writer, "(")?;
                application.argument.print(writer, level)?;
                write!(writer, ")")
            }
            Expression::ReadVariable(name) => {
                write!(writer, "{}", &name.key)
            }
            Expression::Lambda(lambda_expression) => {
                write!(writer, "({}) =>\n", &lambda_expression.parameter_name.key)?;
                let indented = level + 1;
                for _ in 0..(indented * 2) {
                    write!(writer, " ")?;
                }
                lambda_expression.body.print(writer, level + 1)
            }
            Expression::MakeValue(arguments) => {
                write!(writer, "make_value(")?;
                for argument in arguments {
                    argument.print(writer, level)?;
                    write!(writer, ", ")?;
                }
                write!(writer, ")")
            }
        }
    }

    pub async fn deserialize(
        _value: &Value,
        _load_value: &(dyn LoadValue + Sync),
    ) -> Option<Expression> {
        todo!()
    }

    pub async fn serialize(
        &self,
        _storage: &(dyn StoreValue + Sync),
    ) -> std::result::Result<HashedValue, StoreError> {
        todo!()
    }

    pub fn to_string(&self) -> String {
        let mut result = String::new();
        self.print(&mut result, 0).unwrap();
        result
    }

    pub fn find_captured_names(&self) -> BTreeSet<Name> {
        match self {
            Expression::Unit => BTreeSet::new(),
            Expression::Literal(_, _blob_digest) => BTreeSet::new(),
            Expression::Apply(application) => {
                let mut result = application.argument.find_captured_names();
                result.append(&mut application.argument.find_captured_names());
                result
            }
            Expression::ReadVariable(name) => BTreeSet::from([name.clone()]),
            Expression::Lambda(lambda_expression) => {
                let mut result = lambda_expression.body.find_captured_names();
                result.remove(&lambda_expression.parameter_name);
                result
            }
            Expression::MakeValue(arguments) => {
                let mut result = BTreeSet::new();
                for argument in arguments {
                    result.append(&mut argument.find_captured_names());
                }
                result
            }
        }
    }
}

#[derive(Debug)]
pub struct Closure {
    lambda: LambdaExpression,
    captured_variables: BTreeMap<Name, Pointer>,
}

impl Closure {
    pub fn new(lambda: LambdaExpression, captured_variables: BTreeMap<Name, Pointer>) -> Self {
        Self {
            lambda,
            captured_variables,
        }
    }

    pub async fn deserialize(
        value: &Value,
        load_value: &(dyn LoadValue + Sync),
    ) -> Option<Closure> {
        if value.blob().len() != 0 {
            return None;
        }
        if value.references().len() < 1 {
            return None;
        }
        let lambda_expression = LambdaExpression::deserialize(
            load_value
                .load_value(&value.references()[0])
                .await?
                .hash()?
                .value(),
            load_value,
        )
        .await?;
        let captured_variables = BTreeMap::new();
        // TODO: deserialize the captured variables
        Some(Closure::new(lambda_expression, captured_variables))
    }

    async fn serialize(
        &self,
        storage: &(dyn StoreValue + Sync),
    ) -> std::result::Result<HashedValue, StoreError> {
        let lambda = self.lambda.serialize(storage).await?;
        Ok(HashedValue::from(Arc::new(Value::new(
            ValueBlob::empty(),
            vec![*lambda.digest()],
        ))))
    }

    async fn call_method(
        &self,
        argument: &Pointer,
        load_value: &(dyn LoadValue + Sync),
        store_value: &(dyn StoreValue + Sync),
        read_variable: &Arc<ReadVariable>,
        read_literal: &ReadLiteral,
    ) -> std::result::Result<Pointer, StoreError> {
        let read_variable_in_body: Arc<ReadVariable> = Arc::new({
            let parameter_name = self.lambda.parameter_name.clone();
            let argument = argument.clone();
            let captured_variables = self.captured_variables.clone();
            let read_variable = read_variable.clone();
            move |name: &Name| -> Pin<Box<dyn core::future::Future<Output = Pointer> + Send>> {
                if name == &parameter_name {
                    let argument = argument.clone();
                    Box::pin(core::future::ready(argument))
                } else if let Some(found) = captured_variables.get(name) {
                    Box::pin(core::future::ready(found.clone()))
                } else {
                    read_variable(name)
                }
            }
        });
        Box::pin(evaluate(
            &self.lambda.body,
            load_value,
            store_value,
            &read_variable_in_body,
            read_literal,
        ))
        .await
    }
}

#[derive(Debug, Clone)]
pub struct InMemoryValue {
    pub blob: ValueBlob,
    pub references: Vec<Pointer>,
}

impl InMemoryValue {
    pub fn new(blob: ValueBlob, references: Vec<Pointer>) -> Self {
        Self { blob, references }
    }
}

#[derive(Debug, Clone)]
pub enum Pointer {
    Value(HashedValue),
    Reference(BlobDigest),
    InMemoryValue(InMemoryValue),
}

impl Pointer {
    pub fn serialize(self) -> HashedValue {
        match self {
            Pointer::Value(hashed_value) => hashed_value,
            Pointer::Reference(_blob_digest) => todo!(),
            Pointer::InMemoryValue(_in_memory_value) => {
                todo!()
            }
        }
    }

    pub async fn serialize_to_flat_value(&self) -> Option<Arc<Value>> {
        match self {
            Pointer::Value(hashed_value) => {
                if hashed_value.value().references().is_empty() {
                    Some(hashed_value.value().clone())
                } else {
                    None
                }
            }
            Pointer::Reference(_blob_digest) => todo!(),
            Pointer::InMemoryValue(_in_memory_value) => {
                todo!()
            }
        }
    }
}

pub type ReadVariable =
    dyn Fn(&Name) -> Pin<Box<dyn core::future::Future<Output = Pointer> + Send>> + Send + Sync;

pub type ReadLiteral = dyn Fn(Type, HashedValue) -> Pin<Box<dyn core::future::Future<Output = Pointer> + Send>>
    + Send
    + Sync;

pub async fn evaluate(
    expression: &Expression,
    load_value: &(dyn LoadValue + Sync),
    store_value: &(dyn StoreValue + Sync),
    read_variable: &Arc<ReadVariable>,
    read_literal: &ReadLiteral,
) -> std::result::Result<Pointer, StoreError> {
    match expression {
        Expression::Unit => return Ok(Pointer::Value(HashedValue::from(Arc::new(Value::empty())))),
        Expression::Literal(literal_type, literal_value) => {
            let literal = read_literal(literal_type.clone(), literal_value.clone()).await;
            Ok(literal)
        }
        Expression::Apply(application) => {
            let evaluated_callee = Box::pin(evaluate(
                &application.callee,
                load_value,
                store_value,
                read_variable,
                read_literal,
            ))
            .await?;
            let evaluated_argument = Box::pin(evaluate(
                &application.argument,
                load_value,
                store_value,
                read_variable,
                read_literal,
            ))
            .await?;
            let callee_closure =
                match Closure::deserialize(&evaluated_callee.serialize().value(), load_value).await
                {
                    Some(success) => success,
                    None => todo!(),
                };
            let call_result = callee_closure
                .call_method(
                    &evaluated_argument,
                    load_value,
                    store_value,
                    read_variable,
                    read_literal,
                )
                .await;
            call_result
        }
        Expression::ReadVariable(name) => Ok(read_variable(&name).await),
        Expression::Lambda(lambda_expression) => {
            let mut captured_variables = BTreeMap::new();
            for captured_name in lambda_expression.find_captured_names().into_iter() {
                let captured_value = read_variable(&captured_name).await;
                captured_variables.insert(captured_name, captured_value);
            }
            Closure::new((**lambda_expression).clone(), captured_variables)
                .serialize(store_value)
                .await
                .map(|value| Pointer::Value(value))
        }
        Expression::MakeValue(arguments) => {
            let mut evaluated_arguments = Vec::new();
            for argument in arguments {
                let evaluated_argument = Box::pin(evaluate(
                    argument,
                    load_value,
                    store_value,
                    read_variable,
                    read_literal,
                ))
                .await?;
                evaluated_arguments.push(*evaluated_argument.serialize().digest());
            }
            Ok(Pointer::Value(HashedValue::from(Arc::new(Value::new(
                ValueBlob::empty(),
                evaluated_arguments,
            )))))
        }
    }
}
