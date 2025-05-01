use astraea::tree::{BlobDigest, Value, ValueBlob};

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct ConsoleOutput {
    pub message: BlobDigest,
}

impl ConsoleOutput {
    pub fn to_value(&self) -> Value {
        Value::new(ValueBlob::empty(), vec![self.message])
    }

    pub fn from_value(value: &Value) -> Option<ConsoleOutput> {
        if value.blob().len() != 0 {
            return None;
        }
        if value.references().len() != 1 {
            return None;
        }
        Some(ConsoleOutput {
            message: value.references()[0],
        })
    }
}
