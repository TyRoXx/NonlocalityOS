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

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct AndThen {
    pub effect: BlobDigest,
    pub handle_result: BlobDigest,
}

impl AndThen {
    pub fn new(effect: BlobDigest, handle_result: BlobDigest) -> Self {
        Self {
            effect,
            handle_result,
        }
    }

    pub fn to_value(&self) -> Value {
        Value::new(ValueBlob::empty(), vec![self.effect, self.handle_result])
    }

    pub fn from_value(value: &Value) -> Option<AndThen> {
        if value.blob().len() != 0 {
            return None;
        }
        if value.references().len() != 2 {
            return None;
        }
        Some(AndThen {
            effect: value.references()[0],
            handle_result: value.references()[1],
        })
    }
}
