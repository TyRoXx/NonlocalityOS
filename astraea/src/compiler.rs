use crate::tree::{CompilerOutput, InMemoryValueStorage, LoadValue, StoreValue, TypeId, Value};
use std::sync::Arc;
struct Token {}

fn tokenize(source: &str) -> Vec<Token> {
    todo!()
}

pub fn compile(source: &str, loader: &dyn LoadValue, storage: &dyn StoreValue) -> CompilerOutput {
    let errors = Vec::new();
    let entry_point = storage
        .store_value(Arc::new(Value::from_unit()))
        .add_type(TypeId(1));
    CompilerOutput::new(entry_point, errors)
}

#[test]
fn test_compile_empty_source() {
    let value_storage =
        InMemoryValueStorage::new(std::sync::Mutex::new(std::collections::BTreeMap::new()));
    let output = compile("", &value_storage, &value_storage);
    let expected = CompilerOutput::new(
        value_storage
            .store_value(Arc::new(Value::from_unit()))
            .add_type(TypeId(1)),
        Vec::new(),
    );
    assert_eq!(expected, output);
    assert_eq!(1, value_storage.len());
}
