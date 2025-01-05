#![feature(test)]
use astraea::{
    storage::{LoadValue, StoreError, StoreValue},
    tree::{BlobDigest, HashedValue, Value},
};
use async_trait::async_trait;
use dogbox_tree::serialization::{self, FileName};
use lambda::expressions::{Object, Pointer, ReadLiteral, ReadVariable};
use lambda::types::Name;
use std::sync::Arc;

#[cfg(test)]
mod tests;

#[derive(Debug)]
pub struct FileNameObject {
    pub content: FileName,
}

impl FileNameObject {
    pub fn new(content: FileName) -> Self {
        Self { content }
    }
}

#[async_trait]
impl Object for FileNameObject {
    async fn call_method(
        &self,
        _interface: &BlobDigest,
        _method: &Name,
        _argument: &Pointer,
        _storage: &(dyn LoadValue + Sync),
        _read_variable: &Arc<ReadVariable>,
        _read_literal: &ReadLiteral,
    ) -> std::result::Result<Pointer, ()> {
        todo!()
    }

    async fn serialize(
        &self,
        _storage: &dyn StoreValue,
    ) -> std::result::Result<HashedValue, StoreError> {
        todo!()
    }

    async fn serialize_to_flat_value(&self) -> Option<Arc<Value>> {
        match Value::from_object(&self.content) {
            Ok(success) => Some(Arc::new(success)),
            Err(_) => todo!(),
        }
    }
}

#[derive(Debug)]
pub struct SmallBytes {
    single_value_content: HashedValue,
}

impl SmallBytes {
    pub fn new(single_value_content: HashedValue) -> Self {
        Self {
            single_value_content,
        }
    }
}

#[async_trait]
impl Object for SmallBytes {
    async fn call_method(
        &self,
        _interface: &BlobDigest,
        _method: &Name,
        _argument: &Pointer,
        _storage: &(dyn LoadValue + Sync),
        _read_variable: &Arc<ReadVariable>,
        _read_literal: &ReadLiteral,
    ) -> std::result::Result<Pointer, ()> {
        todo!()
    }

    async fn serialize(
        &self,
        _storage: &dyn StoreValue,
    ) -> std::result::Result<HashedValue, StoreError> {
        Ok(self.single_value_content.clone())
    }

    async fn serialize_to_flat_value(&self) -> Option<Arc<Value>> {
        Some(self.single_value_content.value().clone())
    }
}

#[derive(Debug)]
pub struct LoadedFile {
    interface: BlobDigest,
    read: Name,
    content: BlobDigest,
    storage: Arc<(dyn LoadValue + Send + Sync)>,
}

impl LoadedFile {
    pub fn new(
        interface: BlobDigest,
        read: Name,
        content: BlobDigest,
        storage: Arc<(dyn LoadValue + Send + Sync)>,
    ) -> Self {
        Self {
            interface,
            read,
            content,
            storage,
        }
    }
}

#[async_trait]
impl Object for LoadedFile {
    async fn call_method(
        &self,
        interface: &BlobDigest,
        method: &Name,
        _argument: &Pointer,
        _storage: &(dyn LoadValue + Sync),
        _read_variable: &Arc<ReadVariable>,
        _read_literal: &ReadLiteral,
    ) -> std::result::Result<Pointer, ()> {
        if &self.interface == interface {
            if &self.read == method {
                // the argument is unit, so we don't need to check it
                let content = self
                    .storage
                    .load_value(&self.content)
                    .await.unwrap(/*TODO*/);
                let hashed = content.hash().unwrap(/*TODO*/);
                let result: Arc<(dyn Object + Sync)> = Arc::new(SmallBytes::new(hashed));
                Ok(Pointer::Object(result))
            } else {
                todo!()
            }
        } else {
            todo!()
        }
    }

    async fn serialize(
        &self,
        _storage: &dyn StoreValue,
    ) -> std::result::Result<HashedValue, StoreError> {
        todo!()
    }

    async fn serialize_to_flat_value(&self) -> Option<Arc<Value>> {
        None
    }
}

#[derive(Debug)]
pub struct LoadedDirectory {
    value_for_references: Arc<Value>,
    data: dogbox_tree::serialization::DirectoryTree,
    self_interface: BlobDigest,
    get: Name,
    file_interface: BlobDigest,
    read: Name,
    storage: Arc<(dyn LoadValue + Send + Sync)>,
}

impl LoadedDirectory {
    pub fn new(
        value_for_references: Arc<Value>,
        data: dogbox_tree::serialization::DirectoryTree,
        self_interface: BlobDigest,
        get: Name,
        file_interface: BlobDigest,
        read: Name,
        storage: Arc<(dyn LoadValue + Send + Sync)>,
    ) -> Self {
        Self {
            value_for_references,
            data,
            self_interface,
            get,
            file_interface,
            read,
            storage,
        }
    }
}

#[async_trait]
impl Object for LoadedDirectory {
    async fn call_method(
        &self,
        interface: &BlobDigest,
        method: &Name,
        argument: &Pointer,
        _storage: &(dyn LoadValue + Sync),
        _read_variable: &Arc<ReadVariable>,
        _read_literal: &ReadLiteral,
    ) -> std::result::Result<Pointer, ()> {
        if &self.self_interface == interface {
            if &self.get == method {
                let argument_value = match argument.serialize_to_flat_value().await {
                    Some(success) => success,
                    None => todo!(),
                };
                let key: serialization::FileName = match argument_value.to_object() {
                    Ok(success) => success,
                    Err(_) => todo!(),
                };
                let child = match self.data.children.get(&key) {
                    Some(found) => found,
                    None => todo!(),
                };
                match &child.content {
                    dogbox_tree::serialization::ReferenceIndexOrInlineContent::Indirect(
                        reference_index,
                    ) => Ok(Pointer::Object(Arc::new(LoadedFile::new(
                        self.file_interface,
                        self.read.clone(),
                        self.value_for_references.references()[reference_index.0 as usize],
                        self.storage.clone(),
                    )))),
                    dogbox_tree::serialization::ReferenceIndexOrInlineContent::Direct(_vec) => {
                        todo!()
                    }
                }
            } else {
                todo!()
            }
        } else {
            todo!()
        }
    }

    async fn serialize(
        &self,
        _storage: &dyn StoreValue,
    ) -> std::result::Result<HashedValue, StoreError> {
        todo!()
    }

    async fn serialize_to_flat_value(&self) -> Option<Arc<Value>> {
        None
    }
}
