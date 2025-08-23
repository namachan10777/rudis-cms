use std::{collections::HashMap, sync::Arc};

use serde::de::DeserializeOwned;

use crate::preprocess::Schema;

pub mod cloudflare;

pub trait Backend: Sized {
    type Error: Send + Sync + 'static + std::error::Error;

    type ImageBackendConfig: DeserializeOwned;
    type BlobBackendConfig: DeserializeOwned;
    type SetBackendConfig: DeserializeOwned;
    type BackendConfig: DeserializeOwned;
    type RichTextConfig: DeserializeOwned;

    fn print_schema(&self) -> String;

    fn init(
        config: Self::BackendConfig,
        schema: Arc<Schema<Self>>,
    ) -> impl Future<Output = Result<Self, Self::Error>>;

    fn changed(
        &self,
        local_hash_set: HashMap<String, blake3::Hash>,
    ) -> impl Future<Output = Result<String, Self::Error>>;

    fn changed_image(
        &self,
        local_hash_set: HashMap<String, blake3::Hash>,
    ) -> impl Future<Output = Result<String, Self::Error>>;
}
