use std::sync::Arc;

use serde::de::DeserializeOwned;

use crate::preprocess::{Document, Schema};

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

    fn batch(&self, documents: Vec<Document>) -> impl Future<Output = Result<(), Self::Error>>;
}
