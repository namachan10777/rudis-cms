use std::collections::HashMap;

use serde::de::DeserializeOwned;

pub mod cloudflare;

pub trait Backend: Sized {
    type Error;

    type ImageBackendConfig: DeserializeOwned;
    type BlobBackendConfig: DeserializeOwned;
    type SetBackendConfig: DeserializeOwned;
    type BackendConfig: DeserializeOwned;
    type RichTextConfig: DeserializeOwned;

    fn print_schema(&self) -> String;

    fn init(
        config: Self::BackendConfig,
        schema: HashMap<String, crate::config::FieldDef<Self>>,
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
