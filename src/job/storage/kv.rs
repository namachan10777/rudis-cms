use base64::Engine as _;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct Pair {
    key: String,
    value: String,
    base64: bool,
    expiration: Option<i64>,
    expiration_ttl: Option<u64>,
    metadata: Option<serde_json::Value>,
}

#[derive(Default)]
pub struct PairBuilder {
    key: Option<String>,
    value: Option<String>,
    base64: bool,
    expiration: Option<i64>,
    expiration_ttl: Option<u64>,
    metadata: Option<Result<serde_json::Value, serde_json::Error>>,
}

impl Pair {
    pub fn builder() -> PairBuilder {
        Default::default()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum PairBuildError {
    #[error("Missing key")]
    MissingKey,
    #[error("Missing value")]
    MissingValue,
    #[error("Failed to encode metadata: {0}")]
    EncodeMetadata(serde_json::Error),
}

impl PairBuilder {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn key(mut self, key: impl Into<String>) -> Self {
        self.key = Some(key.into());
        self
    }

    pub fn string_value(mut self, value: impl Into<String>) -> Self {
        self.value = Some(value.into());
        self.base64 = false;
        self
    }

    pub fn binary_value(mut self, value: &[u8]) -> Self {
        self.value = Some(base64::engine::general_purpose::STANDARD.encode(value));
        self.base64 = true;
        self
    }

    pub fn expiration(mut self, expiration: chrono::DateTime<impl chrono::TimeZone>) -> Self {
        self.expiration = Some(expiration.timestamp());
        self
    }

    pub fn expiration_ttl(mut self, ttl: std::time::Duration) -> Self {
        self.expiration_ttl = Some(ttl.as_secs());
        self
    }

    pub fn metadata(mut self, metadata: impl serde::Serialize) -> PairBuilder {
        self.metadata = Some(serde_json::to_value(&metadata));
        self
    }

    pub fn build(self) -> Result<Pair, PairBuildError> {
        Ok(Pair {
            key: self.key.ok_or(PairBuildError::MissingKey)?,
            value: self.value.ok_or(PairBuildError::MissingValue)?,
            base64: self.base64,
            expiration: self.expiration,
            expiration_ttl: self.expiration_ttl,
            metadata: self
                .metadata
                .transpose()
                .map_err(PairBuildError::EncodeMetadata)?,
        })
    }
}

pub trait Client {
    type Error;
    fn write_multiple(
        &self,
        namespace: &str,
        pairs: &[Pair],
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
    fn delete_multiple(
        &self,
        namespace: &str,
        keys: &[String],
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
}
