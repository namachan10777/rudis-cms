use base64::Engine as _;
use serde::{Deserialize, Serialize};

use crate::process_data::StorageContent;

#[derive(Serialize, Deserialize, Clone)]
pub struct Pair {
    pub key: String,
    pub value: String,
    pub base64: bool,
    pub expiration: Option<i64>,
    pub expiration_ttl: Option<u64>,
    pub metadata: Option<serde_json::Value>,
}

impl Pair {
    /// Build a `Pair` from a key and a `StorageContent`. Binary content is
    /// base64-encoded automatically.
    pub fn new(key: impl Into<String>, content: StorageContent) -> Self {
        let (value, base64) = match content {
            StorageContent::Text(text) => (text, false),
            StorageContent::Bytes(bin) => {
                (base64::engine::general_purpose::STANDARD.encode(&bin), true)
            }
        };
        Self {
            key: key.into(),
            value,
            base64,
            expiration: None,
            expiration_ttl: None,
            metadata: None,
        }
    }

    pub fn with_expiration(mut self, expiration: chrono::DateTime<impl chrono::TimeZone>) -> Self {
        self.expiration = Some(expiration.timestamp());
        self
    }

    pub fn with_expiration_ttl(mut self, ttl: std::time::Duration) -> Self {
        self.expiration_ttl = Some(ttl.as_secs());
        self
    }

    pub fn with_metadata(
        mut self,
        metadata: impl serde::Serialize,
    ) -> Result<Self, serde_json::Error> {
        self.metadata = Some(serde_json::to_value(&metadata)?);
        Ok(self)
    }
}

pub trait Client {
    type Error: super::BackendError;
    fn put_batch(
        &self,
        namespace: &str,
        pairs: &[Pair],
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
    fn delete_batch(
        &self,
        namespace: &str,
        keys: &[String],
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pair_text_is_not_base64() {
        let p = Pair::new("k", StorageContent::Text("hello".into()));
        assert_eq!(p.key, "k");
        assert_eq!(p.value, "hello");
        assert!(!p.base64);
    }

    #[test]
    fn pair_bytes_is_base64_encoded() {
        let p = Pair::new("k", StorageContent::Bytes(b"\x00\xff\x10".to_vec()));
        assert!(p.base64);
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(&p.value)
            .unwrap();
        assert_eq!(decoded, b"\x00\xff\x10");
    }
}
