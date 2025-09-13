use std::collections::HashSet;

use base64::Engine;
use serde::{Deserialize, Serialize};

pub struct Client {
    account_id: String,
    token: String,
    client: reqwest::Client,
}

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

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("transport error: {0}")]
    Transport(reqwest::Error),
    #[error(
        "failed to manipulate kv store. status: {code}, errors: {errors:?}, messages: {messages:?}"
    )]
    Fail {
        code: reqwest::StatusCode,
        errors: Vec<ResponseInfo>,
        messages: Vec<ResponseInfo>,
    },
    #[error(
        "partial failure to manipulate kv store. status: {code}, errors: {errors:?}, messages: {messages:?}, unsuccessful keys: {unsuccessful_keys:?}"
    )]
    PartialFail {
        code: reqwest::StatusCode,
        errors: Vec<ResponseInfo>,
        messages: Vec<ResponseInfo>,
        unsuccessful_keys: Vec<String>,
    },
    #[error("missing result: {code}, messages: {messages:?}")]
    MissingResult {
        code: reqwest::StatusCode,
        messages: Vec<ResponseInfo>,
    },
}

#[derive(Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
pub struct ResponseInfoPointer {
    pub pointer: Option<String>,
}

#[derive(Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
pub struct ResponseInfo {
    pub code: u16,
    pub message: String,
    pub documentation_url: Option<url::Url>,
    pub source: Option<ResponseInfoPointer>,
}

#[derive(Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct ResponseResult {
    pub successful_key_count: usize,
    pub unsuccessful_keys: HashSet<String>,
}

#[derive(Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Response {
    pub errors: Vec<ResponseInfo>,
    pub messages: Vec<ResponseInfo>,
    pub success: bool,
    pub result: Option<ResponseResult>,
}

pub struct BatchWriteResult {
    pub successful_key_count: usize,
}

impl Client {
    pub fn new(account_id: impl Into<String>, token: impl Into<String>) -> Self {
        Self {
            account_id: account_id.into(),
            token: token.into(),
            client: reqwest::Client::new(),
        }
    }

    pub async fn batch_write(
        &self,
        namespace: &str,
        pairs: &[Pair],
    ) -> Result<BatchWriteResult, Error> {
        let endpoint = format!(
            "https://api.cloudflare.com/client/v4/accounts/{}/storage/kv/namespaces/{namespace}/bulk",
            self.account_id,
        );
        let response = self
            .client
            .put(endpoint)
            .bearer_auth(&self.token)
            .json(pairs)
            .send()
            .await
            .map_err(Error::Transport)?;

        let code = response.status();

        let response = response
            .json::<Response>()
            .await
            .map_err(Error::Transport)?;
        if !response.errors.is_empty() || !response.success {
            return Err(Error::Fail {
                code,
                errors: response.errors,
                messages: response.messages,
            });
        }
        let result = response.result.ok_or_else(|| Error::MissingResult {
            code,
            messages: response.messages,
        })?;
        Ok(BatchWriteResult {
            successful_key_count: result.successful_key_count,
        })
    }
}
