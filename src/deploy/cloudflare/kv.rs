use std::collections::HashSet;

use serde::Deserialize;

use crate::job::storage::kv;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("transport error: {0}")]
    Transport(reqwest::Error),
    #[error(
        "failed to manipulate kv store. status: {code}, errors: {errors:?}, messages: {messages:?}"
    )]
    Fail {
        code: reqwest::StatusCode,
        errors: Vec<super::ResponseInfo>,
        messages: Vec<super::ResponseInfo>,
    },
    #[error(
        "partial failure to manipulate kv store. status: {code}, errors: {errors:?}, messages: {messages:?}, unsuccessful keys: {unsuccessful_keys:?}"
    )]
    PartialFail {
        code: reqwest::StatusCode,
        errors: Vec<super::ResponseInfo>,
        messages: Vec<super::ResponseInfo>,
        unsuccessful_keys: HashSet<String>,
    },
    #[error("missing result: {code}, messages: {messages:?}")]
    MissingResult {
        code: reqwest::StatusCode,
        messages: Vec<super::ResponseInfo>,
    },
}

pub struct Client {
    account_id: String,
    token: String,
    client: reqwest::Client,
}

impl Client {
    pub fn new(account_id: impl Into<String>, token: impl Into<String>) -> Self {
        Self {
            account_id: account_id.into(),
            token: token.into(),
            client: reqwest::Client::new(),
        }
    }
}

#[derive(Deserialize, Debug, Clone, PartialEq, Eq)]
struct ResponseResult {
    pub successful_key_count: usize,
    pub unsuccessful_keys: HashSet<String>,
}

impl kv::Client for Client {
    type Error = Error;
    async fn write_multiple(&self, namespace: &str, pairs: &[kv::Pair]) -> Result<(), Self::Error> {
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
            .json::<super::Response<Option<ResponseResult>>>()
            .await
            .map_err(Error::Transport)?;
        if !response.errors.is_empty() || !response.success {
            return Err(Error::Fail {
                code,
                errors: response.errors,
                messages: response.messages,
            });
        }
        let Some(result) = response.result else {
            return Err(Error::MissingResult {
                code,
                messages: response.messages,
            });
        };
        if !result.unsuccessful_keys.is_empty() {
            return Err(Error::PartialFail {
                code,
                messages: response.messages,
                errors: response.errors,
                unsuccessful_keys: result.unsuccessful_keys,
            });
        }
        Ok(())
    }

    async fn delete_multiple(&self, namespace: &str, keys: &[String]) -> Result<(), Self::Error> {
        let endpoint = format!(
            "https://api.cloudflare.com/client/v4/accounts/{}/storage/kv/namespaces/{namespace}/bulk/delete",
            self.account_id,
        );
        let response = self
            .client
            .put(endpoint)
            .bearer_auth(&self.token)
            .json(keys)
            .send()
            .await
            .map_err(Error::Transport)?;

        let code = response.status();

        let response = response
            .json::<super::Response<Option<ResponseResult>>>()
            .await
            .map_err(Error::Transport)?;
        if !response.errors.is_empty() || !response.success {
            return Err(Error::Fail {
                code,
                errors: response.errors,
                messages: response.messages,
            });
        }
        let Some(result) = response.result else {
            return Err(Error::MissingResult {
                code,
                messages: response.messages,
            });
        };
        if !result.unsuccessful_keys.is_empty() {
            return Err(Error::PartialFail {
                code,
                messages: response.messages,
                errors: response.errors,
                unsuccessful_keys: result.unsuccessful_keys,
            });
        }
        Ok(())
    }
}
