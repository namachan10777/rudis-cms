use serde::{Deserialize, Serialize};
use url::Url;

use crate::{deploy::cloudflare::Response, job};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Transport error: {0}")]
    Transport(reqwest::Error),
    #[error("Query failed: {errors:?} {messages:?} ")]
    QueryFailed {
        errors: Vec<super::ResponseInfo>,
        messages: Vec<super::ResponseInfo>,
    },
    #[error("Empty result: {errors:?} {messages:?}")]
    EmptyResult {
        errors: Vec<super::ResponseInfo>,
        messages: Vec<super::ResponseInfo>,
    },
    #[error("Parse JSON error: {0}")]
    ParseJson(serde_json::Error),
}

pub struct Client {
    token: String,
    client: reqwest::Client,
    url: Url,
}

#[derive(Serialize)]
struct Request<'a> {
    sql: &'a str,
    params: &'a [&'a str],
}

#[derive(Deserialize)]
struct QueryResult<R> {
    #[serde(default = "Vec::new")]
    results: Vec<R>,
}

impl Client {
    pub fn new(
        account_id: String,
        token: String,
        database: String,
    ) -> Result<Self, url::ParseError> {
        Ok(Self {
            token,
            url: format!("https://api.cloudflare.com/client/v4/accounts/{account_id}/d1/database/{database}/query").parse()?,
            client: reqwest::Client::new(),
        })
    }
}

impl job::storage::sqlite::Client for Client {
    type Error = Error;
    async fn query<R>(&self, statement: &str, params: &[&str]) -> Result<Vec<R>, Self::Error>
    where
        R: serde::de::DeserializeOwned
            + for<'a> sqlx::FromRow<'a, sqlx::sqlite::SqliteRow>
            + Send
            + Unpin,
    {
        let response = self
            .client
            .post(self.url.clone())
            .bearer_auth(&self.token)
            .json(&Request {
                sql: statement,
                params,
            })
            .send()
            .await
            .map_err(Error::Transport)?
            .text()
            .await
            .map_err(Error::Transport)?;
        let mut response = serde_json::from_str::<Response<Vec<QueryResult<R>>>>(&response)
            .map_err(Error::ParseJson)?;
        if !response.success {
            return Err(Error::QueryFailed {
                errors: response.errors,
                messages: response.messages,
            });
        }
        let Some(result) = response.result.pop() else {
            return Err(Error::EmptyResult {
                errors: response.errors,
                messages: response.messages,
            });
        };
        Ok(result.results)
    }
}
