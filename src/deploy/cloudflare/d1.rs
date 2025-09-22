use serde::{Deserialize, Serialize};
use tracing::{debug, trace, warn};
use url::Url;
use valuable::Valuable;

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
struct Request<'a, P> {
    sql: &'a str,
    params: &'a [&'a P],
}

#[derive(Deserialize, Valuable)]
enum Region {
    #[serde(rename = "WNAM")]
    WesternNorthAmerica,
    #[serde(rename = "ENAM")]
    EasternNorthAmerica,
    #[serde(rename = "WEUR")]
    WesternEurope,
    #[serde(rename = "EEUR")]
    EasternEurope,
    #[serde(rename = "APAC")]
    AsiaPasific,
    #[serde(rename = "OC")]
    Oceania,
}

#[derive(Deserialize, Valuable)]
struct QueryResultMetaTimings {
    sql_duration_ms: Option<f64>,
}

#[derive(Deserialize, Valuable, Default)]
struct QueryResultMeta {
    changed_db: Option<bool>,
    changes: Option<u64>,
    duration: Option<f64>,
    last_row_id: Option<u64>,
    rows_read: Option<u64>,
    rows_written: Option<u64>,
    served_primary: Option<bool>,
    served_by_region: Option<Region>,
    size_after: Option<u64>,
    timings: Option<QueryResultMetaTimings>,
}

#[derive(Deserialize)]
struct QueryResult<R> {
    #[serde(default)]
    meta: QueryResultMeta,
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
    async fn query<
        'q,
        R: serde::de::DeserializeOwned + for<'a> sqlx::FromRow<'a, sqlx::sqlite::SqliteRow>,
        P: job::storage::sqlite::Param + sqlx::Encode<'q, sqlx::Sqlite>,
    >(
        &self,
        statement: &'q str,
        params: &'q [&'q P],
    ) -> Result<Vec<R>, Self::Error> {
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
        trace!(text = response, "D1 response");

        let mut response = serde_json::from_str::<Response<Vec<QueryResult<R>>>>(&response)
            .map_err(Error::ParseJson)?;
        if !response.success {
            warn!(
                errors = response.errors.as_value(),
                messages = response.messages.as_value(),
                "failed to execute query"
            );
            return Err(Error::QueryFailed {
                errors: response.errors,
                messages: response.messages,
            });
        }
        let Some(result) = response.result.pop() else {
            warn!(
                messages = response.messages.as_value(),
                errors = response.errors.as_value(),
                "empty query result"
            );
            return Err(Error::EmptyResult {
                errors: response.errors,
                messages: response.messages,
            });
        };
        debug!(
            messages = response.messages.as_value(),
            meta = result.meta.as_value(),
            "query succeeded"
        );
        Ok(result.results)
    }
}
