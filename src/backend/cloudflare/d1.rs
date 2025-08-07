use std::collections::HashMap;

use serde::{Deserialize, Serialize, de::DeserializeOwned, forward_to_deserialize_any};
use valuable::Valuable;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("transport error: {0}")]
    Transport(surf::Error),
    #[error("failed to receive json response: {status}: {error}")]
    ReceiveJson {
        error: surf::Error,
        status: surf::StatusCode,
    },
    #[error("batch failed: {messages:?}, {errors:?}")]
    BatchFailed {
        messages: Vec<D1ResponseInfo>,
        errors: Vec<D1ResponseInfo>,
    },
    #[error("query failed")]
    QueryFailed,
    #[error("insufficient result length for index {0}")]
    ResultNotFound(usize),
    #[error("failed to deserialize record: {0}")]
    DeserializeRecord(serde_json::Error),
}

pub struct QueryBuilder {
    pub statements: Vec<String>,
    pub params: Vec<String>,
}

impl Default for QueryBuilder {
    fn default() -> Self {
        QueryBuilder {
            statements: Vec::new(),
            params: Vec::new(),
        }
    }
}

pub trait Bindable {
    fn to_string(self) -> String;
}

impl Bindable for String {
    fn to_string(self) -> String {
        self
    }
}

#[derive(Serialize)]
struct D1Request {
    sql: String,
    params: Vec<String>,
}

pub struct D1Client {
    http: surf::Client,
    endpoint: String,
    api_token: String,
}

impl D1Client {
    pub fn new(account_id: &str, datbaase_id: &str, api_token: &str) -> Self {
        Self {
            endpoint: format!(
                "https://api.cloudflare.com/client/v4/accounts/{account_id}/d1/database/{datbaase_id}/query"
            ),
            api_token: api_token.to_owned(),
            http: surf::Client::new(),
        }
    }
}

#[derive(Debug, PartialEq, Valuable, Clone)]
pub enum D1Value {
    String(String),
    Float(f64),
    Integer(i64),
    Boolean(bool),
    Null,
}

struct D1ValueVisitor;

impl<'de> serde::de::Visitor<'de> for D1ValueVisitor {
    type Value = D1Value;
    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("string, number, boolean, null")
    }

    fn visit_bool<E>(self, v: bool) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(D1Value::Boolean(v))
    }

    fn visit_borrowed_str<E>(self, v: &'de str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(D1Value::String(v.to_owned()))
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(D1Value::String(v.to_owned()))
    }

    fn visit_string<E>(self, v: String) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(D1Value::String(v))
    }

    fn visit_none<E>(self) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(D1Value::Null)
    }

    fn visit_f32<E>(self, v: f32) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(D1Value::Float(v as _))
    }

    fn visit_f64<E>(self, v: f64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(D1Value::Float(v as _))
    }

    fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(D1Value::Integer(v))
    }
}

impl<'de> Deserialize<'de> for D1Value {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_any(D1ValueVisitor)
    }
}

#[derive(Deserialize, Debug, PartialEq, Valuable, Clone)]
pub enum D1Region {
    WNAM,
    ENAM,
    WEUR,
    EEUR,
    APAC,
    OC,
}

#[derive(Deserialize, Debug, PartialEq, Valuable, Clone)]
pub struct D1Pointer {
    pub pointer: Option<String>,
}

#[derive(Deserialize, Debug, PartialEq, Valuable, Clone)]
pub struct D1ResponseInfo {
    pub code: u32,
    pub message: String,
    pub documentation_url: Option<String>,
    pub source: Option<D1Pointer>,
}

/// Various durations for the query.
#[derive(Deserialize, Debug, PartialEq, Valuable, Clone)]
pub struct D1QueryResultMetaTimings {
    /// The duration of the SQL query execution inside the database. Does not include any network communication.
    pub sql_duration_ms: Option<usize>,
}

#[derive(Deserialize, Debug, PartialEq, Valuable, Clone)]
pub struct D1QueryResultMeta {
    /// Denotes if the database has been altered in some way, like deleting rows.
    pub changed_db: Option<bool>,
    /// Rough indication of how many rows were modified by the query, as provided by SQLite's `sqlite4_total_changes()`.
    pub changes: Option<usize>,
    /// The duration of the SQL query execution inside the database. Does not include any network communication.
    pub duration: Option<f64>,
    /// The row ID of the last inserted row in a table with an `INTEGER PRIMARY KEY` as provided by SQLite. Tables created with `WITHOUT ROWID` do not populate this.
    pub last_row_id: Option<i64>,
    /// Number of rows read during the SQL query execution, including indices (not all rows are necessarily returned).
    pub rows_read: Option<usize>,
    /// Number of rows written during the SQL query execution, including indices.
    pub rows_written: Option<usize>,
    /// Denotes if the query has been handled by the database primary instance.
    pub served_by_primary: Option<bool>,
    /// Region location hint of the database instance that handled the query.
    pub served_by_region: Option<D1Region>,
    /// Size of the database after the query committed, in bytes.
    pub size_after: Option<usize>,
    /// Various durations for the query.
    pub timings: Option<D1QueryResultMetaTimings>,
}

#[derive(Deserialize, Debug, PartialEq, Valuable)]
pub struct D1QueryResult {
    pub meta: Option<D1QueryResultMeta>,
    pub results: Option<Vec<HashMap<String, D1Value>>>,
    pub success: Option<bool>,
}

#[derive(Deserialize, Debug, PartialEq, Valuable)]
pub struct D1Response {
    pub errors: Vec<D1ResponseInfo>,
    pub messages: Vec<D1ResponseInfo>,
    pub results: Vec<D1QueryResult>,
    /// Whether the API call was successful
    pub success: bool,
}

impl From<D1Value> for serde_json::Value {
    fn from(value: D1Value) -> Self {
        match value {
            D1Value::Boolean(b) => serde_json::Value::Bool(b),
            D1Value::Null => serde_json::Value::Null,
            D1Value::Integer(i) => serde_json::Value::Number(serde_json::Number::from(i)),
            D1Value::Float(f) => {
                serde_json::Value::Number(serde_json::Number::from_f64(f).unwrap())
            }
            D1Value::String(s) => serde_json::Value::String(s),
        }
    }
}

impl<'de> serde::de::Deserializer<'de> for D1Value {
    type Error = serde_json::Error;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        match self {
            D1Value::Null => visitor.visit_none(),
            D1Value::Boolean(b) => visitor.visit_bool(b),
            D1Value::Integer(i) => visitor.visit_i64(i),
            D1Value::Float(f) => visitor.visit_f64(f),
            D1Value::String(s) => visitor.visit_string(s),
        }
    }

    fn deserialize_enum<V>(
        self,
        name: &'static str,
        variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        serde_json::Value::from(self).deserialize_enum(name, variants, visitor)
    }

    fn deserialize_struct<V>(
        self,
        name: &'static str,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        match self {
            Self::String(s) => serde_json::from_str::<serde_json::Value>(&s)?
                .deserialize_struct(name, fields, visitor),
            _ => Err(serde::de::Error::custom("expected string")),
        }
    }

    fn deserialize_ignored_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        serde_json::Value::from(self).deserialize_ignored_any(visitor)
    }

    forward_to_deserialize_any! {
        bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string
        bytes byte_buf option unit unit_struct newtype_struct seq tuple
        tuple_struct map identifier
    }
}

pub fn deserialize_records<D>(
    results: &Vec<HashMap<String, D1Value>>,
) -> Result<Vec<D>, serde_json::Error>
where
    D: DeserializeOwned,
{
    // FIXME
    results
        .into_iter()
        .map(|row| {
            serde_json::from_value::<D>(serde_json::Value::Object(
                row.iter()
                    .map(|(key, value)| (key.clone(), serde_json::Value::from(value.clone())))
                    .collect(),
            ))
        })
        .collect::<Result<Vec<D>, serde_json::Error>>()
}

impl QueryBuilder {
    pub fn stmt<S: Into<String>>(mut self, query: S) -> Self {
        self.statements.push(query.into());
        self
    }

    pub fn bind<B: Bindable>(mut self, bindable: B) -> Self {
        self.params.push(bindable.to_string());
        self
    }

    pub async fn run(self, client: &D1Client) -> Result<D1Response, Error> {
        let req = D1Request {
            sql: self.statements.join(";"),
            params: self.params,
        };

        let mut response = client
            .http
            .post(&client.endpoint)
            .header("Authorization", format!("Bearer {}", client.api_token))
            .header("Content-Type", "application/json")
            .body_json(&req)
            .map_err(Error::Transport)?
            .send()
            .await
            .map_err(Error::Transport)?;

        let status = response.status();
        let body: D1Response = response
            .body_json()
            .await
            .map_err(|error| Error::ReceiveJson { error, status })?;
        Ok(body)
    }
}

impl D1QueryResult {
    pub fn deserialize<D: DeserializeOwned>(&self) -> Result<Vec<D>, Error> {
        if self.success.unwrap_or_default() {
            return Err(Error::QueryFailed);
        }
        let Some(records) = &self.results else {
            return Ok(Default::default());
        };
        deserialize_records(records).map_err(Error::DeserializeRecord)
    }
}

impl Error {
    pub fn fill_error_details(self, response: &D1Response) -> Error {
        match self {
            Error::QueryFailed => Error::BatchFailed {
                messages: response.messages.clone(),
                errors: response.errors.clone(),
            },
            _ => self,
        }
    }
}

impl D1Response {
    pub fn deserialize_result<D: DeserializeOwned>(&self, index: usize) -> Result<Vec<D>, Error> {
        let Some(result) = self.results.get(index) else {
            return Err(Error::ResultNotFound(index));
        };
        result
            .deserialize()
            .map_err(|e| e.fill_error_details(&self))
    }

    pub fn check(&self) -> Result<(), Error> {
        if !self.success {
            Err(Error::BatchFailed {
                messages: self.messages.clone(),
                errors: self.errors.clone(),
            })
        } else {
            Ok(())
        }
    }
}
