use std::fmt::Debug;

use indexmap::IndexMap;
use itertools::{EitherOrBoth, Itertools};
use serde::Serialize;

use crate::field::markdown::compress::{self};

pub mod config;
pub mod markdown;
pub mod object_loader;

#[derive(Clone, Default)]
pub struct CompoundIdPrefix(Vec<(String, String)>);

#[derive(Clone)]
pub struct CompoundId {
    prefix: CompoundIdPrefix,
    id: String,
    name: String,
}

impl std::fmt::Display for CompoundId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (_, id) in &self.prefix.0 {
            write!(f, "{id}/")?;
        }
        f.write_str(&self.id)
    }
}

impl std::fmt::Debug for CompoundId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        <Self as std::fmt::Display>::fmt(self, f)
    }
}

impl CompoundId {
    pub(crate) fn try_into_prefix(
        self,
        prefix_names: impl Debug + IntoIterator<Item = String>,
    ) -> Result<CompoundIdPrefix, crate::ErrorDetail> {
        let Self { id, name, prefix } = self;
        let prefix = prefix_names
            .into_iter()
            .chain(std::iter::once(name))
            .zip_longest(
                prefix
                    .0
                    .into_iter()
                    .map(|(_, id)| id)
                    .chain(std::iter::once(id)),
            )
            .map(|pair| match pair {
                EitherOrBoth::Both(name, value) => Ok((name, value)),
                EitherOrBoth::Left(_) | EitherOrBoth::Right(_) => {
                    Err(crate::ErrorDetail::InvalidParentIdNames)
                }
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(CompoundIdPrefix(prefix))
    }

    pub(crate) fn assign_to_row(
        self,
        row: IndexMap<String, ColumnValue>,
    ) -> IndexMap<String, ColumnValue> {
        let Self { prefix, id, name } = self;
        let mut new_row = IndexMap::<String, ColumnValue>::new();
        for (name, value) in prefix.0 {
            new_row.insert(name, value.into());
        }
        new_row.insert(name, id.into());
        new_row.extend(row.into_iter());
        new_row
    }
}

impl CompoundIdPrefix {
    pub(crate) fn id(self, name: impl Into<String>, id: impl Into<String>) -> CompoundId {
        CompoundId {
            prefix: self,
            id: id.into(),
            name: name.into(),
        }
    }
}

#[derive(Clone, Serialize, Debug)]
pub struct ImageVariantLocation {
    pub url: url::Url,
    pub width: u32,
    pub height: u32,
    pub content_type: String,
}

#[derive(Clone, Serialize, Debug)]
pub struct ImageReference {
    pub url: url::Url,
    pub width: u32,
    pub height: u32,
    pub content_type: String,
    pub hash: blake3::Hash,
    pub blurhash: Option<String>,
    pub variants: Vec<ImageVariantLocation>,
}

#[derive(Serialize, Debug)]
pub struct FileReference {
    pub url: url::Url,
    pub size: u64,
    pub content_type: String,
    pub hash: blake3::Hash,
}

#[derive(Serialize, Debug)]
#[serde(tag = "type")]
pub enum MarkdownReference {
    Inline { content: compress::RichTextDocument },
    Kv { key: String },
}

#[derive(Serialize, Debug)]
pub enum ColumnValue {
    Null,
    String(String),
    Number(serde_json::Number),
    Boolean(bool),
    Object(serde_json::Map<String, serde_json::Value>),
    Date(chrono::NaiveDate),
    Datetime(chrono::NaiveDateTime),
    Array(Vec<serde_json::Value>),
    Image(ImageReference),
    File(FileReference),
    Markdown(MarkdownReference),
}

impl From<serde_json::Value> for ColumnValue {
    fn from(value: serde_json::Value) -> Self {
        match value {
            serde_json::Value::Null => Self::Null,
            serde_json::Value::Bool(b) => Self::Boolean(b),
            serde_json::Value::Number(n) => Self::Number(n),
            serde_json::Value::String(s) => Self::String(s),
            serde_json::Value::Array(arr) => Self::Array(arr),
            serde_json::Value::Object(obj) => Self::Object(obj),
        }
    }
}

impl From<String> for ColumnValue {
    fn from(value: String) -> Self {
        Self::String(value)
    }
}
