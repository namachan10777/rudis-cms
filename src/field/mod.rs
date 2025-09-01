use std::fmt::Debug;

use itertools::{EitherOrBoth, Itertools};
use serde::Serialize;

pub mod config;
pub mod markdown;
pub mod object_loader;

#[derive(Clone, Default, Debug)]
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
        let Self { id, prefix, .. } = self;
        let prefix = prefix_names
            .into_iter()
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

    pub(crate) fn pairs(&self) -> impl Iterator<Item = (&str, &str)> {
        self.prefix
            .0
            .iter()
            .map(|(name, id)| (name.as_str(), id.as_str()))
            .chain(std::iter::once((self.name.as_str(), self.id.as_str())))
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

#[derive(Clone, Serialize, Debug, Hash)]
pub struct ImageVariantLocation {
    pub url: url::Url,
    pub width: u32,
    pub height: u32,
    pub content_type: String,
}

#[derive(Clone, Serialize, Debug, Hash)]
pub struct ImageReference {
    pub url: url::Url,
    pub width: u32,
    pub height: u32,
    pub content_type: String,
    pub hash: blake3::Hash,
    pub blurhash: Option<String>,
    pub variants: Vec<ImageVariantLocation>,
}

#[derive(Serialize, Debug, Hash)]
pub struct FileReference {
    pub url: url::Url,
    pub size: u64,
    pub content_type: String,
    pub hash: blake3::Hash,
}

#[derive(Serialize, Debug, Hash)]
#[serde(tag = "type")]
pub enum MarkdownReference {
    Inline { content: serde_json::Value },
    Kv { key: String },
}

#[derive(Debug, Hash)]
pub enum ColumnValue {
    Id(String),
    Hash(blake3::Hash),
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

impl Serialize for ColumnValue {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            Self::Id(id) => serializer.serialize_str(id.as_str()),
            Self::Hash(hash) => serializer.serialize_str(&hash.to_string()),
            Self::Null => serializer.serialize_unit(),
            Self::String(s) => serializer.serialize_str(s),
            Self::Number(n) => n.serialize(serializer),
            Self::Boolean(b) => serializer.serialize_bool(*b),
            Self::Object(obj) => obj.serialize(serializer),
            Self::Date(date) => date.serialize(serializer),
            Self::Datetime(datetime) => datetime.serialize(serializer),
            Self::Array(arr) => arr.serialize(serializer),
            Self::Image(image) => image.serialize(serializer),
            Self::File(file) => file.serialize(serializer),
            Self::Markdown(markdown) => markdown.serialize(serializer),
        }
    }
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
