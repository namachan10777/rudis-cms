use std::{
    fmt::{Debug, Write as _},
    path::PathBuf,
};

use base64::Engine;
use itertools::{EitherOrBoth, Itertools};
use serde::{Deserialize, Serialize};

use crate::config;

pub mod markdown;
pub mod object_loader;
pub mod table;

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

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq, Hash)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum StoragePointer {
    R2 { bucket: String, key: String },
    Asset { path: PathBuf },
    Kv { namespace: String, key: String },
    Inline { content: String, base64: bool },
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq, Hash)]
pub enum StorageContent {
    Text(String),
    Bytes(Vec<u8>),
}

impl From<StorageContent> for Vec<u8> {
    fn from(value: StorageContent) -> Self {
        match value {
            StorageContent::Text(text) => text.into_bytes(),
            StorageContent::Bytes(bin) => bin,
        }
    }
}

impl From<StorageContent> for Box<[u8]> {
    fn from(value: StorageContent) -> Self {
        match value {
            StorageContent::Bytes(bytes) => bytes.into_boxed_slice(),
            StorageContent::Text(text) => text.into_bytes().into_boxed_slice(),
        }
    }
}

pub enum StorageContentRef<'a> {
    Text(&'a str),
    Bytes(&'a [u8]),
}

impl StoragePointer {
    pub(crate) fn generate_consistent_hash(&self, base_hash: blake3::Hash) -> blake3::Hash {
        let mut hasher = blake3::Hasher::new();
        hasher.update(base_hash.as_bytes());
        self.update_hash(&mut hasher);
        hasher.finalize()
    }
    pub fn update_hash(&self, hasher: &mut blake3::Hasher) {
        match self {
            StoragePointer::R2 { bucket, key } => {
                hasher.update(b"r2");
                hasher.update(bucket.as_bytes());
                hasher.update(key.as_bytes());
            }
            StoragePointer::Asset { path } => {
                hasher.update(b"asset");
                hasher.update(path.to_string_lossy().as_bytes());
            }
            StoragePointer::Kv { namespace, key } => {
                hasher.update(b"kv");
                hasher.update(namespace.as_bytes());
                hasher.update(key.as_bytes());
            }
            StoragePointer::Inline { .. } => {
                hasher.update(b"inline");
            }
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
pub struct ImageReferenceMeta {
    pub width: u32,
    pub height: u32,
    pub blurhash: Option<String>,
    pub derived_id: String,
}

mod serde_hash {
    use serde::Deserialize as _;

    pub fn deserialize<'de, D>(deserializer: D) -> Result<blake3::Hash, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use std::str::FromStr as _;
        let s = String::deserialize(deserializer)?;
        blake3::Hash::from_str(&s).map_err(serde::de::Error::custom)
    }

    pub fn serialize<S: serde::Serializer>(
        contact: &blake3::Hash,
        s: S,
    ) -> Result<S::Ok, S::Error> {
        s.serialize_str(&contact.to_string())
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
pub struct ObjectReference<M> {
    #[serde(with = "serde_hash")]
    pub hash: blake3::Hash,
    pub size: u64,
    pub content_type: String,
    pub meta: M,
    pub pointer: StoragePointer,
}

impl<'a> StorageContentRef<'a> {
    fn as_bytes(&self) -> &[u8] {
        match self {
            Self::Bytes(b) => b,
            Self::Text(t) => t.as_bytes(),
        }
    }
}

impl<M> ObjectReference<M> {
    pub fn build(
        data: StorageContentRef,
        id: &CompoundId,
        content_type: String,
        meta: M,
        storage: &config::Storage,
        suffix: Option<String>,
    ) -> Self {
        match storage {
            config::Storage::Asset { dir } => {
                let path = PathBuf::from(dir);
                let path = path.join(id.to_string());

                let path = if let Some(suffix) = suffix {
                    path.join(&suffix)
                } else {
                    path
                };

                let pointer = StoragePointer::Asset { path: path.clone() };
                let hash = pointer.generate_consistent_hash(blake3::hash(data.as_bytes()));

                ObjectReference {
                    hash,
                    size: data.as_bytes().len() as _,
                    content_type,
                    meta,
                    pointer: StoragePointer::Asset { path },
                }
            }
            config::Storage::Inline => {
                let pointer = match data {
                    StorageContentRef::Bytes(b) => StoragePointer::Inline {
                        content: base64::engine::general_purpose::STANDARD.encode(b),
                        base64: true,
                    },
                    StorageContentRef::Text(t) => StoragePointer::Inline {
                        content: t.to_string(),
                        base64: false,
                    },
                };
                let hash = pointer.generate_consistent_hash(blake3::hash(data.as_bytes()));
                ObjectReference {
                    hash,
                    size: data.as_bytes().len() as _,
                    content_type,
                    meta,
                    pointer,
                }
            }
            config::Storage::Kv { namespace, prefix } => {
                let mut key = if let Some(prefix) = prefix {
                    format!("{prefix}/{id}")
                } else {
                    id.to_string()
                };
                if let Some(suffix) = suffix {
                    write!(key, "/{suffix}").unwrap();
                }
                let pointer = StoragePointer::Kv {
                    namespace: namespace.clone(),
                    key: key.clone(),
                };
                let hash = pointer.generate_consistent_hash(blake3::hash(data.as_bytes()));
                ObjectReference {
                    hash,
                    size: data.as_bytes().len() as _,
                    content_type,
                    meta,
                    pointer,
                }
            }
            config::Storage::R2 { bucket, prefix } => {
                let mut key = if let Some(prefix) = prefix {
                    format!("{prefix}/{id}")
                } else {
                    id.to_string()
                };
                if let Some(suffix) = suffix {
                    write!(key, "/{suffix}").unwrap();
                }
                let pointer = StoragePointer::R2 {
                    bucket: bucket.clone(),
                    key: key.clone(),
                };
                let hash = pointer.generate_consistent_hash(blake3::hash(data.as_bytes()));
                ObjectReference {
                    hash,
                    size: data.as_bytes().len() as _,
                    content_type,
                    meta,
                    pointer,
                }
            }
        }
    }
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
    Image(ObjectReference<ImageReferenceMeta>),
    File(ObjectReference<()>),
    Markdown(ObjectReference<()>),
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
