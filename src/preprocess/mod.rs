use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{Arc, LazyLock},
};

use chrono::FixedOffset;
use futures::future::try_join_all;
use image::EncodableLayout;
use indexmap::IndexMap;
use itertools::{Either, Itertools};
use serde::Deserialize;
pub mod imagetool;
pub mod rich_text;
pub mod types;

use crate::{
    backend::Backend,
    config::{FieldDef, SetItemType},
    preprocess::rich_text::{Expanded, Extracted},
    schema::Type,
};

#[derive(derive_debug::Dbg)]
pub enum FieldValue {
    String(String),
    Boolean(bool),
    Integer(i64),
    Json(serde_json::Value),
    Datetime(chrono::DateTime<FixedOffset>),
    Date(chrono::NaiveDate),
    Blob {
        #[dbg(skip)]
        data: Arc<Box<[u8]>>,
        hash: blake3::Hash,
        tags: HashMap<String, serde_json::Value>,
    },
    Hash(blake3::Hash),
    Id(String),
    Image {
        image: imagetool::Image,
        tags: HashMap<String, serde_json::Value>,
    },
    RichText {
        ast: rich_text::Transformed,
        tags: HashMap<String, serde_json::Value>,
    },
}

#[derive(Debug)]
pub struct ImageItem {
    pub image: imagetool::Image,
    pub tags: HashMap<String, serde_json::Value>,
}

#[derive(Debug)]
pub struct BlobItem {
    pub hash: blake3::Hash,
    pub data: Arc<Box<[u8]>>,
    pub tags: HashMap<String, serde_json::Value>,
}

#[derive(Debug)]
pub enum SetField {
    String(Vec<String>),
    Boolean(Vec<bool>),
    Integer(Vec<i64>),
    Json(Vec<serde_json::Value>),
    Blob(Vec<BlobItem>),
    Image(Vec<ImageItem>),
}

#[derive(Deserialize)]
#[serde(untagged)]
enum LocalResourceFieldValue {
    WithTags {
        path: String,
        tags: HashMap<String, serde_json::Value>,
    },
    Path(String),
}

impl From<LocalResourceFieldValue> for (String, HashMap<String, serde_json::Value>) {
    fn from(value: LocalResourceFieldValue) -> Self {
        match value {
            LocalResourceFieldValue::Path(path) => (path, Default::default()),
            LocalResourceFieldValue::WithTags { path, tags } => (path, tags),
        }
    }
}

#[derive(Deserialize)]
#[serde(untagged)]
enum RichTextFieldValue {
    WithTags {
        src: String,
        tags: HashMap<String, serde_json::Value>,
    },
    Src(String),
}

impl From<RichTextFieldValue> for (String, HashMap<String, serde_json::Value>) {
    fn from(value: RichTextFieldValue) -> Self {
        match value {
            RichTextFieldValue::Src(src) => (src, Default::default()),
            RichTextFieldValue::WithTags { src, tags } => (src, tags),
        }
    }
}

pub enum DocumentType {
    Toml,
    Yaml,
    Markdown,
}

pub struct Document {
    pub fields: IndexMap<String, FieldValue>,
    pub set_fields: IndexMap<String, SetField>,
    pub id: String,
}

pub struct Schema<B: Backend> {
    pub document_type: DocumentType,
    pub schema: IndexMap<String, FieldDef<B>>,
}

static MARKDOWN_TOML_FRONTMATTER_SEPARATOR: LazyLock<regex::Regex> =
    LazyLock::new(|| regex::Regex::new(r#"\+\+\+\s*"#).unwrap());

static MARKDOWN_YAML_FRONTMATTER_SEPARATOR: LazyLock<regex::Regex> =
    LazyLock::new(|| regex::Regex::new(r#"---\s*"#).unwrap());

static WHITE: LazyLock<regex::Regex> = LazyLock::new(|| regex::Regex::new(r"^\s*$").unwrap());

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Type error: expected: {expected}, got: {got}")]
    Type {
        expected: crate::schema::Type,
        got: crate::schema::Type,
    },
    #[error("Missing required field: {0}")]
    MissingRequiredField(String),
    #[error("Load local resource error: {path:?}, {error}")]
    LoadLocalResource {
        path: PathBuf,
        error: std::io::Error,
    },
    #[error("Load remote resource error: {url}, {error}")]
    LoadRemoteResource { url: String, error: surf::Error },
    #[error("Failed to parse date: {date}: {error}")]
    DateFormat {
        date: String,
        error: chrono::ParseError,
    },
    #[error("Failed to parse datetime: {datetime}: {error}")]
    DatetimeFormat {
        datetime: String,
        error: chrono::ParseError,
    },
    #[error("Field conflict: {0}")]
    FieldConflict(String),
    #[error("Failed to parse toml: {0}")]
    ParseToml(toml::de::Error),
    #[error("Failed to parse yaml: {0}")]
    ParseYaml(serde_yaml::Error),
}

fn split_markdown_impl<'s>(src: &'s str, separator: &regex::Regex) -> Option<(&'s str, &'s str)> {
    let begin_range = separator.find_at(src, 0)?;
    let end_range = separator.find_at(src, begin_range.end() + 1)?;
    let frontmatter = &src[begin_range.end()..end_range.start()];
    let body = &src[end_range.end()..];
    if !WHITE.is_match(&src[..begin_range.start()]) {
        return None;
    }
    Some((frontmatter, body))
}

struct PreprocessContext<'a, B: Backend> {
    article_path: &'a Path,
    name: &'a str,
    def: &'a FieldDef<B>,
}

async fn preprocess_set_field<B: Backend>(
    _: PreprocessContext<'_, B>,
    def: SetItemType,
    value: Vec<serde_json::Value>,
) -> Result<SetField, Error> {
    match def {
        SetItemType::Boolean => value
            .into_iter()
            .map(|v| match v {
                serde_json::Value::Bool(b) => Ok(b),
                _ => Err(Error::Type {
                    expected: Type::Boolean,
                    got: (&v).into(),
                }),
            })
            .collect::<Result<Vec<_>, _>>()
            .map(SetField::Boolean),
        SetItemType::String => value
            .into_iter()
            .map(|v| match v {
                serde_json::Value::String(s) => Ok(s),
                _ => Err(Error::Type {
                    expected: Type::String,
                    got: (&v).into(),
                }),
            })
            .collect::<Result<Vec<_>, _>>()
            .map(SetField::String),
        SetItemType::Integer => value
            .into_iter()
            .map(|v| match v {
                serde_json::Value::Number(n) if n.is_i64() => Ok(n.as_i64().unwrap()),
                _ => Err(Error::Type {
                    expected: Type::Integer,
                    got: (&v).into(),
                }),
            })
            .collect::<Result<Vec<_>, _>>()
            .map(SetField::Integer),
    }
}

async fn preprocess_field<B: Backend>(
    ctx: PreprocessContext<'_, B>,
    value: Option<serde_json::Value>,
) -> Result<Option<Either<FieldValue, SetField>>, Error> {
    let field = match value {
        Some(field) => field,
        None => {
            if matches!(ctx.def, FieldDef::Hash {}) {
                return Ok(None);
            } else if ctx.def.is_required() {
                return Err(Error::MissingRequiredField(ctx.name.to_owned()));
            } else {
                return Ok(None);
            }
        }
    };
    match (ctx.def, field) {
        (FieldDef::Integer { .. }, serde_json::Value::Number(n)) if n.is_i64() => {
            Ok(Some(Either::Left(FieldValue::Integer(n.as_i64().unwrap()))))
        }
        (FieldDef::Json { .. }, any) => Ok(Some(Either::Left(FieldValue::Json(any)))),
        (FieldDef::String { .. }, serde_json::Value::String(s)) => {
            Ok(Some(Either::Left(FieldValue::String(s))))
        }
        (
            FieldDef::Set {
                at_least_once,
                item,
                ..
            },
            serde_json::Value::Array(values),
        ) => {
            if *at_least_once && values.is_empty() {
                return Err(Error::MissingRequiredField(ctx.name.to_owned()));
            }
            Ok(Some(Either::Right(
                preprocess_set_field(ctx, *item, values).await?,
            )))
        }
        (FieldDef::Boolean { .. }, serde_json::Value::Bool(b)) => {
            Ok(Some(Either::Left(FieldValue::Boolean(b))))
        }
        (FieldDef::Date { .. }, serde_json::Value::String(s)) => {
            let date = chrono::NaiveDate::parse_from_str(&s, "%Y-%m-%d")
                .map_err(|error| Error::DateFormat { date: s, error })?;
            Ok(Some(Either::Left(FieldValue::Date(date))))
        }
        (FieldDef::Datetime { .. }, serde_json::Value::String(s)) => {
            let datetime = chrono::DateTime::parse_from_rfc3339(&s)
                .map_err(|error| Error::DatetimeFormat { datetime: s, error })?;
            Ok(Some(Either::Left(FieldValue::Datetime(datetime))))
        }
        (FieldDef::Hash {}, _) => Err(Error::FieldConflict(ctx.name.to_owned())),
        (FieldDef::Id {}, serde_json::Value::String(id)) => {
            Ok(Some(Either::Left(FieldValue::Id(id))))
        }
        (FieldDef::Id {}, serde_json::Value::Number(id)) => {
            Ok(Some(Either::Left(FieldValue::Id(id.to_string()))))
        }
        (FieldDef::Blob { .. }, value) => {
            let got = (&value).into();
            let def = serde_json::from_value::<LocalResourceFieldValue>(value).map_err(|_| {
                Error::Type {
                    expected: Type::Object,
                    got,
                }
            })?;
            let (path, tags) = def.into();
            let data = smol::fs::read(&path)
                .await
                .map_err(|error| Error::LoadLocalResource {
                    path: PathBuf::from(path),
                    error,
                })?;
            Ok(Some(Either::Left(FieldValue::Blob {
                hash: blake3::hash(&data),
                data: Arc::new(data.into_boxed_slice()),
                tags,
            })))
        }
        (FieldDef::Image { .. }, value) => {
            let got = (&value).into();
            let def = serde_json::from_value::<LocalResourceFieldValue>(value).map_err(|_| {
                Error::Type {
                    expected: Type::Object,
                    got,
                }
            })?;
            let (path, tags) = def.into();
            Ok(Some(Either::Left(FieldValue::Image {
                image: imagetool::load_image(ctx.article_path, &path).await,
                tags,
            })))
        }
        (FieldDef::Markdown { embed_svg, .. }, value) => {
            let got = (&value).into();
            let def =
                serde_json::from_value::<RichTextFieldValue>(value).map_err(|_| Error::Type {
                    expected: Type::Object,
                    got,
                })?;
            let (src, tags) = def.into();
            let ast = rich_text::parser::markdown::parse(&src);
            let ast = rich_text::transform::transform(ctx.article_path, *embed_svg, ast).await?;
            Ok(Some(Either::Left(FieldValue::RichText { ast, tags })))
        }
        (def, value) => Err(Error::Type {
            expected: def.into(),
            got: (&value).into(),
        }),
    }
}

fn hash_recursive(hasher: &mut blake3::Hasher, ast: &Expanded<Extracted>) {
    match ast {
        Expanded::Eager { children, .. } => {
            children
                .iter()
                .for_each(|child| hash_recursive(hasher, child));
        }
        Expanded::Text(_) => {}
        Expanded::Lazy {
            keep: Extracted::Raster(img),
            children,
        } => {
            hasher.update(img.data.as_bytes());
            children
                .iter()
                .for_each(|child| hash_recursive(hasher, child));
        }
        Expanded::Lazy {
            keep: Extracted::Vector(img),
            children,
        } => {
            hasher.update(img.raw.as_bytes());
            children
                .iter()
                .for_each(|child| hash_recursive(hasher, child));
        }
        Expanded::Lazy { children, .. } => {
            children
                .iter()
                .for_each(|child| hash_recursive(hasher, child));
        }
    }
}

impl<B: Backend> Schema<B> {
    async fn preprocess_fields(
        &self,
        path: &Path,
        mut src: HashMap<String, serde_json::Value>,
    ) -> Result<(IndexMap<String, FieldValue>, IndexMap<String, SetField>), Error> {
        let fields = try_join_all(self.schema.iter().map(|(name, def)| {
            let field = src.remove(name);
            async move {
                let ctx = PreprocessContext {
                    article_path: path,
                    name,
                    def,
                };
                let field = preprocess_field(ctx, field).await?;
                Ok::<_, Error>((name.clone(), field))
            }
        }))
        .await?;
        let (fields, set_fields) = fields
            .into_iter()
            .flat_map(|(name, field)| field.map(|f| (name, f)))
            .partition_map(|(name, field)| match field {
                Either::Left(field) => Either::Left((name, field)),
                Either::Right(field) => Either::Right((name, field)),
            });
        Ok((fields, set_fields))
    }

    pub async fn preprocess_document(&self, path: &Path, src: &str) -> Result<Document, Error> {
        let mut hasher = blake3::Hasher::new();
        hasher.update(src.as_bytes());
        let (mut fields, set_fields) = match self.document_type {
            DocumentType::Toml => {
                let fields = toml::from_str(src).map_err(Error::ParseToml)?;
                self.preprocess_fields(path, fields).await
            }
            DocumentType::Yaml => {
                let fields = toml::from_str(src).map_err(Error::ParseToml)?;
                self.preprocess_fields(path, fields).await
            }
            DocumentType::Markdown => {
                let (mut fields, markdown) = if let Some((frontmatter, body)) =
                    split_markdown_impl(src, &MARKDOWN_TOML_FRONTMATTER_SEPARATOR)
                {
                    let fields: HashMap<String, serde_json::Value> =
                        toml::from_str(frontmatter).map_err(Error::ParseToml)?;
                    (fields, body)
                } else if let Some((frontmatter, body)) =
                    split_markdown_impl(src, &MARKDOWN_YAML_FRONTMATTER_SEPARATOR)
                {
                    let fields = serde_yaml::from_str(frontmatter).map_err(Error::ParseYaml)?;
                    (fields, body)
                } else {
                    (Default::default(), src)
                };
                fields.insert(
                    "body".into(),
                    serde_json::Value::String(markdown.to_owned()),
                );
                self.preprocess_fields(path, fields).await
            }
        }?;

        fields.iter().for_each(|(_, field)| match field {
            FieldValue::Blob { data, .. } => {
                hasher.update(&data.as_bytes());
            }
            FieldValue::Image { image, .. } => {
                image.hash(&mut hasher);
            }
            FieldValue::RichText { ast, .. } => {
                for node in &ast.children {
                    hash_recursive(&mut hasher, node);
                }
                for (_, node) in &ast.footnotes {
                    hash_recursive(&mut hasher, node);
                }
            }
            _ => {}
        });

        let (hash_name, _) = self
            .schema
            .iter()
            .find(|(_, def)| matches!(def, FieldDef::Hash {}))
            .ok_or_else(|| Error::MissingRequiredField("Hash".into()))?;

        fields.insert(hash_name.clone(), FieldValue::Hash(hasher.finalize()));

        Ok(Document {
            fields,
            set_fields,
            id: self
                .schema
                .iter()
                .find(|(_, def)| matches!(def, FieldDef::Id {}))
                .ok_or_else(|| Error::MissingRequiredField("Id".into()))?
                .0
                .clone(),
        })
    }
}

#[cfg(test)]
mod test {
    #[test]
    fn test_frontmatter_split() {
        let toml = r#"
+++
title = "Test document"
+++
# This is a test document with TOML frontmatter.
"#;

        let (frontmatter, body) =
            super::split_markdown_impl(toml, &*super::MARKDOWN_TOML_FRONTMATTER_SEPARATOR)
                .expect("Failed to split frontmatter");
        assert_eq!(frontmatter, "title = \"Test document\"\n");
        assert_eq!(body, "# This is a test document with TOML frontmatter.\n");

        let yaml = r#"
---
title = "Test document"
---
# This is a test document with YAML frontmatter.
"#;

        let (frontmatter, body) =
            super::split_markdown_impl(yaml, &*&super::MARKDOWN_YAML_FRONTMATTER_SEPARATOR)
                .expect("Failed to split frontmatter");
        assert_eq!(frontmatter, "title = \"Test document\"\n");
        assert_eq!(body, "# This is a test document with YAML frontmatter.\n");
    }
}
