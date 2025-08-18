use std::{collections::HashMap, sync::LazyLock};

use serde::{Deserialize, Serialize};
use valuable::Valuable;

use crate::{backend::Backend, config::FieldDef};

pub mod d1;

pub struct CloudflareBackend {}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("id not defined")]
    IdNotDefined,
    #[error("hash not defined")]
    HashNotDefined,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Valuable)]
#[serde(rename_all = "kebab-case", tag = "type")]
pub enum ImageBackendConfig {
    R2 {
        zone: String,
        bucket: String,
        prefix: Option<String>,
    },
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Valuable)]
#[serde(rename_all = "kebab-case", tag = "type")]
pub enum BlobBackendConfig {
    R2 {
        zone: String,
        bucket: String,
        prefix: Option<String>,
    },
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Valuable)]
#[serde(rename_all = "kebab-case", tag = "type")]
pub struct BackendConfig {
    database_id: String,
    table: String,
    image_table: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Valuable)]
#[serde(rename_all = "kebab-case", tag = "type")]
pub enum SetBackendConfig {
    D1 { table: String },
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Valuable)]
#[serde(rename_all = "kebab-case", tag = "type")]
pub struct RichTextBackendConfig {}

impl super::Backend for CloudflareBackend {
    type Error = Error;

    type ImageBackendConfig = ImageBackendConfig;
    type BlobBackendConfig = BlobBackendConfig;
    type SetBackendConfig = SetBackendConfig;
    type BackendConfig = BackendConfig;
    type RichTextConfig = RichTextBackendConfig;

    fn print_schema(&self) -> String {
        unimplemented!()
    }

    async fn init(
        _: BackendConfig,
        _: HashMap<String, FieldDef<Self>>,
    ) -> Result<Self, Self::Error> {
        unimplemented!()
    }

    async fn changed(
        &self,
        _: std::collections::HashMap<String, blake3::Hash>,
    ) -> Result<String, Self::Error> {
        unimplemented!()
    }

    async fn changed_image(
        &self,
        _: std::collections::HashMap<String, blake3::Hash>,
    ) -> Result<String, Self::Error> {
        unimplemented!()
    }
}

#[allow(unused)]
static SQL_TEMPLATE_DDL: LazyLock<liquid::Template> = LazyLock::new(|| {
    liquid::Parser::new()
        .parse(include_str!("./templates/ddl.sql.liquid"))
        .unwrap()
});

#[derive(Serialize, Deserialize, Valuable, Clone, Debug, PartialEq, Eq, Hash)]
pub struct ImageVariant {
    pub width: u32,
    pub height: u32,
    pub content_type: String,
    pub url: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
pub struct Image {
    pub hash: blake3::Hash,
    pub variants: Vec<ImageVariant>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
pub struct Blob {
    pub hash: blake3::Hash,
    pub content_type: String,
    pub url: String,
}

#[allow(unused)]
fn create_sql_liquid_context<B: Backend>(
    backend_config: &BackendConfig,
    schema: HashMap<String, FieldDef<B>>,
) -> Result<liquid::Object, Error> {
    let (id_name, _) = schema
        .iter()
        .find(|(_, attr)| matches!(attr, FieldDef::Id {}))
        .ok_or(Error::IdNotDefined)?;
    let (hash_name, _) = schema
        .iter()
        .find(|(_, attr)| matches!(attr, FieldDef::Hash {}))
        .ok_or(Error::HashNotDefined)?;
    let scalar_attrs = schema
        .iter()
        .filter_map(|(name, attr)| {
            let sqlite_type = match attr {
                FieldDef::Boolean { .. } => "BOOL",
                FieldDef::Datetime { .. } => "TEXT",
                FieldDef::Integer { .. } => "INTEGER",
                FieldDef::Json { .. } => "TEXT",
                FieldDef::String { .. } => "TEXT",
                FieldDef::Blob { .. } => "TEXT",
                FieldDef::Markdown { .. } => "TEXT",
                FieldDef::Image { .. } => "TEXT",
                _ => return None,
            };
            Some(liquid::object!({
                "name": name,
                "type": sqlite_type,
                "required": attr.is_required(),
                "index": attr.needs_index(),
            }))
        })
        .collect::<Vec<_>>();
    Ok(liquid::object!({
        "table_name": backend_config.table,
        "image_table_name": backend_config.image_table,
        "id_name": id_name,
        "scalar_attrs": scalar_attrs,
        "hash_name": hash_name
    }))
}
