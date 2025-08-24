use std::{
    collections::HashMap,
    sync::{Arc, LazyLock},
};

use aws_config::BehaviorVersion;
use serde::{Deserialize, Serialize};
use valuable::Valuable;

use crate::{
    config::{FieldDef, SetItemType},
    preprocess::{Document, Schema},
};

pub mod d1;

enum ThreadPoolRequest {}

pub struct CloudflareBackend {
    schema: Arc<Schema<Self>>,
    config: BackendConfig,
    r2: aws_sdk_s3::Client,
    d1: d1::D1Client,
    thread_pool_tx: async_channel::Sender<ThreadPoolRequest>,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("id not defined")]
    IdNotDefined,
    #[error("hash not defined")]
    HashNotDefined,
    #[error("env var {0} not found")]
    EnvVarNotFound(&'static str),
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Valuable)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum ImageBackendConfig {
    R2 {
        zone: String,
        bucket: String,
        prefix: Option<String>,
    },
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Valuable)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum BlobBackendConfig {
    R2 {
        zone: String,
        bucket: String,
        prefix: Option<String>,
    },
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Valuable)]
#[serde(rename_all = "snake_case", tag = "type")]
pub struct BackendConfig {
    database_id: String,
    table: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Valuable)]
#[serde(rename_all = "snake_case", tag = "type")]
pub struct SetBackendConfig {
    table: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Valuable)]
#[serde(rename_all = "snake_case", tag = "type")]
pub struct RichTextBackendConfig {
    pub image_table: String,
}

#[allow(unused)]
static SQL_TEMPLATE_DDL: LazyLock<liquid::Template> = LazyLock::new(|| {
    liquid::ParserBuilder::with_stdlib()
        .build()
        .unwrap()
        .parse(include_str!("./templates/ddl.sql.liquid"))
        .unwrap()
});

struct TransactionSideEffects {}

impl CloudflareBackend {
    async fn to_json(&self, document: Document) -> serde_json::Value {
        unimplemented!()
    }
}

fn blocking_task(rx: async_channel::Receiver<ThreadPoolRequest>) {
    unimplemented!()
}

impl super::Backend for CloudflareBackend {
    type Error = Error;

    type ImageBackendConfig = ImageBackendConfig;
    type BlobBackendConfig = BlobBackendConfig;
    type SetBackendConfig = SetBackendConfig;
    type BackendConfig = BackendConfig;
    type RichTextConfig = RichTextBackendConfig;

    fn print_schema(&self) -> String {
        let liquid_ctx = create_sql_liquid_context(&self.config, &self.schema.schema).unwrap();
        SQL_TEMPLATE_DDL.render(&liquid_ctx).unwrap()
    }

    async fn init(config: BackendConfig, schema: Arc<Schema<Self>>) -> Result<Self, Self::Error> {
        let account_id =
            std::env::var("CF_ACCOUNT_ID").map_err(|_| Error::EnvVarNotFound("CF_ACCOUNT_ID"))?;
        let access_key_id = std::env::var("R2_ACCESS_KEY_ID")
            .map_err(|_| Error::EnvVarNotFound("R2_ACCESS_KEY_ID"))?;
        let access_key_secret = std::env::var("R2_ACCESS_KEY_SECRET")
            .map_err(|_| Error::EnvVarNotFound("R2_ACCESS_KEY_SECRET"))?;
        let api_token =
            std::env::var("CF_API_TOKEN").map_err(|_| Error::EnvVarNotFound("CF_API_TOKEN"))?;

        let aws_config = aws_config::defaults(BehaviorVersion::latest())
            .endpoint_url(format!("https://{}.r2.cloudflarestorage.com", account_id))
            .credentials_provider(aws_sdk_s3::config::Credentials::new(
                access_key_id,
                access_key_secret,
                None,
                None,
                "R2",
            ))
            .region("auto")
            .load()
            .await;

        let r2 = aws_sdk_s3::Client::new(&aws_config);
        let d1 = d1::D1Client::new(&account_id, &config.database_id, &api_token);
        let (thread_pool_tx, thread_pool_rx) = async_channel::unbounded();

        for _ in 0..(num_cpus::get() - 2).max(1) {
            let thread_pool_rx = thread_pool_rx.clone();
            std::thread::spawn(move || {
                blocking_task(thread_pool_rx);
            });
        }

        Ok(Self {
            schema,
            config,
            r2,
            d1,
            thread_pool_tx,
        })
    }

    async fn batch(&self, documents: Vec<crate::preprocess::Document>) -> Result<(), Self::Error> {
        unimplemented!()
    }
}

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
fn create_sql_liquid_context(
    backend_config: &BackendConfig,
    schema: &HashMap<String, FieldDef<CloudflareBackend>>,
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
    let mut set_attrs = schema
        .iter()
        .filter_map(|(name, attr)| match attr {
            FieldDef::Set {
                item,
                backend,
                column_name,
                ..
            } => {
                let sqlite_type = match item {
                    SetItemType::Boolean => "INTEGER",
                    SetItemType::Integer => "INTEGER",
                    SetItemType::String => "TEXT",
                };
                let name = column_name.as_ref().unwrap_or(name);
                Some(liquid::object!({
                    "name": name,
                    "type": sqlite_type,
                    "table": backend.table,
                }))
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    let rich_text_images = schema.iter().filter_map(|(_, value)| match value {
        FieldDef::Markdown {
            embed_svg,
            document_body,
            required,
            config,
        } => Some(liquid::object!({
            "name": "image",
            "type": "TEXT",
            "table": config.image_table,
        })),
        _ => None,
    });
    set_attrs.extend(rich_text_images);
    Ok(liquid::object!({
        "table_name": backend_config.table,
        "id_name": id_name,
        "scalar_attrs": scalar_attrs,
        "set_attrs": set_attrs,
        "hash_name": hash_name
    }))
}
