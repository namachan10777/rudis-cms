use std::{
    collections::HashMap,
    sync::{Arc, LazyLock},
};

use aws_config::BehaviorVersion;
use blake3::Hash;
use futures::future::try_join_all;
use image::ImageEncoder;
use indexmap::IndexMap;
use maplit::hashset;
use serde::{Deserialize, Serialize};
use valuable::Valuable;

use crate::{
    config::{FieldDef, RasterImageFormat, SetItemType},
    preprocess::{Document, FieldValue, Schema, imagetool, rich_text::Transformed},
};

pub mod d1;

enum ThreadPoolRequest {}

struct R2Upload {
    body: Box<[u8]>,
    content_type: &'static str,
    key: String,
    bucket: String,
}

struct KvUpload {}

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
pub struct ImageBackendConfig {
    zone: String,
    bucket: String,
    prefix: Option<String>,
    #[serde(default)]
    redistribution: bool,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Valuable)]
#[serde(rename_all = "snake_case", tag = "type")]
pub struct BlobBackendConfig {
    zone: String,
    bucket: String,
    prefix: Option<String>,
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

#[derive(Default)]
pub struct Uploads {
    r2_blobs: Vec<R2Upload>,
    r2_images: Vec<R2Upload>,
    kv_documents: Vec<KvUpload>,
}

#[derive(Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
struct ImageDerived {
    width: u32,
    height: u32,
    url: String,
    content_type: &'static str,
    bucket: Option<String>,
    key: Option<String>,
}

#[derive(Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ImageUploaded {
    DataUrl {
        url: url::Url,
        tags: HashMap<String, serde_json::Value>,
    },
    Unknown {
        tags: HashMap<String, serde_json::Value>,
    },
    Raster {
        url: url::Url,
        tags: HashMap<String, serde_json::Value>,
        content_type: String,
        width: u32,
        height: u32,
        bucket: Option<String>,
        key: Option<String>,
        derived: Vec<ImageDerived>,
    },
    Vector {
        url: url::Url,
        tags: HashMap<String, serde_json::Value>,
        content_type: &'static str,
        width: u32,
        height: u32,
        bucket: Option<String>,
        key: Option<String>,
    },
}

impl CloudflareBackend {
    async fn blob_to_json(
        &self,
        uploads: &smol::lock::Mutex<Uploads>,
        id: &str,
        name: &str,
        data: Arc<Box<[u8]>>,
        hash: Hash,
        tags: HashMap<String, serde_json::Value>,
    ) -> Result<serde_json::Value, Error> {
        let Some(FieldDef::Blob { backend, .. }) = self.schema.schema.get(name) else {
            unreachable!()
        };
        let key = if let Some(prefix) = &backend.prefix {
            format!("{prefix}/{}{name}", id)
        } else {
            format!("{}/{name}", id)
        };
        let url = format!("{}/{key}", backend.zone);
        uploads.lock().await.r2_blobs.push(R2Upload {
            body: (&*data).clone(),
            content_type: "application/octet-stream",
            key: key.clone(),
            bucket: backend.bucket.clone(),
        });
        Ok(serde_json::json!({
            "bucket": backend.bucket.clone(),
            "key": key.clone(),
            "url": url.clone(),
            "hash": hash.to_string(),
            "tags": tags,
        }))
    }

    async fn image_transform(
        &self,
        uploads: &smol::lock::Mutex<Uploads>,
        id: &str,
        name: &str,
        image: imagetool::Image,
        tags: HashMap<String, serde_json::Value>,
    ) -> Result<ImageUploaded, Error> {
        let Some(FieldDef::Image {
            backend, transform, ..
        }) = self.schema.schema.get(name)
        else {
            unreachable!()
        };
        match image {
            imagetool::Image::Data { url } => Ok(ImageUploaded::DataUrl { url, tags }),
            imagetool::Image::Unknown => Ok(ImageUploaded::Unknown { tags }),
            imagetool::Image::Raster {
                remote_url: Some((url, content_type)),
                data,
            } if !backend.redistribution => Ok(ImageUploaded::Raster {
                url,
                tags,
                content_type,
                width: data.width(),
                height: data.height(),
                bucket: Some(backend.bucket.clone()),
                key: None,
                derived: Default::default(),
            }),
            imagetool::Image::Svg {
                remote_url: Some(url),
                width,
                height,
                ..
            } if !backend.redistribution => Ok(ImageUploaded::Vector {
                url,
                tags,
                content_type: "image/svg+xml",
                width: width as _,
                height: height as _,
                bucket: Some(backend.bucket.clone()),
                key: None,
            }),
            imagetool::Image::Svg {
                raw, width, height, ..
            } => {
                let key = if let Some(prefix) = &backend.prefix {
                    format!("{prefix}/{id}/{name}.svg")
                } else {
                    format!("{id}/{name}.svg")
                };
                let url = format!("{}/{}", backend.zone, key);
                uploads.lock().await.r2_images.push(R2Upload {
                    body: raw.into_bytes().into_boxed_slice(),
                    content_type: "image/svg+xml",
                    key: key.clone(),
                    bucket: backend.bucket.clone(),
                });
                Ok(ImageUploaded::Vector {
                    url: url.parse().unwrap(),
                    tags,
                    content_type: "image/svg+xml",
                    width: width as _,
                    height: height as _,
                    bucket: Some(backend.bucket.clone()),
                    key: Some(key),
                })
            }
            imagetool::Image::Raster { data, .. } => {
                let (widths, formats) = if let Some(transform) = transform {
                    (transform.width.clone(), transform.format.clone())
                } else {
                    (hashset![data.width()], hashset![RasterImageFormat::Avif])
                };
                let widths = if widths.is_empty() {
                    hashset![data.width()]
                } else {
                    widths
                };
                let formats = if formats.is_empty() {
                    hashset![RasterImageFormat::Avif]
                } else {
                    formats
                };
                // png fallback
                let mut png = Vec::new();
                image::codecs::png::PngEncoder::new(&mut png)
                    .write_image(
                        data.as_bytes(),
                        data.width(),
                        data.height(),
                        image::ExtendedColorType::Rgba8,
                    )
                    .unwrap();
                let prefix = if let Some(prefix) = &backend.prefix {
                    format!("{prefix}/{id}/{name}")
                } else {
                    format!("{id}/{name}")
                };
                let key = format!("{prefix}/fallback.png");
                uploads.lock().await.r2_images.push(R2Upload {
                    body: png.into_boxed_slice(),
                    content_type: "image/png",
                    key: key.clone(),
                    bucket: backend.bucket.clone(),
                });
                let mut derived = Vec::new();
                for width in widths {
                    for format in &formats {
                        let mut buffer = Vec::new();
                        let nheight = (width as f32 / data.width() as f32 * data.height() as f32)
                            .round() as u32;
                        let data =
                            data.resize(width, nheight, image::imageops::FilterType::Triangle);
                        match format {
                            RasterImageFormat::Avif => {
                                image::codecs::avif::AvifEncoder::new(&mut buffer)
                                    .write_image(
                                        data.as_bytes(),
                                        width,
                                        nheight,
                                        image::ExtendedColorType::Rgba8,
                                    )
                                    .unwrap();
                            }
                            RasterImageFormat::Png => {
                                image::codecs::png::PngEncoder::new(&mut buffer)
                                    .write_image(
                                        data.as_bytes(),
                                        width,
                                        nheight,
                                        image::ExtendedColorType::Rgba8,
                                    )
                                    .unwrap();
                            }
                            RasterImageFormat::Webp => {
                                image::codecs::webp::WebPEncoder::new_lossless(&mut buffer)
                                    .write_image(
                                        data.as_bytes(),
                                        width,
                                        nheight,
                                        image::ExtendedColorType::Rgba8,
                                    )
                                    .unwrap();
                            }
                        }
                        let content_type = match format {
                            RasterImageFormat::Png => "image/png",
                            RasterImageFormat::Webp => "image/webp",
                            RasterImageFormat::Avif => "image/avif",
                        };
                        let key = format!("{prefix}/{width}x{nheight}.{format}");
                        let url = format!("{}/{key}", backend.zone);
                        derived.push(ImageDerived {
                            width,
                            height: nheight,
                            url: url.parse().unwrap(),
                            content_type,
                            bucket: Some(backend.bucket.clone()),
                            key: Some(key),
                        });
                    }
                }
                Ok(ImageUploaded::Raster {
                    url: format!("{}/{key}", backend.zone).parse().unwrap(),
                    tags,
                    content_type: "image/png".into(),
                    width: data.width(),
                    height: data.height(),
                    bucket: Some(backend.bucket.clone()),
                    key: Some(key),
                    derived,
                })
            }
        }
    }

    async fn image_to_json(
        &self,
        uploads: &smol::lock::Mutex<Uploads>,
        id: &str,
        name: &str,
        image: imagetool::Image,
        tags: HashMap<String, serde_json::Value>,
    ) -> Result<serde_json::Value, Error> {
        Ok(
            serde_json::to_value(self.image_transform(uploads, id, name, image, tags).await?)
                .unwrap(),
        )
    }

    async fn rich_text_to_json(
        &self,
        uploads: &smol::lock::Mutex<Uploads>,
        id: &str,
        name: &str,
        ast: Transformed,
        tags: HashMap<String, serde_json::Value>,
    ) -> Result<serde_json::Value, Error> {
        unimplemented!()
    }

    async fn to_json(
        &self,
        uploads: &smol::lock::Mutex<Uploads>,
        document: Document,
    ) -> Result<serde_json::Value, Error> {
        let fields = document.fields.into_iter().map(|(name, field)| async {
            let field = match field {
                FieldValue::Blob { data, hash, tags } => {
                    self.blob_to_json(uploads, &document.id, &name, data, hash, tags)
                        .await
                }
                FieldValue::Boolean(b) => Ok(serde_json::Value::Bool(b)),
                FieldValue::String(s) => Ok(serde_json::Value::String(s)),
                FieldValue::Date(d) => Ok(serde_json::Value::String(d.to_string())),
                FieldValue::Datetime(dt) => Ok(serde_json::Value::String(dt.to_string())),
                FieldValue::Hash(h) => Ok(serde_json::Value::String(h.to_string())),
                FieldValue::Id(id) => Ok(serde_json::Value::String(id.to_string())),
                FieldValue::Integer(i) => Ok(serde_json::Value::Number(i.into())),
                FieldValue::Json(json) => Ok(json),
                FieldValue::Image { image, tags } => {
                    self.image_to_json(uploads, &document.id, &name, image, tags)
                        .await
                }
                FieldValue::RichText { ast, tags } => {
                    self.rich_text_to_json(uploads, &document.id, &name, ast, tags)
                        .await
                }
            };
            field.map(|field| (name, field))
        });
        let fields = try_join_all(fields).await?;
        Ok(serde_json::Value::Object(fields.into_iter().collect()))
    }
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
    schema: &IndexMap<String, FieldDef<CloudflareBackend>>,
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
