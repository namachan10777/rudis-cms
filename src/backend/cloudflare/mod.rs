use std::{
    collections::HashMap,
    sync::{Arc, LazyLock},
};

use aws_config::BehaviorVersion;
use blake3::Hash;
use futures::future::try_join_all;
use image::{DynamicImage, ImageEncoder};
use indexmap::IndexMap;
use maplit::hashset;
use serde::{Deserialize, Serialize};
use valuable::Valuable;

use crate::{
    config::{FieldDef, RasterImageFormat, SetItemType},
    preprocess::{
        Document, FieldValue, Schema,
        imagetool::{self, RasterImage, VectorImage},
        rich_text::{Expanded, Extracted, Lazy, Transformed},
    },
};

pub mod d1;

struct ImageProcessed {
    data: Box<[u8]>,
    width: u32,
    height: u32,
    content_type: &'static str,
}

enum ThreadPoolRequest {
    ProcessRasterImage {
        img: Arc<DynamicImage>,
        width: u32,
        format: RasterImageFormat,
        reply: smol::channel::Sender<ImageProcessed>,
    },
}

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
enum ImageColumnContent {
    DataUrl {
        url: url::Url,
        tags: HashMap<String, serde_json::Value>,
        src_id: String,
    },
    Unknown {
        tags: HashMap<String, serde_json::Value>,
        src_id: String,
    },
    Raster {
        url: url::Url,
        tags: HashMap<String, serde_json::Value>,
        content_type: Option<String>,
        width: u32,
        height: u32,
        bucket: Option<String>,
        key: Option<String>,
        src_id: String,
        derived: Vec<ImageDerived>,
    },
    Vector {
        url: url::Url,
        tags: HashMap<String, serde_json::Value>,
        content_type: &'static str,
        width: u32,
        height: u32,
        bucket: Option<String>,
        src_id: String,
        key: Option<String>,
    },
}

struct R2UploadObject {
    key: String,
    body: Box<[u8]>,
    content_type: Option<String>,
}

struct R2UploadList {
    buckets: smol::lock::Mutex<IndexMap<String, Vec<R2UploadObject>>>,
}

impl Default for R2UploadList {
    fn default() -> Self {
        Self {
            buckets: smol::lock::Mutex::new(Default::default()),
        }
    }
}

impl R2UploadList {
    async fn register(
        &self,
        bucket: impl Into<String>,
        key: impl Into<String>,
        data: Box<[u8]>,
        content_type: Option<impl Into<String>>,
    ) {
        let bucket = bucket.into();
        let key = key.into();
        let object = R2UploadObject {
            key,
            body: data,
            content_type: content_type.map(Into::into),
        };
        self.buckets
            .lock()
            .await
            .entry(bucket.into())
            .or_default()
            .push(object);
    }
}

struct KvUploadObject {
    key: String,
    body: String,
}

struct KvUploadList {
    namespaces: smol::lock::Mutex<IndexMap<String, Vec<KvUploadObject>>>,
}

impl Default for KvUploadList {
    fn default() -> Self {
        Self {
            namespaces: smol::lock::Mutex::new(Default::default()),
        }
    }
}

impl KvUploadList {
    async fn register(
        &self,
        namespace: impl Into<String>,
        key: impl Into<String>,
        data: impl Into<String>,
    ) {
        self.namespaces
            .lock()
            .await
            .entry(namespace.into())
            .or_default()
            .push(KvUploadObject {
                key: key.into(),
                body: data.into(),
            });
    }
}

struct FieldCompileContext<'a> {
    id: &'a str,
    name: &'a str,
    kv: &'a KvUploadList,
    r2: &'a R2UploadList,
}

struct RichTextTransformContext {}

impl CloudflareBackend {
    async fn blob_to_json(
        &self,
        ctx: FieldCompileContext<'_>,
        src_id: Option<impl Into<String>>,
        data: Arc<Box<[u8]>>,
        hash: Hash,
        tags: HashMap<String, serde_json::Value>,
    ) -> Result<serde_json::Value, Error> {
        let Some(FieldDef::Blob { backend, .. }) = self.schema.schema.get(ctx.name) else {
            unreachable!()
        };
        let key = if let Some(prefix) = &backend.prefix {
            format!("{prefix}/{}{}", ctx.name, ctx.id)
        } else {
            format!("{}/{}", ctx.name, ctx.id)
        };
        let url = format!("{}/{key}", backend.zone);
        ctx.r2
            .register(
                &backend.bucket,
                &key,
                (&*data).clone(),
                Some("application/octet-stream"),
            )
            .await;
        Ok(serde_json::json!({
            "bucket": backend.bucket.clone(),
            "key": key,
            "url": url.clone(),
            "hash": hash.to_string(),
            "tags": tags,
            "src_id": src_id.map(Into::into),
        }))
    }

    async fn image_transform(
        &self,
        ctx: FieldCompileContext<'_>,
        src_id: impl Into<String>,
        image: imagetool::Image,
        tags: HashMap<String, serde_json::Value>,
    ) -> Result<ImageColumnContent, Error> {
        let Some(FieldDef::Image {
            backend, transform, ..
        }) = self.schema.schema.get(ctx.name)
        else {
            unreachable!()
        };
        match image {
            imagetool::Image::Unknown => Ok(ImageColumnContent::Unknown {
                tags: tags,
                src_id: src_id.into(),
            }),
            imagetool::Image::Raster(RasterImage {
                remote_url: Some(url),
                data,
                format,
                ..
            }) if !backend.redistribution => Ok(ImageColumnContent::Raster {
                url,
                tags: tags,
                src_id: src_id.into(),
                width: data.width(),
                height: data.height(),
                bucket: Some(backend.bucket.clone()),
                key: None,
                derived: Default::default(),
                content_type: Some(format.as_mime_str().into()),
            }),
            imagetool::Image::Vector(VectorImage {
                remote_url: Some(url),
                width,
                height,
                ..
            }) if !backend.redistribution => Ok(ImageColumnContent::Vector {
                url,
                tags: tags,
                src_id: src_id.into(),
                content_type: "image/svg+xml",
                width: width as _,
                height: height as _,
                bucket: Some(backend.bucket.clone()),
                key: None,
            }),
            imagetool::Image::Vector(VectorImage {
                raw, width, height, ..
            }) => {
                let key = if let Some(prefix) = &backend.prefix {
                    format!("{prefix}/{}/{}.svg", ctx.id, ctx.name)
                } else {
                    format!("{}/{}.svg", ctx.id, ctx.name)
                };
                let url = format!("{}/{}", backend.zone, key);
                ctx.r2
                    .register(
                        &backend.bucket,
                        &key,
                        raw.into_bytes().into_boxed_slice(),
                        Some("image/svg+xml"),
                    )
                    .await;
                Ok(ImageColumnContent::Vector {
                    url: url.parse().unwrap(),
                    tags: tags,
                    src_id: src_id.into(),
                    content_type: "image/svg+xml",
                    width: width as _,
                    height: height as _,
                    bucket: Some(backend.bucket.clone()),
                    key: Some(key),
                })
            }
            imagetool::Image::Raster(img) => {
                let (widths, formats) = if let Some(transform) = transform {
                    (transform.width.clone(), transform.format.clone())
                } else {
                    (
                        hashset![img.data.width()],
                        hashset![RasterImageFormat::Avif],
                    )
                };
                let widths = if widths.is_empty() {
                    hashset![img.data.width()]
                } else {
                    widths
                };
                let formats = if formats.is_empty() {
                    hashset![RasterImageFormat::Avif]
                } else {
                    formats
                };
                let (reply, rx) = smol::channel::bounded(1);
                self.thread_pool_tx
                    .send(ThreadPoolRequest::ProcessRasterImage {
                        img: img.data.clone(),
                        width: img.data.width(),
                        format: RasterImageFormat::Jpeg,
                        reply,
                    })
                    .await
                    .unwrap();
                let body = rx.recv().await.unwrap().data;
                let prefix = if let Some(prefix) = &backend.prefix {
                    format!("{prefix}/{}/{}", ctx.id, ctx.name)
                } else {
                    format!("{}/{}", ctx.id, ctx.name)
                };
                let key = format!("{prefix}/fallback.png");
                ctx.r2
                    .register(&backend.bucket, &key, body, Some("image/png"))
                    .await;
                let mut derived = Vec::new();
                for width in widths {
                    for format in &formats {
                        let (reply, rx) = smol::channel::bounded(1);
                        self.thread_pool_tx
                            .send(ThreadPoolRequest::ProcessRasterImage {
                                img: img.data.clone(),
                                width,
                                format: *format,
                                reply,
                            })
                            .await
                            .unwrap();
                        let ImageProcessed { width, height, .. } = rx.recv().await.unwrap();

                        let key = format!("{prefix}/{width}x{height}.{format}");
                        let url = format!("{}/{key}", backend.zone);
                        derived.push(ImageDerived {
                            width,
                            height,
                            url: url.parse().unwrap(),
                            content_type: format.as_mime_str(),
                            bucket: Some(backend.bucket.clone()),
                            key: Some(key),
                        });
                    }
                }
                Ok(ImageColumnContent::Raster {
                    url: format!("{}/{key}", backend.zone).parse().unwrap(),
                    tags: tags,
                    src_id: src_id.into(),
                    content_type: Some("image/png".into()),
                    width: img.data.width(),
                    height: img.data.height(),
                    bucket: Some(backend.bucket.clone()),
                    key: Some(key),
                    derived,
                })
            }
        }
    }

    async fn image_to_json(
        &self,
        ctx: FieldCompileContext<'_>,
        image: imagetool::Image,
        tags: HashMap<String, serde_json::Value>,
    ) -> Result<serde_json::Value, Error> {
        Ok(serde_json::to_value(self.image_transform(ctx, image, tags).await?).unwrap())
    }

    fn transform_rich_text(
        &self,
        ctx: &mut RichTextTransformContext,
        ast: Expanded<Extracted>,
    ) -> Expanded<Lazy> {
        match ast {
            Expanded::Eager {
                tag,
                attrs,
                children,
            } => Expanded::Eager {
                tag,
                attrs,
                children: children
                    .into_iter()
                    .map(|child| self.transform_rich_text(ctx, child))
                    .collect(),
            },
            Expanded::Text(text) => Expanded::Text(text),
            Expanded::Lazy { keep, children } => {
                let extracted = match keep {
                    Extracted::IsolatedLink { card } => Lazy::IsolatedLink {
                        title: card.title,
                        description: card.description,
                        image: card.image,
                        favicon: card.favicon,
                    },
                    Extracted::Raster(img) => unimplemented!(),
                    Extracted::Vector(img) => unimplemented!(),
                    Extracted::Alert { kind } => Lazy::Alert { kind },
                    Extracted::Codeblock { title, lang, lines } => {
                        Lazy::Codeblock { title, lang, lines }
                    }
                    Extracted::Heading { level, slug, attrs } => {
                        Lazy::Heading { level, slug, attrs }
                    }
                };
                Expanded::Lazy {
                    keep: extracted,
                    children: children
                        .into_iter()
                        .map(|child| self.transform_rich_text(ctx, child))
                        .collect(),
                }
            }
        }
    }

    async fn rich_text_to_json(
        &self,
        ctx: FieldCompileContext<'_>,
        ast: Transformed,
        tags: HashMap<String, serde_json::Value>,
    ) -> Result<serde_json::Value, Error> {
        for child in ast.children {}
        unimplemented!()
    }

    async fn to_json(
        &self,
        kv: &KvUploadList,
        r2: &R2UploadList,
        document: Document,
    ) -> Result<serde_json::Value, Error> {
        let fields = document.fields.into_iter().map(|(name, field)| async {
            let ctx = FieldCompileContext {
                id: &document.id,
                name: &name,
                kv,
                r2,
            };
            let field = match field {
                FieldValue::Blob { data, hash, tags } => {
                    self.blob_to_json(ctx, data, hash, tags).await
                }
                FieldValue::Boolean(b) => Ok(serde_json::Value::Bool(b)),
                FieldValue::String(s) => Ok(serde_json::Value::String(s)),
                FieldValue::Date(d) => Ok(serde_json::Value::String(d.to_string())),
                FieldValue::Datetime(dt) => Ok(serde_json::Value::String(dt.to_string())),
                FieldValue::Hash(h) => Ok(serde_json::Value::String(h.to_string())),
                FieldValue::Id(id) => Ok(serde_json::Value::String(id.to_string())),
                FieldValue::Integer(i) => Ok(serde_json::Value::Number(i.into())),
                FieldValue::Json(json) => Ok(json),
                FieldValue::Image { image, tags } => self.image_to_json(ctx, image, tags).await,
                FieldValue::RichText { ast, tags } => self.rich_text_to_json(ctx, ast, tags).await,
            };
            field.map(|field| (name, field))
        });
        let fields = try_join_all(fields).await?;
        Ok(serde_json::Value::Object(fields.into_iter().collect()))
    }
}

fn blocking(rx: async_channel::Receiver<ThreadPoolRequest>) {
    while let Ok(request) = rx.recv_blocking() {
        match request {
            ThreadPoolRequest::ProcessRasterImage {
                img,
                width,
                format,
                reply,
            } => {
                let mut buffer = Vec::new();
                let nheight =
                    (width as f32 / img.width() as f32 * img.height() as f32).round() as u32;
                let data = img.resize(width, nheight, image::imageops::FilterType::Triangle);
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
                    RasterImageFormat::Jpeg => {
                        image::codecs::jpeg::JpegEncoder::new(&mut buffer)
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
                reply
                    .send_blocking(ImageProcessed {
                        data: buffer.into_boxed_slice(),
                        width,
                        height: nheight,
                        content_type: format.as_mime_str(),
                    })
                    .unwrap();
            }
        }
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
        for _ in 0..(num_cpus::get() - 2).max(2) {
            let rx = thread_pool_rx.clone();
            std::thread::spawn(move || {
                blocking(rx);
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
