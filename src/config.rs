use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use valuable::Valuable;

use crate::backend::{self, Backend};

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, Hash, Valuable)]
#[serde(rename_all = "kebab-case")]
pub enum RasterImageFormat {
    Png,
    Webp,
    Avif,
}

impl RasterImageFormat {
    pub fn as_str(&self) -> &str {
        match self {
            Self::Png => "png",
            Self::Webp => "webp",
            Self::Avif => "avif",
        }
    }

    pub fn as_mime_str(&self) -> &str {
        match self {
            Self::Png => "image/png",
            Self::Webp => "image/webp",
            Self::Avif => "image/avif",
        }
    }
}

impl std::fmt::Display for RasterImageFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Valuable)]
#[serde(rename_all = "kebab-case", tag = "type")]
pub struct ImageTransform {
    width: Option<u32>,
    format: Option<RasterImageFormat>,
}

impl ImageTransform {
    pub fn validate(&self) -> Result<(), String> {
        if self.width.map(|w| w == 0).unwrap_or_default() {
            return Err("Width must be greater than 0".into());
        }
        Ok(())
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Valuable)]
#[serde(rename_all = "kebab-case", tag = "type")]
pub struct RichTextImageBackend {
    zone: String,
    bucket: String,
    prefix: Option<String>,
}

impl RichTextImageBackend {
    pub fn validate(&self) -> Result<(), String> {
        if self.zone.is_empty() || self.bucket.is_empty() {
            return Err("R2 zone and bucket must not be empty".into());
        }
        if self
            .prefix
            .as_ref()
            .map(|prefix| prefix.starts_with("/") || prefix.ends_with("/"))
            .unwrap_or_default()
        {
            return Err("R2 prefix must not start or end with a slash".into());
        }
        Ok(())
    }
}

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, Hash, Valuable)]
#[serde(rename_all = "kebab-case")]
pub enum SetItemType {
    String,
    Integer,
    Boolean,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Valuable)]
pub struct RichTextImageTransforms {
    width: Option<Vec<u32>>,
    format: Option<Vec<u32>>,
}

impl RichTextImageTransforms {
    pub fn validate(&self) -> Result<(), String> {
        if let Some(widths) = &self.width
            && (widths.is_empty() || widths.contains(&0))
        {
            return Err("Width must be greater than 0".into());
        }
        if let Some(formats) = &self.format
            && formats.is_empty()
        {
            return Err("At least one format must be specified".into());
        }
        Ok(())
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct MarkdownImageConfig<I> {
    backend: I,
    transforms: RichTextImageTransforms,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "kebab-case", tag = "type")]
pub enum FieldDef<B: Backend> {
    String {
        #[serde(default)]
        required: bool,
        #[serde(default)]
        index: bool,
    },
    Integer {
        #[serde(default)]
        required: bool,
        #[serde(default)]
        index: bool,
    },
    Boolean {
        #[serde(default)]
        required: bool,
        #[serde(default)]
        index: bool,
    },
    Datetime {
        #[serde(default)]
        required: bool,
        #[serde(default)]
        index: bool,
    },
    Id {},
    Hash {},
    Json {
        #[serde(default)]
        required: bool,
    },
    Image {
        #[serde(default)]
        required: bool,
        backend: B::ImageBackendConfig,
        transform: Option<ImageTransform>,
    },
    Blob {
        #[serde(default)]
        required: bool,
        backend: B::BlobBackendConfig,
    },
    Markdown {
        #[serde(default)]
        embed_svg: bool,
        #[serde(default)]
        document_body: bool,
        #[serde(default)]
        required: bool,
        config: B::RichTextConfig,
    },
    Set {
        #[serde(default)]
        at_least_once: bool,
        item: SetItemType,
        backend: B::SetBackendConfig,
    },
}

impl<B: Backend> FieldDef<B> {
    pub fn is_required(&self) -> bool {
        match self {
            Self::Boolean { required, .. } => *required,
            Self::Datetime { required, .. } => *required,
            Self::Id {} => true,
            Self::Hash {} => true,
            Self::Image { required, .. } => *required,
            Self::Integer { required, .. } => *required,
            Self::Json { required, .. } => *required,
            Self::Markdown { required, .. } => *required,
            Self::Set { at_least_once, .. } => *at_least_once,
            Self::String { required, .. } => *required,
            Self::Blob { required, .. } => *required,
        }
    }

    pub fn needs_index(&self) -> bool {
        match self {
            Self::Boolean { index, .. } => *index,
            Self::Datetime { index, .. } => *index,
            Self::Integer { index, .. } => *index,
            Self::String { index, .. } => *index,
            _ => false,
        }
    }
}

pub trait ImageBackendConfig {
    fn validate(&self) -> Result<(), String>;
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Valuable)]
pub struct D1Config {
    pub database_id: String,
    pub table: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Valuable)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum BackendVariants {
    Cloudflare(backend::cloudflare::BackendConfig),
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct CollectionConfig {
    pub backend: BackendVariants,
    pub schema: serde_json::Value,
    pub glob: String,
}

pub type Config = HashMap<String, CollectionConfig>;
