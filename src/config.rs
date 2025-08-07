use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use valuable::Valuable;

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
pub enum ImageBackend {
    R2 {
        zone: String,
        bucket: String,
        prefix: Option<String>,
    },
}

impl ImageBackend {
    pub fn validate(&self) -> Result<(), String> {
        match self {
            Self::R2 {
                zone,
                bucket,
                prefix,
            } => {
                if zone.is_empty() || bucket.is_empty() {
                    return Err("R2 zone and bucket must not be empty".into());
                }
                if prefix
                    .as_ref()
                    .map(|prefix| prefix.starts_with("/") || prefix.ends_with("/"))
                    .unwrap_or_default()
                {
                    return Err("R2 prefix must not start or end with a slash".into());
                }
                Ok(())
            }
        }
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
        if let Some(widths) = &self.width {
            if widths.is_empty() || widths.iter().any(|&w| w == 0) {
                return Err("Width must be greater than 0".into());
            }
        }
        if let Some(formats) = &self.format {
            if formats.is_empty() {
                return Err("At least one format must be specified".into());
            }
        }
        Ok(())
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Valuable)]
pub struct MarkdownImageConfig {
    backend: ImageBackend,
    transforms: RichTextImageTransforms,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Valuable)]
#[serde(rename_all = "kebab-case", tag = "type")]
pub enum Attr {
    String {
        #[serde(default)]
        required: bool,
    },
    Integer {
        #[serde(default)]
        required: bool,
    },
    Boolean {
        #[serde(default)]
        required: bool,
    },
    Datetime {
        #[serde(default)]
        required: bool,
    },
    Id {},
    Json {
        #[serde(default)]
        required: bool,
    },
    Image {
        #[serde(default)]
        required: bool,
        backend: ImageBackend,
        transform: Option<ImageTransform>,
    },
    Markdown {
        #[serde(default)]
        embed_svg: bool,
        image: MarkdownImageConfig,
    },
    Set {
        #[serde(default)]
        at_least_once: bool,
        item: SetItemType,
    },
}

impl Attr {
    pub fn validate(&self) -> Result<(), String> {
        match self {
            Self::String { .. } => Ok(()),
            Self::Integer { .. } => Ok(()),
            Self::Boolean { .. } => Ok(()),
            Self::Datetime { .. } => Ok(()),
            Self::Id {} => Ok(()),
            Self::Json { .. } => Ok(()),
            Self::Image {
                backend, transform, ..
            } => {
                if let Some(transform) = transform {
                    transform.validate()?;
                }
                backend.validate()
            }
            Self::Markdown { .. } => Ok(()),
            Self::Set { .. } => Ok(()),
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Valuable)]
pub struct D1Config {
    pub database_id: String,
    pub table: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Valuable)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Backend {
    Cloudflare {
        database_id: String,
        table: String,
        image_table: String,
    },
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Valuable)]
pub struct CollectionConfig {
    pub backend: Backend,
    pub schema: HashMap<String, Attr>,
}

impl CollectionConfig {
    pub fn validate(&self) -> Result<(), String> {
        for (name, attr) in &self.schema {
            if name.is_empty() {
                return Err("Attribute names must not be empty".into());
            }
            attr.validate()?;
        }
        Ok(())
    }
}

pub type Config = HashMap<String, CollectionConfig>;
