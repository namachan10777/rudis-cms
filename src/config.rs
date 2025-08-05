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
            RasterImageFormat::Png => "png",
            RasterImageFormat::Webp => "webp",
            RasterImageFormat::Avif => "avif",
        }
    }

    pub fn as_mime_str(&self) -> &str {
        match self {
            RasterImageFormat::Png => "image/png",
            RasterImageFormat::Webp => "image/webp",
            RasterImageFormat::Avif => "image/avif",
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
pub enum ImageTransform {
    Simple {
        width: Option<u32>,
        format: Option<RasterImageFormat>,
    },
    Matrix {
        width: Option<Vec<u32>>,
        format: Option<Vec<u32>>,
    },
}

impl ImageTransform {
    pub fn validate(&self) -> Result<(), String> {
        match self {
            ImageTransform::Simple { width, .. } => {
                if width.map(|w| w == 0).unwrap_or_default() {
                    return Err("Width must be greater than 0".into());
                }
                Ok(())
            }
            ImageTransform::Matrix { width, format } => {
                if width.as_ref().map(|w| w.len()) == Some(0) {
                    return Err("Width must not be an empty vector".into());
                }
                if format.as_ref().map(|f| f.len()) == Some(0) {
                    return Err("Format must not be an empty vector".into());
                }
                if width.iter().flatten().any(|w| *w == 0) {
                    return Err("Width values must be greater than 0".into());
                }
                Ok(())
            }
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Valuable)]
#[serde(rename_all = "kebab-case", tag = "type")]
pub enum ImageBackend {
    R2 {
        zone: String,
        bucket: String,
        prefix: Option<String>,
        transform: Option<ImageTransform>,
    },
}

impl ImageBackend {
    pub fn validate(&self) -> Result<(), String> {
        match self {
            ImageBackend::R2 {
                zone,
                bucket,
                prefix,
                transform,
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
                if let Some(transform) = transform {
                    transform.validate()?;
                }
                Ok(())
            }
        }
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
    },
    Images {
        backend: ImageBackend,
    },
    Markdown {
        #[serde(default)]
        embed_svg: bool,
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
            Attr::String { .. } => Ok(()),
            Attr::Integer { .. } => Ok(()),
            Attr::Boolean { .. } => Ok(()),
            Attr::Datetime { .. } => Ok(()),
            Attr::Id {} => Ok(()),
            Attr::Json { .. } => Ok(()),
            Attr::Image { backend, .. } => backend.validate(),
            Attr::Images { backend } => backend.validate(),
            Attr::Markdown { .. } => Ok(()),
            Attr::Set { .. } => Ok(()),
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Valuable)]
pub struct D1Config {
    pub database_id: String,
    pub table: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Valuable)]
pub struct CollectionConfig {
    pub d1: D1Config,
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
