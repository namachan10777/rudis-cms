use std::{collections::HashSet, path::PathBuf};

use indexmap::IndexMap;
use serde::Deserialize;

#[derive(Deserialize, Clone, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ImageStorage {
    R2 {
        zone: String,
        bucket: String,
        prefix: Option<String>,
    },
    Asset {
        zone: String,
        remote_prefix: Option<String>,
        local_dir: PathBuf,
    },
}

#[derive(Deserialize, Clone, Debug)]
pub enum FileStorage {
    R2 {
        zone: Option<String>,
        bucket: String,
        prefix: Option<String>,
    },
    Asset {
        zone: String,
        remote_prefix: Option<String>,
        local_dir: PathBuf,
    },
}

#[derive(Deserialize, Hash, PartialEq, Eq, Clone, Copy, Debug)]
#[serde(rename_all = "snake_case")]
pub enum ImageFormat {
    Jpeg,
    Png,
    Webp,
    Avif,
}

#[derive(Deserialize, Clone, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ImageTransform {
    Transform {
        width: u32,
        format: ImageFormat,
    },
    Matrix {
        widths: HashSet<u32>,
        formats: HashSet<ImageFormat>,
        default_format: ImageFormat,
    },
}

#[derive(Deserialize, Clone, Debug)]
pub struct MarkdownImageConfig {
    pub transform: ImageTransform,
    pub table: String,
    pub storage: ImageStorage,
    pub embed_svg_threshold: usize,
}

#[derive(Deserialize, Clone, Debug)]
pub struct MarkdownConfig {}

#[derive(Deserialize, Clone, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum MarkdownStorage {
    Inline,
    Kv {
        namespace: String,
        prefix: Option<String>,
    },
}

#[derive(Deserialize, Clone)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Field {
    Id,
    Hash,
    String {
        #[serde(default)]
        index: bool,
        #[serde(default)]
        required: bool,
    },
    Integer {
        #[serde(default)]
        index: bool,
        #[serde(default)]
        required: bool,
    },
    Real {
        #[serde(default)]
        index: bool,
        #[serde(default)]
        required: bool,
    },
    Boolean {
        #[serde(default)]
        index: bool,
        #[serde(default)]
        required: bool,
    },
    Date {
        #[serde(default)]
        index: bool,
        #[serde(default)]
        required: bool,
    },
    Datetime {
        #[serde(default)]
        index: bool,
        #[serde(default)]
        required: bool,
    },
    Image {
        #[serde(default)]
        required: bool,
        storage: ImageStorage,
        transform: ImageTransform,
    },
    File {
        #[serde(default)]
        required: bool,
        storage: FileStorage,
    },
    Records {
        #[serde(default)]
        index: bool,
        #[serde(default)]
        required: bool,
        inherit_ids: Vec<String>,
        schema: IndexMap<String, Field>,
        table: String,
    },
    Markdown {
        #[serde(default)]
        required: bool,
        image: MarkdownImageConfig,
        config: MarkdownConfig,
        storage: MarkdownStorage,
    },
}

#[derive(Deserialize, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum DocumentSyntax {
    Yaml,
    Toml,
    Markdown { column: String },
}

#[derive(Deserialize)]
pub struct Collection {
    pub glob: String,
    pub syntax: DocumentSyntax,
    pub table: String,
    pub schema: IndexMap<String, Field>,
}
