use serde::Serialize;

pub mod debug;

use crate::{
    config,
    field::{self, CompoundId, FileReference, ImageReference, markdown::compress, object_loader},
};

#[derive(Debug)]
pub enum MarkdownStorage {
    Kv {
        namespace: String,
        prefix: Option<String>,
    },
}

#[derive(Serialize)]
#[serde(tag = "type")]
pub enum MarkdownReference {
    Kv { key: String },
}

impl From<MarkdownReference> for field::MarkdownReference {
    fn from(value: MarkdownReference) -> Self {
        match value {
            MarkdownReference::Kv { key } => Self::Kv { key },
        }
    }
}

pub trait RecordBackend {
    fn push_markdown(
        &self,
        table: impl Into<String>,
        column: impl Into<String>,
        id: &CompoundId,
        storage: &MarkdownStorage,
        document: compress::RichTextDocument,
        frontmatter: serde_json::Value,
    ) -> Result<MarkdownReference, crate::ErrorDetail>;

    fn push_markdown_image(
        &self,
        table: impl Into<String>,
        column: impl Into<String>,
        id: &CompoundId,
        transform: &config::ImageTransform,
        storage: &config::ImageStorage,
        image: object_loader::Image,
    ) -> Result<ImageReference, crate::ErrorDetail>;

    fn push_image(
        &self,
        table: impl Into<String>,
        column: impl Into<String>,
        id: &CompoundId,
        transform: &config::ImageTransform,
        storage: &config::ImageStorage,
        image: object_loader::Image,
    ) -> Result<ImageReference, crate::ErrorDetail>;

    fn push_file(
        &self,
        table: impl Into<String>,
        column: impl Into<String>,
        id: &CompoundId,
        storage: &config::FileStorage,
        file: object_loader::Object,
    ) -> Result<FileReference, crate::ErrorDetail>;
}
