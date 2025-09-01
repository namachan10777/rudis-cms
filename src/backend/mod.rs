use indexmap::IndexMap;
use serde::Serialize;

use crate::{
    config,
    field::{
        ColumnValue, CompoundId, FileReference, ImageReference, markdown::compress, object_loader,
    },
};

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

pub trait RecordBackend {
    fn push_row(&self, table: impl Into<String>, row: IndexMap<String, ColumnValue>);

    fn push_markdown(
        &self,
        id: &CompoundId,
        storage: &MarkdownStorage,
        document: compress::RichTextDocument,
    ) -> Result<MarkdownReference, crate::ErrorDetail>;

    fn push_markdown_image(
        &self,
        table: impl Into<String>,
        transform: &config::ImageTransform,
        storage: &config::ImageStorage,
        image: object_loader::Image,
    ) -> Result<ImageReference, crate::ErrorDetail>;

    fn push_image(
        &self,
        table: impl Into<String>,
        column: impl Into<String>,
        transform: &config::ImageTransform,
        storage: &config::ImageStorage,
        image: object_loader::Image,
    ) -> Result<ImageReference, crate::ErrorDetail>;

    fn push_file(
        &self,
        table: impl Into<String>,
        column: impl Into<String>,
        storage: &config::FileStorage,
        file: object_loader::Object,
    ) -> Result<FileReference, crate::ErrorDetail>;
}
