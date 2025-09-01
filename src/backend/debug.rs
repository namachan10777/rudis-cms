use tracing::info;

use crate::field::{self, CompoundId, FileReference};

#[derive(Default)]
pub struct DebugBackend {}

impl super::RecordBackend for DebugBackend {
    fn push_file(
        &self,
        table: impl Into<String>,
        column: impl Into<String>,
        id: &CompoundId,
        storage: &crate::config::FileStorage,
        file: crate::field::object_loader::Object,
    ) -> Result<crate::field::FileReference, crate::ErrorDetail> {
        let table = table.into();
        let column = column.into();
        info!(table, column, ?storage, ?file, "push file");
        Ok(FileReference {
            url: format!("http://localhost:8080/files/{table}/{column}/{id}")
                .parse()
                .unwrap(),
            size: file.body.len() as _,
            content_type: file.content_type,
            hash: file.hash,
        })
    }

    fn push_image(
        &self,
        table: impl Into<String>,
        column: impl Into<String>,
        id: &CompoundId,
        _: &crate::config::ImageTransform,
        storage: &crate::config::ImageStorage,
        image: crate::field::object_loader::Image,
    ) -> Result<crate::field::ImageReference, crate::ErrorDetail> {
        let table = table.into();
        let column = column.into();
        info!(table, column, ?storage, ?image, "push image");
        let (width, height) = image.body.dimensions();
        Ok(field::ImageReference {
            url: format!("http://localhost:8080/images/{table}/{column}/{id}")
                .parse()
                .unwrap(),
            width,
            height,
            blurhash: None,
            content_type: image.content_type,
            hash: image.hash,
            variants: Default::default(),
        })
    }

    fn push_markdown(
        &self,
        table: impl Into<String>,
        column: impl Into<String>,
        id: &crate::field::CompoundId,
        storage: &super::MarkdownStorage,
        document: crate::field::markdown::compress::RichTextDocument,
    ) -> Result<super::MarkdownReference, crate::ErrorDetail> {
        let table = table.into();
        let column = column.into();
        info!(table, column, ?storage, ?document, "push markdown");
        Ok(super::MarkdownReference::Kv {
            key: id.to_string(),
        })
    }

    fn push_markdown_image(
        &self,
        table: impl Into<String>,
        column: impl Into<String>,
        id: &CompoundId,
        transform: &crate::config::ImageTransform,
        storage: &crate::config::ImageStorage,
        image: crate::field::object_loader::Image,
    ) -> Result<crate::field::ImageReference, crate::ErrorDetail> {
        let table = table.into();
        let column = column.into();
        let (width, height) = image.body.dimensions();
        info!(
            table,
            column,
            ?storage,
            ?image,
            ?transform,
            "push markdown image"
        );
        Ok(field::ImageReference {
            url: format!("http://localhost:8080/images/{table}/{column}/{id}")
                .parse()
                .unwrap(),
            width,
            height,
            blurhash: None,
            content_type: image.content_type,
            hash: image.hash,
            variants: Default::default(),
        })
    }

    fn push_row(
        &self,
        table: impl Into<String>,
        id: CompoundId,
        row: indexmap::IndexMap<String, crate::field::ColumnValue>,
    ) {
        let table = table.into();
        info!(table, %id, ?row, "push row");
    }
}
