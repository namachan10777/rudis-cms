//! Image upload registerer used during Markdown processing.
//!
//! Extracted from `transform.rs` so the Markdown image-collection plumbing is
//! independent of the row-transformation pipeline.

use crate::{
    config,
    process_data::{
        CompoundId, ImageReferenceMeta, ObjectReference, StorageContentRef,
        markdown::resolver::ImageUploadRegisterer, object_loader,
    },
};

/// Collects images encountered during Markdown resolution into a queue, so
/// they can later be promoted into the surrounding image table's `RowNode`s
/// and accompanying `Upload`s.
pub(super) struct MarkdownImageUploader<'a> {
    pub(super) storage: &'a config::Storage,
    pub(super) queue: crossbeam::queue::SegQueue<(ObjectReference<ImageReferenceMeta>, Vec<u8>)>,
    pub(super) id: &'a CompoundId,
}

impl<'a> MarkdownImageUploader<'a> {
    pub(super) fn new(storage: &'a config::Storage, id: &'a CompoundId) -> Self {
        Self {
            storage,
            queue: Default::default(),
            id,
        }
    }
}

impl<'a> ImageUploadRegisterer for MarkdownImageUploader<'a> {
    fn register(&self, image: object_loader::Image) -> ObjectReference<ImageReferenceMeta> {
        let (width, height) = image.body.dimensions();
        let meta = ImageReferenceMeta {
            width,
            height,
            derived_id: image.derived_id.clone(),
            blurhash: None, // TODO
        };
        let reference = ObjectReference::build(
            StorageContentRef::Bytes(&image.original),
            self.id,
            image.content_type,
            meta,
            self.storage,
            Some(image.derived_id),
        );
        self.queue
            .push((reference.clone(), image.original.into_vec()));
        reference
    }
}
