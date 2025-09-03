use indexmap::IndexMap;
use tracing::trace;

use crate::{
    config,
    field::{
        CompoundId, FileReference, ImageReference, MarkdownReference, StoragePointer,
        markdown::compress, object_loader,
    },
    job,
};
use std::{fmt::Write, path::PathBuf};

#[derive(Debug)]
pub enum MarkdownStorage {
    Kv {
        namespace: String,
        prefix: Option<String>,
    },
}

#[derive(Default)]
pub struct UploadCollector {
    kv: crossbeam::queue::SegQueue<(blake3::Hash, job::KvUpload)>,
    r2: crossbeam::queue::SegQueue<(blake3::Hash, job::R2Upload)>,
    asset: crossbeam::queue::SegQueue<(blake3::Hash, job::AssetUpload)>,
}

pub struct Uploads {
    pub r2: IndexMap<blake3::Hash, job::R2Upload>,
    pub kv: IndexMap<blake3::Hash, job::KvUpload>,
    pub asset: IndexMap<blake3::Hash, job::AssetUpload>,
}

impl UploadCollector {
    pub(crate) fn push_markdown(
        &self,
        storage: &MarkdownStorage,
        id: &CompoundId,
        document: compress::RichTextDocument,
        frontmatter: serde_json::Value,
    ) -> MarkdownReference {
        match storage {
            MarkdownStorage::Kv { namespace, prefix } => {
                let key = if let Some(prefix) = prefix {
                    format!("{prefix}/{id}")
                } else {
                    id.to_string()
                };
                let content =
                    serde_json::to_string(&serde_json::json!({ "frontmatter": frontmatter, "root": document.root, "footnotes": document.footnotes, "sections": document.sections })).unwrap();
                let pointer = StoragePointer::Kv {
                    namespace: namespace.clone(),
                    key: key.clone(),
                };
                let hash = pointer.generate_consistent_hash(blake3::hash(content.as_bytes()));
                self.kv.push((
                    hash,
                    job::KvUpload {
                        namespace: namespace.clone(),
                        key: key.clone(),
                        content: content.clone(),
                    },
                ));
                MarkdownReference::build(
                    &key,
                    hash,
                    StoragePointer::Kv {
                        namespace: namespace.clone(),
                        key: key.clone(),
                    },
                )
            }
        }
    }

    pub(crate) fn push_image(
        &self,
        storage: &config::ImageStorage,
        id: &CompoundId,
        image: object_loader::Image,
        distinguish_by_image_id: bool,
    ) -> ImageReference {
        match storage {
            config::ImageStorage::R2 { bucket, prefix } => {
                let mut key = String::new();
                if let Some(prefix) = prefix {
                    write!(key, "{prefix}/").unwrap();
                }
                write!(key, "{id}").unwrap();
                if distinguish_by_image_id {
                    write!(key, "/{}", image.derived_id).unwrap();
                }
                let pointer = StoragePointer::R2 {
                    bucket: bucket.clone(),
                    key: key.clone(),
                };
                let hash = pointer.generate_consistent_hash(image.hash);
                self.r2.push((
                    hash,
                    job::R2Upload {
                        bucket: bucket.clone(),
                        key: key.clone(),
                        body: image.original.clone(),
                        content_type: image.content_type.clone(),
                    },
                ));
                ImageReference::build(image, hash, pointer)
            }
            config::ImageStorage::Asset { dir } => {
                let path = PathBuf::from(dir);
                let path = path.join(id.to_string());

                let path = if distinguish_by_image_id {
                    path.join(&image.derived_id)
                } else {
                    path
                };

                let pointer = StoragePointer::Asset { path: path.clone() };
                let hash = pointer.generate_consistent_hash(image.hash);

                self.asset.push((
                    hash,
                    job::AssetUpload {
                        path: path.clone(),
                        body: image.original.clone(),
                    },
                ));
                ImageReference::build(image, hash, pointer)
            }
        }
    }

    pub(crate) fn push_file(
        &self,
        storage: &config::FileStorage,
        id: &CompoundId,
        file: object_loader::Object,
    ) -> FileReference {
        match storage {
            config::FileStorage::R2 { bucket, prefix } => {
                let key = if let Some(prefix) = prefix {
                    format!("{}/{}", prefix, id)
                } else {
                    id.to_string()
                };
                let pointer = StoragePointer::R2 {
                    bucket: bucket.clone(),
                    key: key.clone(),
                };
                let hash = pointer.generate_consistent_hash(file.hash);
                self.r2.push((
                    hash,
                    job::R2Upload {
                        bucket: bucket.clone(),
                        key: key.clone(),
                        body: file.body.clone(),
                        content_type: file.content_type.clone(),
                    },
                ));
                FileReference::build(&file, hash, pointer)
            }
            config::FileStorage::Asset { dir } => {
                let path = PathBuf::from(dir);
                let path = path.join(id.to_string());

                let pointer = StoragePointer::Asset { path: path.clone() };
                let hash = pointer.generate_consistent_hash(file.hash);
                self.asset.push((
                    hash,
                    job::AssetUpload {
                        path: path.clone(),
                        body: file.body.clone(),
                    },
                ));
                FileReference::build(&file, hash, pointer)
            }
        }
    }

    pub async fn collect(self) -> Uploads {
        trace!("collect all uploads");
        let kv = self
            .kv
            .into_iter()
            .inspect(|obj| trace!(?obj, "kv"))
            .collect::<IndexMap<_, _>>();
        let r2 = self
            .r2
            .into_iter()
            .inspect(|obj| trace!(?obj, "r2"))
            .collect::<IndexMap<_, _>>();
        let asset = self
            .asset
            .into_iter()
            .inspect(|obj| trace!(?obj, "asset"))
            .collect::<IndexMap<_, _>>();
        Uploads { kv, r2, asset }
    }
}
