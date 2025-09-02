use derive_debug::Dbg;
use tracing::trace;

use crate::{
    config,
    field::{
        CompoundId, FileReference, ImageReference, MarkdownReference, StoragePointer,
        markdown::compress, object_loader,
    },
};
use std::{fmt::Write, path::PathBuf};

#[derive(Debug)]
pub enum MarkdownStorage {
    Kv {
        namespace: String,
        prefix: Option<String>,
    },
}

#[derive(Dbg)]
pub struct KvObject {
    pub namespace: String,
    pub key: String,
    pub content: String,
}

#[derive(Dbg)]
pub struct R2Object {
    pub bucket: String,
    pub key: String,
    #[dbg(skip)]
    pub body: Box<[u8]>,
    pub content_type: String,
}

#[derive(Dbg)]
pub struct AssetObject {
    pub path: PathBuf,
    #[dbg(skip)]
    pub body: Box<[u8]>,
}

#[derive(Default)]
pub struct Uploads {
    local_mock: bool,
    kv: crossbeam::queue::SegQueue<KvObject>,
    r2: crossbeam::queue::SegQueue<R2Object>,
    asset: crossbeam::queue::SegQueue<AssetObject>,
}

impl Uploads {
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
                self.kv.push(KvObject {
                    namespace: namespace.clone(),
                    key: key.clone(),
                    content,
                });
                MarkdownReference::Kv {
                    key: key.clone(),
                    pointer: StoragePointer::Kv {
                        namespace: namespace.clone(),
                        key,
                    },
                }
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
                self.r2.push(R2Object {
                    bucket: bucket.clone(),
                    key: key.clone(),
                    body: image.original.clone(),
                    content_type: image.content_type.clone(),
                });
                let pointer = StoragePointer::R2 {
                    bucket: bucket.clone(),
                    key: key.clone(),
                };
                ImageReference::build(image, pointer)
            }
            config::ImageStorage::Asset { dir } => {
                let path = PathBuf::from(dir);
                let path = path.join(id.to_string());

                let path = if distinguish_by_image_id {
                    path.join(&image.derived_id)
                } else {
                    path
                };

                self.asset.push(AssetObject {
                    path: path.clone(),
                    body: image.original.clone(),
                });
                let pointer = StoragePointer::Asset { path };
                ImageReference::build(image, pointer)
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
                self.r2.push(R2Object {
                    bucket: bucket.clone(),
                    key: key.clone(),
                    body: file.body.clone(),
                    content_type: file.content_type.clone(),
                });
                let pointer = StoragePointer::R2 {
                    bucket: bucket.clone(),
                    key: key.clone(),
                };
                FileReference::build(&file, pointer)
            }
            config::FileStorage::Asset { dir } => {
                let path = PathBuf::from(dir);
                let path = path.join(id.to_string());

                self.asset.push(AssetObject {
                    path: path.clone(),
                    body: file.body.clone(),
                });
                let pointer = StoragePointer::Asset { path };
                FileReference::build(&file, pointer)
            }
        }
    }

    pub async fn collect(self) {
        trace!("collect all uploads");
        let kv = self.kv.into_iter().collect::<Vec<_>>();
        let r2 = self.r2.into_iter().collect::<Vec<_>>();
        let asset = self.asset.into_iter().collect::<Vec<_>>();
        dbg!(kv);
        dbg!(r2);
        dbg!(asset);
    }
}
