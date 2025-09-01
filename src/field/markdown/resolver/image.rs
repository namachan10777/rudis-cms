use std::{collections::HashSet, path::Path};

use futures::future::try_join_all;
use indexmap::IndexMap;

use crate::{
    ErrorDetail,
    backend::RecordBackend,
    config,
    field::{
        ImageReference,
        markdown::{Node, parser::KeepRaw},
        object_loader::{self, ImageContent, SvgNode},
    },
};

#[derive(Default)]
pub(super) struct ImageSrcExtractor<'s> {
    src_set: HashSet<&'s str>,
}

impl<'s> ImageSrcExtractor<'s> {
    pub(super) fn analyze(&mut self, node: &'s Node<KeepRaw>) {
        match node {
            Node::Eager { children, .. } => children.iter().for_each(|node| self.analyze(node)),
            Node::Lazy {
                keep: KeepRaw::Image { url, .. },
                children,
            } => {
                self.src_set.insert(&*url);
                children.iter().for_each(|node| self.analyze(node));
            }
            Node::Lazy { children, .. } => children.iter().for_each(|node| self.analyze(node)),
            Node::Text(_) => {}
        }
    }
}

pub struct ImageResolver {
    map: IndexMap<String, ImageResolved>,
}

pub struct Config<'a> {
    pub(super) transform: &'a config::ImageTransform,
    pub(super) storage: &'a config::ImageStorage,
    pub(super) embed_svg_threshold: usize,
}

pub(super) enum ImageResolved {
    EmbedSvg {
        dimensions: (f32, f32),
        tree: SvgNode,
    },
    Reference(ImageReference),
}

impl<'a> ImageSrcExtractor<'a> {
    pub(super) async fn into_resolver<R: RecordBackend>(
        self,
        document_path: Option<&Path>,
        backend: &'a R,
        table: &str,
        config: Config<'a>,
    ) -> Result<ImageResolver, ErrorDetail> {
        let tasks = self.src_set.into_iter().map(|src| async move {
            let image = object_loader::load_image(src, document_path)
                .await
                .map_err(ErrorDetail::LoadImage)?;

            match image {
                object_loader::Image {
                    body:
                        ImageContent::Vector {
                            dimensions,
                            tree,
                            size,
                        },
                    ..
                } if size < config.embed_svg_threshold => {
                    Ok((src.to_owned(), ImageResolved::EmbedSvg { dimensions, tree }))
                }
                image => {
                    let image = backend.push_markdown_image(
                        table,
                        config.transform,
                        config.storage,
                        image,
                    )?;
                    Ok((src.to_owned(), ImageResolved::Reference(image)))
                }
            }
        });
        Ok(ImageResolver {
            map: try_join_all(tasks).await?.into_iter().collect(),
        })
    }
}

impl ImageResolver {
    pub(super) fn resolve(&self, src: &str) -> Option<&ImageResolved> {
        self.map.get(src)
    }
}
