use std::{collections::HashSet, path::Path};

use futures::future::try_join_all;
use indexmap::IndexMap;

use crate::{
    ErrorDetail,
    backend::RecordBackend,
    config,
    field::{
        CompoundId, ImageReference,
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
    hashes: Vec<blake3::Hash>,
}

pub struct Config<'a> {
    pub(super) transform: &'a config::ImageTransform,
    pub(super) storage: &'a config::ImageStorage,
    pub(super) embed_svg_threshold: usize,
}

pub(super) enum ImageResolved {
    EmbedSvg { tree: SvgNode },
    Reference(ImageReference),
}

impl<'a> ImageSrcExtractor<'a> {
    pub(super) async fn into_resolver<R: RecordBackend>(
        self,
        document_path: Option<&Path>,
        backend: &'a R,
        table: &str,
        column: &str,
        id: &CompoundId,
        config: Config<'a>,
    ) -> Result<ImageResolver, ErrorDetail> {
        let tasks = self.src_set.into_iter().map(|src| async move {
            let image = object_loader::load_image(src, document_path)
                .await
                .map_err(ErrorDetail::LoadImage)?;

            match image {
                object_loader::Image {
                    body: ImageContent::Vector { tree, size, .. },
                    hash,
                    ..
                } if size < config.embed_svg_threshold => {
                    Ok((src.to_owned(), (ImageResolved::EmbedSvg { tree }, hash)))
                }
                image => {
                    let hash = image.hash;
                    let image = backend.push_markdown_image(
                        table,
                        column,
                        id,
                        config.transform,
                        config.storage,
                        image,
                    )?;
                    Ok((src.to_owned(), (ImageResolved::Reference(image), hash)))
                }
            }
        });
        let (map, hashes) = try_join_all(tasks)
            .await?
            .into_iter()
            .map(|(src, (resolved, hash))| ((src, resolved), hash))
            .unzip();
        Ok(ImageResolver { map, hashes })
    }
}

impl ImageResolver {
    pub(super) fn resolve(&self, src: &str) -> Option<&ImageResolved> {
        self.map.get(src)
    }

    pub(super) fn hashes(self) -> Vec<blake3::Hash> {
        self.hashes
    }
}
