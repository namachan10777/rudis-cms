use std::{collections::HashSet, path::Path};

use futures::future::try_join_all;
use indexmap::IndexMap;

use crate::{
    ErrorDetail,
    process_data::{
        ImageReferenceMeta, ObjectReference,
        markdown::{Node, parser::KeepRaw},
        object_loader,
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
                self.src_set.insert(url);
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

pub struct Config {
    pub(super) embed_svg_threshold: usize,
}

pub(super) enum ImageResolved {
    EmbedSvg { tree: object_loader::SvgNode },
    Reference(ObjectReference<ImageReferenceMeta>),
}

pub trait ImageUploadRegisterer {
    fn register(&self, image: object_loader::Image) -> ObjectReference<ImageReferenceMeta>;
}

impl<'a> ImageSrcExtractor<'a> {
    pub(super) async fn into_resolver(
        self,
        document_path: Option<&Path>,
        image_locator: &impl ImageUploadRegisterer,
        config: Config,
    ) -> Result<ImageResolver, ErrorDetail> {
        let tasks = self.src_set.into_iter().map(|src| async move {
            let image = object_loader::load_image(src, document_path)
                .await
                .map_err(ErrorDetail::LoadImage)?;

            match image {
                object_loader::Image {
                    body: object_loader::ImageContent::Vector { tree, size, .. },
                    hash,
                    ..
                } if size < config.embed_svg_threshold => {
                    Ok((src.to_owned(), (ImageResolved::EmbedSvg { tree }, hash)))
                }
                image => {
                    let hash = image.hash;
                    let reference = image_locator.register(image);
                    Ok((src.to_owned(), (ImageResolved::Reference(reference), hash)))
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
