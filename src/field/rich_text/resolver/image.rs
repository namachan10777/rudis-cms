use std::{collections::HashSet, path::Path};

use futures::future::try_join_all;
use indexmap::IndexMap;

use crate::field::{
    object_loader,
    rich_text::{Node, parser::KeepRaw},
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

pub struct ImageResolver<'s> {
    map: IndexMap<&'s str, object_loader::Image>,
}

impl<'s> ImageSrcExtractor<'s> {
    pub(super) async fn into_resolver(
        self,
        document_path: Option<&Path>,
    ) -> Result<ImageResolver<'s>, object_loader::ImageLoadError> {
        let tasks = self.src_set.into_iter().map(|src| async move {
            let image = object_loader::load_image(src, document_path).await?;
            Ok((src, image))
        });
        Ok(ImageResolver {
            map: try_join_all(tasks).await?.into_iter().collect(),
        })
    }
}

pub trait ImageLocator {}
