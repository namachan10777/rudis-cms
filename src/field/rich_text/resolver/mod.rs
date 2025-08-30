use std::path::Path;

use crate::field::{
    object_loader,
    rich_text::{
        Node,
        compress::{Codeblock, Heading},
        parser::{KeepRaw, RichTextDocumentRaw},
    },
};

mod footnote;
mod image;
mod link_card;

pub enum Keep {
    Heading(Heading),
    Image {
        image: object_loader::Image,
        alt: String,
        id: String,
    },
    LinkCard {
        favicon: Option<object_loader::Image>,
        image: Option<object_loader::Image>,
        title: String,
        description: String,
        href: url::Url,
    },
    Codeblock(Codeblock),
}

pub struct Footnote {
    pub id: String,
    pub reference_number: Option<usize>,
    pub content: Vec<Node<Keep>>,
}

pub struct RichTextDocument {
    pub root: Vec<Node<Keep>>,
    pub footnotes: Vec<Footnote>,
}

struct Resolvers<'s> {
    link_card: link_card::LinkCardResolver<'s>,
    image: image::ImageResolver<'s>,
    footnote: footnote::FootnoteResolver<'s>,
}

impl<'s> Resolvers<'s> {
    fn rewrite(&self, node: Node<KeepRaw>) -> Node<Keep> {
        unimplemented!()
    }
}

impl RichTextDocument {
    pub async fn resolve(
        document: RichTextDocumentRaw,
        document_path: Option<&Path>,
    ) -> Result<Self, object_loader::ImageLoadError> {
        let mut footnote_resolver = footnote::FootnoteResolver::default();
        let mut image_extractor = image::ImageSrcExtractor::default();
        let mut link_card_extractor = link_card::LinkCardExtractor::default();

        document.for_each_content(|node| footnote_resolver.analyze(node));
        document.for_each_content(|node| image_extractor.analyze(node));
        document.for_each_content(|node| link_card_extractor.analyze(node));

        let resolvers = Resolvers {
            footnote: footnote_resolver,
            image: image_extractor.into_resolver(document_path).await?,
            link_card: link_card_extractor.into_resolver().await,
        };

        unimplemented!()
    }
}
