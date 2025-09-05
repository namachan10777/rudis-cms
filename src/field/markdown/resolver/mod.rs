use indexmap::indexmap;
use std::path::Path;

use crate::{
    field::markdown::{
        Node,
        compress::{Codeblock, FootnoteReference, Heading, Image, Keep},
        parser::{KeepRaw, RichTextDocumentRaw},
        resolver::image::ImageResolved,
        text_content,
    },
    table,
};

mod codeblock;
mod footnote;
mod image;
mod link_card;

pub struct Footnote {
    pub id: String,
    pub reference_number: Option<usize>,
    pub content: Vec<Node<Keep>>,
}

pub struct RichTextDocument {
    pub root: Vec<Node<Keep>>,
    pub footnotes: Vec<Footnote>,
}

struct Resolvers<'r> {
    link_card: &'r link_card::LinkCardResolver,
    image: &'r image::ImageResolver,
    footnote: &'r footnote::FootnoteResolver,
}

fn slugify(text: &str) -> String {
    text.to_lowercase()
        .chars()
        .filter(|c| c.is_alphanumeric() || *c == '-')
        .collect()
}

impl<'r> Resolvers<'r> {
    fn rewrite(&self, node: Node<KeepRaw>) -> Node<Keep> {
        if let Some(link_card) = self.link_card.resolve(&node) {
            return Node::Lazy {
                keep: Keep::LinkCard(Box::new(link_card.clone())),
                children: Default::default(),
            };
        }
        match node {
            Node::Text(text) => Node::Text(text),
            Node::Eager {
                tag,
                attrs,
                children,
            } => Node::Eager {
                tag,
                attrs,
                children: children
                    .into_iter()
                    .map(|node| self.rewrite(node))
                    .collect(),
            },
            Node::Lazy {
                keep: KeepRaw::Alert { kind },
                children,
            } => Node::Lazy {
                keep: Keep::Alert(kind),
                children: children
                    .into_iter()
                    .map(|node| self.rewrite(node))
                    .collect(),
            },
            Node::Lazy {
                keep: KeepRaw::Codeblock { meta },
                children,
            } => {
                let mut code = String::new();
                text_content(&mut code, &children);
                let lines = code.lines().count();
                Node::Lazy {
                    children: codeblock::highlight(&code, &meta.lang),
                    keep: Keep::Codeblock(Codeblock {
                        lang: meta.lang,
                        lines,
                        title: meta
                            .attrs
                            .get("title")
                            .and_then(|v| v.to_str())
                            .map(ToString::to_string),
                    }),
                }
            }
            Node::Lazy {
                keep: KeepRaw::FootnoteReference { id },
                ..
            } => match self.footnote.resolve(&id) {
                Some((reference, content)) => Node::Lazy {
                    keep: Keep::FootnoteReference(FootnoteReference {
                        reference: Some(reference),
                        id: id.to_string(),
                        content: Some(content.to_string()),
                    }),
                    children: Default::default(),
                },
                None => Node::Lazy {
                    keep: Keep::FootnoteReference(FootnoteReference {
                        id: id.to_string(),
                        reference: None,
                        content: None,
                    }),
                    children: Default::default(),
                },
            },
            Node::Lazy {
                keep: KeepRaw::Heading { level, attrs },
                children,
            } => {
                let slug = match attrs.get("id").and_then(|id| id.to_str()) {
                    Some(id) => id.to_string(),
                    None => {
                        let mut text = String::new();
                        text_content(&mut text, &children);
                        slugify(&text)
                    }
                };
                Node::Lazy {
                    keep: Keep::Heading(Heading { level, slug }),
                    children: children
                        .into_iter()
                        .map(|node| self.rewrite(node))
                        .collect(),
                }
            }
            Node::Lazy {
                keep: KeepRaw::Image { title, id, url },
                children,
            } => {
                let img = match self.image.resolve(&url) {
                    Some(ImageResolved::Reference(reference)) => Node::Lazy {
                        keep: Keep::Image(Image {
                            storage: reference.pointer.clone(),
                            blurhash: reference.blurhash.clone(),
                            alt: title,
                            width: reference.width,
                            height: reference.height,
                            content_type: reference.content_type.clone(),
                        }),
                        children: Default::default(),
                    },
                    Some(ImageResolved::EmbedSvg { tree, .. }) => {
                        let mut node = tree.clone().into();
                        if let Node::Eager { attrs, .. } = &mut node {
                            attrs.insert("role".into(), "img".into());
                            attrs.insert("aria-label".into(), title.into());
                        }
                        node
                    }
                    None => Node::Eager {
                        tag: "img".into(),
                        attrs: indexmap! {
                            "alt".into() => title.into(),
                            "id".into() => id.into()
                        },
                        children: Default::default(),
                    },
                };
                Node::Eager {
                    tag: "figure".into(),
                    attrs: Default::default(),
                    children: vec![
                        img,
                        Node::Eager {
                            tag: "figcaption".into(),
                            attrs: Default::default(),
                            children: children
                                .into_iter()
                                .map(|node| self.rewrite(node))
                                .collect(),
                        },
                    ],
                }
            }
            Node::Lazy {
                keep:
                    KeepRaw::Link {
                        dest_url,
                        title,
                        id,
                        ..
                    },
                children,
            } => Node::Eager {
                tag: "a".into(),
                attrs: indexmap! {
                    "href".into() => dest_url.into(),
                    "title".into() => title.into(),
                    "id".into() => id.into(),
                },
                children: children
                    .into_iter()
                    .map(|node| self.rewrite(node))
                    .collect(),
            },
        }
    }
}

impl RichTextDocument {
    pub(crate) async fn resolve(
        document: RichTextDocumentRaw,
        document_path: Option<&Path>,
        uploader: &table::MarkdownImageCollector<'_>,
        embed_svg_threshold: usize,
    ) -> Result<(Self, Vec<blake3::Hash>), crate::ErrorDetail> {
        let mut footnote_resolver = footnote::FootnoteResolver::new(&document.footnotes);
        let mut image_extractor = image::ImageSrcExtractor::default();
        let mut link_card_extractor = link_card::LinkCardExtractor::default();

        document.for_each_content(|node| footnote_resolver.analyze(node));
        document.for_each_content(|node| image_extractor.analyze(node));
        document.for_each_content(|node| link_card_extractor.analyze(node));

        let config = image::Config {
            embed_svg_threshold,
        };

        let image_resolver = image_extractor
            .into_resolver(document_path, uploader, config)
            .await?;
        let link_card_resolver = link_card_extractor.into_resolver().await;
        let resolvers = Resolvers {
            footnote: &footnote_resolver,
            image: &image_resolver,
            link_card: &link_card_resolver,
        };

        let RichTextDocumentRaw { root, footnotes } = document;
        let footnotes = footnotes
            .into_iter()
            .map(|(id, node)| {
                let reference_number = footnote_resolver.resolve(&id);
                Footnote {
                    id,
                    reference_number: reference_number.map(|(n, _)| n),
                    content: node
                        .into_iter()
                        .map(|node| resolvers.rewrite(node))
                        .collect(),
                }
            })
            .collect();

        let document = RichTextDocument {
            root: root
                .into_iter()
                .map(|node| resolvers.rewrite(node))
                .collect(),
            footnotes,
        };
        Ok((document, image_resolver.hashes()))
    }
}
