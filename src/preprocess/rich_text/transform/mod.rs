use std::{
    collections::{HashMap, HashSet},
    path::Path,
};

use futures::future::join_all;
use maplit::hashmap;

use crate::preprocess::{
    imagetool,
    rich_text::{self, Expanded, LinkType, raw_to_expanded, transform::isolated_url::LinkCard},
};

use super::{RawExtracted, RichTextDocument, Transformed};
pub mod highlighter;
pub mod isolated_url;

pub struct TransformConfig {
    pub embed_svg: bool,
}

#[derive(Default)]
struct Extracted {
    links: HashSet<String>,
    images: HashSet<String>,
    footnotes: HashMap<String, usize>,
    footnotes_count: usize,
}

fn analyze(ctx: &mut Extracted, src: &Expanded<RawExtracted>) {
    match src {
        Expanded::Lazy {
            keep: extracted,
            children,
        } => {
            match extracted {
                RawExtracted::Image { url, .. } => {
                    ctx.images.insert(url.clone());
                }
                RawExtracted::FootnoteReference { id } => {
                    ctx.footnotes.entry(id.clone()).or_insert_with(|| {
                        ctx.footnotes_count += 1;
                        ctx.footnotes_count
                    });
                }
                _ => {}
            }
            children.iter().for_each(|child| analyze(ctx, child));
        }
        // isolated link
        Expanded::Eager { tag, children, .. } if tag.as_ref() == "p" => {
            if let &[
                Expanded::Lazy {
                    keep:
                        RawExtracted::Link {
                            link_type: LinkType::Autolink,
                            dest_url,
                            ..
                        },
                    children,
                },
            ] = &children.as_slice()
            {
                ctx.links.insert(dest_url.to_string());
                children.iter().for_each(|child| analyze(ctx, child));
            } else {
                children.iter().for_each(|child| analyze(ctx, child));
            }
        }
        Expanded::Eager { children, .. } => children.iter().for_each(|child| analyze(ctx, child)),
        Expanded::Text(_) => {}
    }
}

struct TransformContext {
    embed_svg: bool,
    links: HashMap<String, LinkCard>,
    images: HashMap<String, imagetool::Image>,
    footnote_numbers: HashMap<String, usize>,
}

impl Extracted {
    async fn resolve(self, article_path: &Path, embed_svg: bool) -> TransformContext {
        let Self {
            links,
            images,
            footnotes,
            ..
        } = self;
        let links = join_all(links.into_iter().map(|link| async move {
            let card = isolated_url::resolve_link(&link).await;
            (link, card)
        }))
        .await
        .into_iter()
        .collect();
        let images = join_all(images.into_iter().map(|src| async move {
            let image = imagetool::load_image(article_path, &src).await;
            (src.clone(), image)
        }))
        .await
        .into_iter()
        .collect();
        TransformContext {
            embed_svg,
            links,
            images,
            footnote_numbers: footnotes,
        }
    }
}

fn slugify(children: &[Expanded<RawExtracted>]) -> String {
    children
        .iter()
        .map(|child| match child {
            Expanded::Text(text) => text.to_lowercase().replace([' ', '-'], "-"),
            _ => "".to_string(),
        })
        .collect::<Vec<_>>()
        .join("-")
}

fn transform_impl(
    ctx: &TransformContext,
    tree: Expanded<RawExtracted>,
) -> Expanded<rich_text::Extracted> {
    match tree {
        Expanded::Text(text) => Expanded::Text(text),
        Expanded::Eager {
            tag,
            children,
            attrs,
        } if tag.as_ref() == "p" => {
            if let &[
                Expanded::Lazy {
                    keep:
                        RawExtracted::Link {
                            link_type: LinkType::Autolink,
                            dest_url,
                            ..
                        },
                    children: _,
                },
            ] = &children.as_slice()
            {
                let card = ctx.links.get(dest_url).expect("why?");
                Expanded::Lazy {
                    keep: rich_text::Extracted::IsolatedLink { card: card.clone() },
                    children: vec![],
                }
            } else {
                Expanded::Eager {
                    tag,
                    attrs: attrs.clone(),
                    children: children
                        .into_iter()
                        .map(|child| transform_impl(ctx, child))
                        .collect::<Vec<_>>(),
                }
            }
        }
        Expanded::Eager {
            tag,
            attrs,
            children,
        } => Expanded::Eager {
            tag,
            attrs,
            children: children
                .into_iter()
                .map(|child| transform_impl(ctx, child))
                .collect::<Vec<_>>(),
        },
        Expanded::Lazy {
            keep: extracted,
            children,
        } => match extracted {
            RawExtracted::Alert { kind } => Expanded::Lazy {
                keep: rich_text::Extracted::Alert { kind },
                children: children
                    .into_iter()
                    .map(|child| transform_impl(ctx, child))
                    .collect::<Vec<_>>(),
            },
            RawExtracted::Codeblock { meta } => {
                let mut content = String::new();
                rich_text::text_content(&mut content, &children);
                let lines = content.lines().count();
                let content = highlighter::highlight(&content, &meta.lang).unwrap_or(content);
                Expanded::Lazy {
                    keep: rich_text::Extracted::Codeblock {
                        title: meta
                            .attrs
                            .get("title")
                            .and_then(|s| s.to_str())
                            .map(|s| s.to_owned()),
                        lang: meta.lang,
                        lines,
                    },
                    children: vec![Expanded::Eager {
                        tag: "code".into(),
                        attrs: hashmap! {},
                        children: raw_to_expanded(&content),
                    }],
                }
            }
            RawExtracted::Heading { level, attrs } => {
                let extracted = rich_text::Extracted::Heading {
                    level,
                    slug: attrs
                        .get("id")
                        .and_then(|s| s.to_str())
                        .map(|s| s.to_owned())
                        .unwrap_or(slugify(&children)),
                    attrs,
                };
                Expanded::Lazy {
                    keep: extracted,
                    children: children
                        .into_iter()
                        .map(|child| transform_impl(ctx, child))
                        .collect::<Vec<_>>(),
                }
            }
            RawExtracted::FootnoteReference { id } => {
                let n = ctx
                    .footnote_numbers
                    .get(&id)
                    .map(|n| n.to_string())
                    .unwrap_or("?".into());
                Expanded::Eager {
                    tag: "sup".into(),
                    attrs: Default::default(),
                    children: vec![Expanded::Eager {
                        tag: "a".into(),
                        attrs: hashmap! {
                            "href".into() => format!("#footnote-{}", id).into(),
                            "id".into() => format!("footnote-ref-{}", id).into(),
                        },
                        children: vec![Expanded::Text(format!("[{n}]"))],
                    }],
                }
            }
            RawExtracted::Image { title, id, url } => {
                let img = match ctx.images.get(&url).cloned() {
                    Some(image) => Expanded::Lazy {
                        keep: rich_text::Extracted::Image {
                            alt: title,
                            id: Some(id),
                            image,
                        },
                        children: Default::default(),
                    },
                    None => Expanded::Lazy {
                        keep: rich_text::Extracted::Image {
                            alt: title,
                            id: Some(id),
                            image: imagetool::Image::Unknown { src_id: url },
                        },
                        children: Default::default(),
                    },
                };
                Expanded::Eager {
                    tag: "figure".into(),
                    attrs: hashmap! {},
                    children: vec![
                        img,
                        Expanded::Eager {
                            tag: "figcaption".into(),
                            attrs: hashmap! {},
                            children: children
                                .into_iter()
                                .map(|child| transform_impl(ctx, child))
                                .collect::<Vec<_>>(),
                        },
                    ],
                }
            }
            RawExtracted::Link { dest_url, .. } => Expanded::Eager {
                tag: "a".into(),
                attrs: hashmap! {
                    "href".into() => dest_url.into(),
                },
                children: children
                    .into_iter()
                    .map(|child| transform_impl(ctx, child))
                    .collect::<Vec<_>>(),
            },
        },
    }
}

pub async fn transform(
    article_path: &Path,
    embed_svg: bool,
    src: RichTextDocument<RawExtracted>,
) -> Result<Transformed, crate::preprocess::Error> {
    let mut extracted = Extracted::default();
    for child in &src.children {
        analyze(&mut extracted, child);
    }
    for child in src.footnote_definitions.values() {
        analyze(&mut extracted, child);
    }
    let ctx = extracted.resolve(article_path, embed_svg).await;
    let root = src
        .children
        .into_iter()
        .map(|child| transform_impl(&ctx, child))
        .collect::<Vec<_>>();
    let mut footnotes = src
        .footnote_definitions
        .into_iter()
        .filter_map(|(id, content)| {
            let n = *ctx.footnote_numbers.get(&id)?;
            let content = transform_impl(&ctx, content);
            Some((n, content))
        })
        .collect::<Vec<_>>();
    footnotes.sort_by_key(|(k, _)| *k);
    Ok(Transformed {
        children: root,
        footnotes,
    })
}
