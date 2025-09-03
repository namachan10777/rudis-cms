use std::collections::HashSet;

use futures::future::join_all;
use indexmap::IndexMap;
use tracing::warn;
use url::Url;

use crate::field::{
    markdown::{
        LinkType, Node,
        compress::{LinkCard, LinkCardImage},
        parser::KeepRaw,
    },
    object_loader,
};

#[derive(Default)]
pub(super) struct LinkCardExtractor<'s> {
    links: HashSet<&'s str>,
}

fn extract_isolated_link(node: &Node<KeepRaw>) -> Option<&str> {
    match node {
        Node::Eager { tag, children, .. } => {
            if tag.as_ref() != "p" {
                return None;
            }
            if children.len() > 1 {
                return None;
            }
            let Some(Node::Lazy {
                keep:
                    KeepRaw::Link {
                        link_type: LinkType::Autolink,
                        dest_url,
                        ..
                    },
                ..
            }) = children.iter().next()
            else {
                return None;
            };
            Some(dest_url)
        }
        _ => None,
    }
}

impl<'s> LinkCardExtractor<'s> {
    pub(super) fn analyze(&mut self, node: &'s Node<KeepRaw>) {
        match node {
            Node::Text(_) => {}
            Node::Lazy { children, .. } => children.iter().for_each(|node| self.analyze(node)),
            eager @ Node::Eager { children, .. } => {
                if let Some(link) = extract_isolated_link(eager) {
                    self.links.insert(link);
                } else {
                    children.iter().for_each(|node| self.analyze(node));
                }
            }
        }
    }
}

pub(super) struct LinkCardResolver {
    links: IndexMap<String, LinkCard>,
}

async fn load_image(src: &str) -> Option<LinkCardImage> {
    let url = Url::parse(src).ok()?;
    let image = object_loader::load_image(src, None).await.ok()?;
    let (width, height) = image.body.dimensions();
    Some(LinkCardImage {
        src: url,
        width,
        height,
        content_type: image.content_type,
    })
}

async fn resolve_link_card(link: &str) -> Result<LinkCard, anyhow::Error> {
    let response = reqwest::Client::new()
        .get(link)
        .header("Accept", "text/html")
        .send()
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    let html = response.text().await.map_err(|e| anyhow::anyhow!("{e}"))?;

    let doc = scraper::Html::parse_document(&html);
    let og_selector = scraper::Selector::parse(r#"meta[property^="og:"]"#).unwrap();
    let twitter_selector = scraper::Selector::parse(r#"meta[name^="twitter:"]"#).unwrap();
    let title_selector = scraper::Selector::parse(r#"title"#).unwrap();
    let description_selector = scraper::Selector::parse(r#"meta[name="description"]"#).unwrap();
    let icon_selector = scraper::Selector::parse(r#"link[rel^="icon"]"#).unwrap();

    let meta_props = doc
        .select(&og_selector)
        .flat_map(|meta| {
            let name = meta.value().attr("property");
            let value = meta.value().attr("content");
            match (name, value) {
                (Some(name), Some(value)) => Some((name, value)),
                _ => None,
            }
        })
        .chain(doc.select(&twitter_selector).flat_map(|meta| {
            let name = meta.value().attr("name");
            let value = meta.value().attr("content");
            match (name, value) {
                (Some(name), Some(value)) => Some((name, value)),
                _ => None,
            }
        }))
        .collect::<IndexMap<_, _>>();

    let href = url::Url::parse(link)?;

    let title = doc
        .select(&title_selector)
        .next()
        .map(|tag| tag.text().collect::<Vec<_>>().join(""));
    let description = doc
        .select(&description_selector)
        .next()
        .map(|tag| tag.text().collect::<Vec<_>>().join(""));
    let favicon = doc
        .select(&icon_selector)
        .next()
        .and_then(|tag| tag.attr("href"))
        .and_then(|favicon| {
            if favicon.starts_with("http://") || favicon.starts_with("https://") {
                Some(favicon.to_string())
            } else {
                href.host_str()
                    .map(|host| format!("{}://{host}{favicon}", href.scheme()))
            }
        });

    let title = meta_props
        .get("og:title")
        .or_else(|| meta_props.get("twitter:title"))
        .copied()
        .or(title.as_deref());

    let description = meta_props
        .get("og:description")
        .or_else(|| meta_props.get("twitter:description"))
        .copied()
        .or(description.as_deref());

    let image = meta_props
        .get("og:image")
        .or_else(|| meta_props.get("twitter:image"))
        .copied()
        .or(favicon.as_deref());

    let hostname = url::Url::parse(link)
        .ok()
        .and_then(|url| url.host_str().map(ToString::to_string))
        .unwrap_or_else(|| link.to_owned());

    let og_image = if let Some(image) = image {
        load_image(image).await
    } else {
        None
    };

    let favicon = if let Some(favicon) = favicon {
        load_image(&favicon).await
    } else {
        None
    };

    Ok(LinkCard {
        title: title
            .map(ToOwned::to_owned)
            .unwrap_or_else(|| hostname.clone()),
        description: description
            .map(ToOwned::to_owned)
            .unwrap_or_else(|| hostname.clone()),
        og_image,
        favicon,
        href,
    })
}

impl<'s> LinkCardExtractor<'s> {
    pub(super) async fn into_resolver(self) -> LinkCardResolver {
        let tasks = self.links.into_iter().map(|link| async move {
            let card = resolve_link_card(link)
                .await
                .inspect_err(|e| warn!(%e, "failed to resolve isolated link"))
                .ok()?;
            Some((link.to_owned(), card))
        });
        LinkCardResolver {
            links: join_all(tasks).await.into_iter().flatten().collect(),
        }
    }
}

impl LinkCardResolver {
    pub(super) fn resolve(&self, node: &Node<KeepRaw>) -> Option<&LinkCard> {
        extract_isolated_link(node).and_then(|href| self.links.get(href))
    }
}
