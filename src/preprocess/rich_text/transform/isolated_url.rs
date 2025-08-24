use std::collections::HashMap;

use anyhow::bail;
use serde::{Deserialize, Serialize};
use tracing::{info, warn};
use url::Url;
use valuable::Valuable;

use crate::preprocess::imagetool::load_remote_image;

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct LinkCardImage {
    url: url::Url,
    width: usize,
    height: usize,
}

#[derive(Clone, Debug)]
pub struct LinkCard {
    title: String,
    description: String,
    image: Option<LinkCardImage>,
    favicon: Option<String>,
}

impl LinkCard {
    pub fn fallback(link: &str) -> Self {
        Self {
            title: link.to_string(),
            description: link.to_string(),
            image: None,
            favicon: None,
        }
    }
}

async fn resolve_link_impl(link: &str) -> Result<LinkCard, anyhow::Error> {
    let Ok(mut response) = surf::get(link).send().await else {
        bail!("failed to fetch remote content: {link}")
    };
    let Ok(html) = response.body_string().await else {
        bail!("failed to fetch remote content: {link}")
    };

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
        .collect::<HashMap<_, _>>();

    let url = Url::parse(link)?;

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
                url.host_str()
                    .map(|host| format!("{}://{host}{favicon}", url.scheme()))
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

    let hostname = Url::parse(link)
        .ok()
        .and_then(|url| url.host_str().map(ToString::to_string))
        .unwrap_or_else(|| link.to_owned());

    let image = if let Some(url) = image.and_then(|url| Url::parse(url).ok()) {
        let image = load_remote_image(url.clone()).await;
        image
            .dimensions()
            .map(|(width, height)| LinkCardImage { url, width, height })
    } else {
        None
    };

    let card = LinkCard {
        title: title
            .map(ToString::to_string)
            .unwrap_or_else(|| hostname.clone()),
        description: description
            .map(ToString::to_string)
            .unwrap_or_else(|| hostname.clone()),
        image,
        favicon,
    };
    info!(?card, "isolated link detected");

    Ok(card)
}

pub async fn resolve_link(link: &str) -> LinkCard {
    resolve_link_impl(link)
        .await
        .inspect_err(|e| {
            warn!(e = e.to_string(), "failed to resolve link");
        })
        .unwrap_or_else(|_| LinkCard::fallback(link))
}
