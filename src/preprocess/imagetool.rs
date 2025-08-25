use std::{collections::HashMap, fmt::Display, path::Path, sync::Arc};

use anyhow::Context;
use data_url::DataUrl;
use image::{DynamicImage, EncodableLayout, GenericImageView as _};
use tracing::{trace, warn};

use crate::{
    config::RasterImageFormat,
    preprocess::types::{AttrValue, Name},
};

#[derive(Clone, derive_debug::Dbg)]
pub struct RasterImage {
    pub src_id: String,
    pub data: Arc<DynamicImage>,
    pub format: RasterImageFormat,
    pub remote_url: Option<url::Url>,
}

#[derive(Clone, derive_debug::Dbg)]
pub struct VectorImage {
    pub src_id: String,
    pub raw: String,
    pub remote_url: Option<url::Url>,
    pub width: u32,
    pub height: u32,
    pub attrs: HashMap<Name, AttrValue>,
    pub content: String,
}

#[derive(Clone, derive_debug::Dbg)]
pub enum Image {
    Raster(RasterImage),
    Vector(VectorImage),
    Unknown { src_id: String },
}

impl Image {
    pub fn dimensions(&self) -> Option<(u32, u32)> {
        match self {
            Image::Raster(data) => Some(data.data.dimensions()),
            Image::Vector(svg) => Some((svg.width, svg.height)),
            _ => None,
        }
    }

    pub fn hash(&self, hasher: &mut blake3::Hasher) {
        match self {
            Self::Raster(img) => {
                hasher.update(&if img.remote_url.is_some() { [1] } else { [0] });
                hasher.update(img.data.as_bytes());
            }
            Self::Vector(svg) => {
                hasher.update(&if svg.remote_url.is_some() { [1] } else { [0] });
                hasher.update(svg.raw.as_bytes());
            }
            Self::Unknown { .. } => {
                hasher.update("unknown".as_bytes());
            }
        }
    }
}

fn format_element_with_children(node: &roxmltree::Node) -> String {
    let mut result = String::new();

    // Opening tag
    result.push('<');
    result.push_str(node.tag_name().name());

    // Attributes
    for attr in node.attributes() {
        result.push_str(&format!(" {}=\"{}\"", attr.name(), attr.value()));
    }

    if node
        .children()
        .any(|c| c.is_element() || (c.is_text() && c.text().is_some_and(|t| !t.trim().is_empty())))
    {
        result.push('>');

        // Children
        for child in node.children() {
            if child.is_element() {
                result.push_str(&format_element_with_children(&child));
            } else if child.is_text()
                && let Some(text) = child.text()
            {
                result.push_str(text);
            }
        }

        // Closing tag
        result.push_str(&format!("</{}>", node.tag_name().name()));
    } else {
        result.push_str(" />");
    }

    result
}

pub struct Svg {
    pub width: u32,
    pub height: u32,
    pub attrs: HashMap<Name, AttrValue>,
    pub content: String,
}

fn parse_svg(data: &[u8]) -> anyhow::Result<Svg> {
    let rtree = usvg::Tree::from_data(data, &usvg::Options::default())?;

    let size = rtree.size();
    let width = size.width() as u32;
    let height = size.height() as u32;

    // Parse the SVG XML to extract root attributes and inner content
    let svg_string = String::from_utf8_lossy(data);
    let doc = roxmltree::Document::parse(&svg_string)?;

    let mut attributes: HashMap<String, AttrValue> = HashMap::new();
    let mut svg_inner = String::new();

    if let Some(svg_node) = doc.descendants().find(|n| n.has_tag_name("svg")) {
        // Extract all attributes from the SVG root element
        for attr in svg_node.attributes() {
            attributes.insert(
                attr.name().into(),
                AttrValue::OwnedStr(attr.value().to_string()),
            );
        }

        // Extract inner content (everything inside the SVG tag)
        for child in svg_node.children() {
            if child.is_element() {
                svg_inner.push_str(&format_element_with_children(&child));
            } else if child.is_text()
                && let Some(text) = child.text()
            {
                svg_inner.push_str(text);
            }
        }
    }
    Ok(Svg {
        width,
        height,
        attrs: attributes.into_iter().map(|(k, v)| (k.into(), v)).collect(),
        content: svg_inner,
    })
}

async fn load_image_from_memory(
    src: impl Display,
    bytes: &[u8],
    remote_url: Option<url::Url>,
) -> Image {
    let img = image::load_from_memory(bytes);

    let format = match image::guess_format(&bytes) {
        Ok(image::ImageFormat::Avif) => RasterImageFormat::Avif,
        Ok(image::ImageFormat::Png) => RasterImageFormat::Png,
        Ok(image::ImageFormat::WebP) => RasterImageFormat::Webp,
        Ok(image::ImageFormat::Jpeg) => RasterImageFormat::Jpeg,
        Ok(format) => {
            warn!(?format, "unsupported image format. fallback to png");
            RasterImageFormat::Png
        }
        Err(e) => {
            warn!(%e, "failed to guess image format");
            RasterImageFormat::Png
        }
    };

    img.inspect_err(|e| trace!(%src, %e, "this is not raster image"))
        .map(|data| {
            Image::Raster(RasterImage {
                src_id: src.to_string(),
                data: Arc::new(data),
                format,
                remote_url: remote_url.clone(),
            })
        })
        .or_else(|_| {
            let svg = parse_svg(bytes).inspect_err(|e| trace!(%src, %e, "this is not svg"))?;
            let raw = String::from_utf8(bytes.to_vec())
                .inspect_err(|e| warn!(%src, %e, "non utf-8 svg"))?;
            Ok::<_, anyhow::Error>(Image::Vector(VectorImage {
                src_id: src.to_string(),
                raw,
                remote_url,
                width: svg.width,
                height: svg.height,
                attrs: svg.attrs,
                content: svg.content,
            }))
        })
        .unwrap_or(Image::Unknown {
            src_id: src.to_string(),
        })
}

pub async fn load_remote_image(url: url::Url) -> Image {
    let Ok(mut response) = surf::get(&url).send().await else {
        return Image::Unknown {
            src_id: url.to_string(),
        };
    };

    let Ok(body) = response
        .body_bytes()
        .await
        .inspect_err(|e| warn!(%e, "failed to fetch content body"))
    else {
        return Image::Unknown {
            src_id: url.to_string(),
        };
    };

    load_image_from_memory(&url, body.as_bytes(), Some(url.clone())).await
}

async fn load_local_image<P: AsRef<Path>>(article_path: P, src: &str) -> anyhow::Result<Image> {
    let article_path = article_path.as_ref();
    let article_path = article_path
        .canonicalize()
        .inspect_err(|e| warn!(?article_path, %e, src, "Failed to canonicalize path"))?;
    let parent = article_path
        .parent()
        .with_context(|| "parent dir not found")?;
    let data = smol::fs::read(parent.join(src))
        .await
        .inspect_err(|e| warn!(%e, src, "failed to read local image"))?;
    Ok(load_image_from_memory(src, &data, None).await)
}

pub async fn load_image<P: AsRef<Path>>(article_path: P, src: &str) -> Image {
    if src.starts_with("http://") | src.starts_with("https://") {
        let Ok(url) = src.parse() else {
            warn!(src, "failed to parsse url");
            return Image::Unknown {
                src_id: src.to_string(),
            };
        };
        load_remote_image(url).await
    } else if let Ok(data_url) = DataUrl::process(&src) {
        let Ok((data, _)) = data_url.decode_to_vec() else {
            return Image::Unknown {
                src_id: src.to_string(),
            };
        };
        load_image_from_memory(src, &data, None).await
    } else {
        load_local_image(article_path, src)
            .await
            .unwrap_or(Image::Unknown {
                src_id: src.to_string(),
            })
    }
}
