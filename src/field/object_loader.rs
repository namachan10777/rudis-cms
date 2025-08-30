use std::path::{Path, PathBuf};

use derive_debug::Dbg;
use image::GenericImageView as _;
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};

use crate::field::types::{AttrValue, Name};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("failed to fetch remote object ({url}): {error}")]
    FetchRemote { error: surf::Error, url: url::Url },
    #[error("failed to decode data URL ({url}): {error}")]
    DecodeDataUrl {
        error: data_url::forgiving_base64::InvalidBase64,
        url: String,
    },
    #[error("failed to read local file ({path}): {error}")]
    ReadLocal { error: std::io::Error, path: String },
    #[error("failed to canonicalize path ({path:?}): {error}")]
    CanonicalizePath {
        error: std::io::Error,
        path: PathBuf,
    },
    #[error("parent path not found ({path:?})")]
    ParentPathNotFound { path: PathBuf },
}

#[derive(Serialize, Deserialize, Dbg, Clone, PartialEq, Eq)]
pub enum Origin {
    Remote(url::Url),
    Local(String),
    DataUrl,
    Nowhere,
}

#[derive(Serialize, Deserialize, Dbg, Clone, PartialEq, Eq)]
pub struct Object {
    #[dbg(skip)]
    pub body: Box<[u8]>,
    pub derived_id: String,
    pub hash: blake3::Hash,
    pub origin: Origin,
}

async fn load_remote(url: &url::Url) -> Result<Box<[u8]>, Error> {
    let mut response = surf::get(url)
        .send()
        .await
        .map_err(|error| Error::FetchRemote {
            error,
            url: url.clone(),
        })?;
    response
        .body_bytes()
        .await
        .map(|body| body.into_boxed_slice())
        .map_err(|error| Error::FetchRemote {
            error,
            url: url.clone(),
        })
}

fn derive_id_from_path(path: &str) -> String {
    let id = path;
    let id = id.strip_prefix("./").unwrap_or(id);
    let id = id.strip_prefix("/").unwrap_or(id);
    let id = id.strip_suffix("/").unwrap_or(id);
    id.to_string()
}

fn derive_id_from_url(url: &str) -> String {
    urlencoding::encode(url).to_string()
}

pub async fn load(src: &str, document_path: Option<&Path>) -> Result<Object, Error> {
    if let Ok(url) = url::Url::parse(src) {
        if matches!(url.scheme(), "https" | "http") {
            let body = load_remote(&url).await?;
            return Ok(Object {
                hash: blake3::hash(&body),
                derived_id: derive_id_from_url(src),
                origin: Origin::Remote(url),
                body,
            });
        }
    }
    if let Ok(data) = data_url::DataUrl::process(src) {
        let (body, _) = data.decode_to_vec().map_err(|error| Error::DecodeDataUrl {
            error,
            url: src.to_string(),
        })?;
        return Ok(Object {
            hash: blake3::hash(&body),
            derived_id: derive_id_from_url(src),
            origin: Origin::DataUrl,
            body: body.into_boxed_slice(),
        });
    }

    let path = if let Some(document_path) = document_path {
        let document_path =
            document_path
                .canonicalize()
                .map_err(|error| Error::CanonicalizePath {
                    error,
                    path: document_path.to_owned(),
                })?;
        let parent_path = document_path
            .parent()
            .ok_or_else(|| Error::ParentPathNotFound {
                path: document_path.clone(),
            })?;
        parent_path.join(src)
    } else {
        PathBuf::from(src)
    };

    let body = smol::fs::read(path)
        .await
        .map_err(|error| Error::ReadLocal {
            error,
            path: src.to_owned(),
        })?
        .into_boxed_slice();
    Ok(Object {
        hash: blake3::hash(&body),
        derived_id: derive_id_from_path(src),
        origin: Origin::Local(src.to_string()),
        body,
    })
}

#[derive(Dbg, Clone)]
pub enum ImageContent {
    Raster {
        #[dbg(skip)]
        data: image::DynamicImage,
    },
    Vector {
        dimensions: (f32, f32),
        #[dbg(skip)]
        tree: SvgNode,
    },
}

impl ImageContent {
    pub fn dimensions(&self) -> (u32, u32) {
        match self {
            ImageContent::Raster { data } => data.dimensions(),
            ImageContent::Vector {
                dimensions: (w, h), ..
            } => (*w as _, *h as _),
        }
    }
}

#[derive(Dbg, Clone)]
pub struct Image {
    pub body: ImageContent,
    pub derived_id: String,
    pub hash: blake3::Hash,
    pub origin: Origin,
}

#[derive(Debug, thiserror::Error)]
pub enum ImageLoadError {
    #[error("load error: {0}")]
    Load(Error),
    #[error("decode raster image: {origin}: {error}")]
    DecodeRaster {
        origin: String,
        error: image::ImageError,
    },
    #[error("parse xml image: {origin}: {error}")]
    ParseXml {
        origin: String,
        error: roxmltree::Error,
    },
    #[error("analyze svg image: {origin}: {error}")]
    AnalyzeSvg { origin: String, error: usvg::Error },
}

#[derive(Debug, Clone)]
pub enum SvgNode {
    Node {
        name: Name,
        attrs: IndexMap<Name, AttrValue>,
        children: Vec<SvgNode>,
    },
    Text(Box<str>),
}

fn build_svg_tree<'a, 'input>(xml: roxmltree::Node<'a, 'input>) -> SvgNode {
    if let Some(text) = xml.text() {
        SvgNode::Text(text.to_owned().into_boxed_str())
    } else {
        let name: Name = xml.tag_name().name().to_owned().into();
        let attrs = xml
            .attributes()
            .map(|attr| {
                let name: Name = attr.name().to_owned().into();
                let value = if let Ok(i) = attr.value().parse::<i64>() {
                    AttrValue::Integer(i)
                } else if let Ok(b) = attr.value().parse::<bool>() {
                    AttrValue::Bool(b)
                } else if attr.value().is_empty() {
                    AttrValue::Bool(true)
                } else {
                    AttrValue::OwnedStr(attr.value().to_owned())
                };
                (name, value)
            })
            .collect();
        SvgNode::Node {
            name,
            attrs,
            children: xml.children().map(build_svg_tree).collect(),
        }
    }
}

pub async fn load_image(src: &str, document_path: Option<&Path>) -> Result<Image, ImageLoadError> {
    let object = load(src, document_path)
        .await
        .map_err(ImageLoadError::Load)?;

    match str::from_utf8(&object.body) {
        Ok(src) => {
            let size = usvg::Tree::from_data(&object.body, &usvg::Options::default())
                .map_err(|error| ImageLoadError::AnalyzeSvg {
                    origin: src.to_string(),
                    error,
                })?
                .size();
            let tree =
                roxmltree::Document::parse(src).map_err(|error| ImageLoadError::ParseXml {
                    error,
                    origin: src.to_string(),
                })?;
            let tree = build_svg_tree(tree.root());
            Ok(Image {
                body: ImageContent::Vector {
                    dimensions: (size.width(), size.height()),
                    tree,
                },
                derived_id: object.derived_id,
                hash: object.hash,
                origin: object.origin,
            })
        }
        Err(_) => Ok(Image {
            body: ImageContent::Raster {
                data: image::load_from_memory(&object.body).map_err(|error| {
                    ImageLoadError::DecodeRaster {
                        error,
                        origin: src.to_string(),
                    }
                })?,
            },
            derived_id: object.derived_id,
            hash: object.hash,
            origin: object.origin,
        }),
    }
}
