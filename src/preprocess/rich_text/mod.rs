use std::{collections::HashMap, hash::Hash, sync::Arc};

use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use tracing::warn;
use valuable::Valuable;

use crate::preprocess::rich_text::transform::isolated_url::LinkCard;
use crate::preprocess::types::{AttrValue, Name};
use codeblock::meta_parser::CodeblockMeta;

pub mod codeblock;
pub mod parser;
pub mod transform;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Valuable)]
#[serde(rename_all = "snake_case")]
pub enum AlertKind {
    Caution,
    Important,
    Note,
    Warning,
    Tip,
}

#[derive(Serialize, Deserialize, Debug, Clone, Valuable)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum LinkType {
    Autolink,
    Wikilink,
    Normal,
    Email,
    Broken,
}

#[derive(Debug, Clone, Valuable)]
pub enum RawExtracted {
    FootnoteReference {
        id: String,
    },
    Alert {
        kind: AlertKind,
    },
    Codeblock {
        meta: CodeblockMeta,
    },
    Heading {
        level: u8,
        attrs: HashMap<Name, AttrValue>,
    },
    Image {
        title: String,
        id: String,
        url: String,
    },
    Link {
        link_type: LinkType,
        dest_url: String,
        title: String,
        id: String,
    },
}

#[derive(derive_debug::Dbg)]
pub enum Extracted {
    IsolatedLink {
        card: LinkCard,
    },
    RasterImage {
        #[dbg(skip)]
        data: Arc<image::DynamicImage>,
        alt: String,
        id: String,
    },
    VectorImage {
        width: u32,
        height: u32,
        raw: String,
        attrs: HashMap<Name, AttrValue>,
    },
    Codeblock {
        title: Option<String>,
        lang: Option<String>,
        lines: usize,
    },
    Alert {
        kind: AlertKind,
    },
    Heading {
        level: u8,
        slug: String,
        attrs: HashMap<Name, AttrValue>,
    },
}

#[derive(Debug, Clone)]
pub enum Expanded<E> {
    Eager {
        tag: Name,
        attrs: HashMap<Name, AttrValue>,
        children: Vec<Expanded<E>>,
    },
    Text(String),
    Lazy {
        extracted: E,
        children: Vec<Expanded<E>>,
    },
}

pub(crate) fn raw_to_expanded<E>(src: &str) -> Vec<Expanded<E>> {
    match html_parser::Dom::parse(&src) {
        Ok(dom) => {
            return dom
                .children
                .into_iter()
                .map(|element| element.into())
                .collect();
        }
        Err(e) => {
            warn!(%e, "failed to parse html");
            vec![Expanded::Text(src.to_string())]
        }
    }
}

impl<E> From<html_parser::Node> for Expanded<E> {
    fn from(value: html_parser::Node) -> Self {
        match value {
            html_parser::Node::Comment(_) => Expanded::Text("".to_string()),
            html_parser::Node::Element(html_parser::Element {
                id,
                name,
                children,
                attributes,
                classes,
                ..
            }) => {
                let mut attrs = attributes
                    .into_iter()
                    .map(|(name, value)| match value {
                        Some(value) => (name.into(), value.into()),
                        None => (name.into(), AttrValue::Bool(true)),
                    })
                    .collect::<HashMap<Name, AttrValue>>();
                if let Some(id) = id {
                    attrs.insert("id".into(), id.into());
                }
                if !classes.is_empty() {
                    attrs.insert("class".into(), classes.join(" ").into());
                }
                let children = children.into_iter().map(Into::into).collect();
                Self::Eager {
                    tag: name.into(),
                    attrs,
                    children,
                }
            }
            html_parser::Node::Text(text) => Expanded::Text(text),
        }
    }
}

#[derive(Debug, Clone)]
pub struct RichTextDocument<E> {
    pub children: Vec<Expanded<E>>,
    pub footnote_definitions: IndexMap<String, Expanded<E>>,
}

#[derive(Debug)]
pub struct Transformed {
    pub children: Vec<Expanded<Extracted>>,
    pub footnotes: Vec<(usize, Expanded<Extracted>)>,
}

pub fn text_content<E>(out: &mut String, src: &Vec<Expanded<E>>) {
    for child in src {
        match child {
            Expanded::Text(t) => out.push_str(t),
            Expanded::Eager { children, .. } => {
                text_content(out, children);
            }
            Expanded::Lazy { children, .. } => {
                text_content(out, children);
            }
        }
    }
}
