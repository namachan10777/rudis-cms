use std::{collections::HashMap, hash::Hash, sync::Arc};

use serde::{Deserialize, Serialize};
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
        raw: String,
        width: u32,
        height: u32,
        attrs: HashMap<String, AttrValue>,
        inner_content: String,
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
    Raw(String),
    Eager {
        tag: Name,
        attrs: HashMap<String, AttrValue>,
        children: Vec<Expanded<E>>,
    },
    Text(String),
    Lazy {
        extracted: E,
        children: Vec<Expanded<E>>,
    },
}

#[derive(Debug, Clone)]
pub struct RichTextDocument<E> {
    pub children: Vec<Expanded<E>>,
    pub footnote_definitions: HashMap<String, Expanded<E>>,
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
            Expanded::Raw(_) => {}
            Expanded::Eager { children, .. } => {
                text_content(out, children);
            }
            Expanded::Lazy { children, .. } => {
                text_content(out, children);
            }
        }
    }
}
