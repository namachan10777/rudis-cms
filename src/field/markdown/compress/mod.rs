use std::fmt::Write;

use indexmap::IndexMap;
use serde::{Deserialize, Serialize};

use crate::field::markdown::{
    AlertKind, resolver, text_content,
    types::{AttrValue, Name},
};

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub enum HeadingLevel {
    H1,
    H2,
    H3,
    H4,
    H5,
    H6,
}

impl HeadingLevel {
    pub fn from_tag_name(tag: &str) -> Option<Self> {
        match tag {
            "h1" => Some(Self::H1),
            "h2" => Some(Self::H2),
            "h3" => Some(Self::H3),
            "h4" => Some(Self::H4),
            "h5" => Some(Self::H5),
            "h6" => Some(Self::H6),
            _ => None,
        }
    }
}

impl From<pulldown_cmark::HeadingLevel> for HeadingLevel {
    fn from(value: pulldown_cmark::HeadingLevel) -> Self {
        match value {
            pulldown_cmark::HeadingLevel::H1 => Self::H1,
            pulldown_cmark::HeadingLevel::H2 => Self::H2,
            pulldown_cmark::HeadingLevel::H3 => Self::H3,
            pulldown_cmark::HeadingLevel::H4 => Self::H4,
            pulldown_cmark::HeadingLevel::H5 => Self::H5,
            pulldown_cmark::HeadingLevel::H6 => Self::H6,
        }
    }
}

impl From<HeadingLevel> for u8 {
    fn from(value: HeadingLevel) -> Self {
        match value {
            HeadingLevel::H1 => 1,
            HeadingLevel::H2 => 2,
            HeadingLevel::H3 => 3,
            HeadingLevel::H4 => 4,
            HeadingLevel::H5 => 5,
            HeadingLevel::H6 => 6,
        }
    }
}

impl Serialize for HeadingLevel {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            Self::H1 => serializer.serialize_i8(1),
            Self::H2 => serializer.serialize_i8(2),
            Self::H3 => serializer.serialize_i8(3),
            Self::H4 => serializer.serialize_i8(4),
            Self::H5 => serializer.serialize_i8(5),
            Self::H6 => serializer.serialize_i8(6),
        }
    }
}

impl<'de> Deserialize<'de> for HeadingLevel {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let level = u8::deserialize(deserializer)?;
        match level {
            1 => Ok(HeadingLevel::H1),
            2 => Ok(HeadingLevel::H2),
            3 => Ok(HeadingLevel::H3),
            4 => Ok(HeadingLevel::H4),
            5 => Ok(HeadingLevel::H5),
            6 => Ok(HeadingLevel::H6),
            _ => Err(serde::de::Error::custom("invalid heading level")),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Heading {
    pub level: HeadingLevel,
    pub slug: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct ImageSizeVariant {
    pub src: url::Url,
    pub width: u32,
    pub height: u32,
    pub content_type: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Image {
    pub src: url::Url,
    pub blurhash: Option<String>,
    pub alt: String,
    pub width: u32,
    pub height: u32,
    pub content_type: String,
    pub variants: Vec<ImageSizeVariant>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct LinkCardImage {
    pub src: url::Url,
    pub width: u32,
    pub height: u32,
    pub content_type: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct LinkCard {
    pub href: url::Url,
    pub title: String,
    pub description: String,
    pub favicon: Option<LinkCardImage>,
    pub og_image: Option<LinkCardImage>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Codeblock {
    pub lang: Option<String>,
    pub title: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct FootnoteReference {
    pub id: String,
    pub content: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(tag = "type")]
pub enum Keep {
    Heading(Heading),
    Image(Image),
    LinkCard(LinkCard),
    Codeblock(Codeblock),
    Alert(AlertKind),
    FootnoteReference(FootnoteReference),
}

#[derive(Serialize, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Node {
    KeepLazy {
        keep: Keep,
        children: Vec<Node>,
    },
    KeepEager {
        keep: Keep,
        content: String,
    },
    Lazy {
        tag: Name,
        attrs: IndexMap<Name, AttrValue>,
        children: Vec<Node>,
    },
    Eager {
        tag: Name,
        attrs: IndexMap<Name, AttrValue>,
        content: String,
    },
    Text(String),
}

#[derive(Serialize, Debug)]
pub struct Footnote {
    pub id: String,
    pub reference: Option<usize>,
    pub content: Fragment,
}

#[derive(Serialize, Debug)]
pub struct Header {
    pub header: String,
    pub level: HeadingLevel,
    pub id: String,
    pub children: Fragment,
}

#[derive(Serialize, Debug)]
pub struct Section {
    pub level: HeadingLevel,
    pub id: String,
    pub title: String,
    pub content: String,
}

#[derive(Serialize, Debug)]
pub struct RichTextDocument {
    pub root: Fragment,
    pub footnotes: Vec<Footnote>,
    pub sections: Vec<Section>,
}

pub struct MarkdownConfig {
    pub keep_image: bool,
}

type ResolverNode = super::Node<Keep>;

fn write_attrs<W: std::fmt::Write>(
    out: &mut W,
    attrs: &IndexMap<Name, AttrValue>,
) -> std::fmt::Result {
    for (name, value) in attrs {
        match value {
            AttrValue::Bool(false) => {}
            AttrValue::Bool(true) => {
                write!(out, " {name}").unwrap();
            }
            AttrValue::Integer(i) => {
                write!(out, " {name}={}", i).unwrap();
            }
            AttrValue::OwnedStr(s) => {
                write!(out, " {name}=\"{}\"", s).unwrap();
            }
            AttrValue::StaticStr(s) => {
                write!(out, " {name}=\"{}\"", s).unwrap();
            }
        }
    }
    Ok(())
}

fn compress_children(children: impl IntoIterator<Item = ResolverNode>) -> Fragment {
    let mut out = Vec::new();
    children.into_iter().for_each(|node| match node {
        ResolverNode::Text(text) => {
            if let Some(Node::Text(prev)) = out.last_mut() {
                prev.push_str(&text);
            } else {
                out.push(Node::Text(text));
            }
        }
        ResolverNode::Eager {
            tag,
            attrs,
            children,
        } => match compress_children(children) {
            Fragment::Tree { children } => {
                out.push(Node::Lazy {
                    tag,
                    attrs,
                    children,
                });
            }
            Fragment::Html { content } => out.push(Node::Eager {
                tag,
                attrs,
                content,
            }),
        },
        ResolverNode::Lazy { keep, children } => match compress_children(children) {
            Fragment::Tree { children } => {
                out.push(Node::KeepLazy { keep, children });
            }
            Fragment::Html { content } => {
                out.push(Node::KeepEager { keep, content });
            }
        },
    });
    if out
        .iter()
        .all(|node| matches!(node, Node::Text(_) | Node::Eager { .. }))
    {
        let mut out_string = String::new();
        out.iter().for_each(|node| match node {
            Node::Text(text) => out_string.push_str(text),
            Node::Eager {
                tag,
                attrs,
                content,
            } => {
                if content.is_empty() {
                    write!(out_string, "<{tag}").unwrap();
                    write_attrs(&mut out_string, attrs).unwrap();
                    write!(out_string, "/>").unwrap();
                } else {
                    write!(out_string, "<{tag}").unwrap();
                    write_attrs(&mut out_string, attrs).unwrap();
                    write!(out_string, ">{content}</{tag}>").unwrap();
                }
            }
            _ => unreachable!(),
        });
        Fragment::Html {
            content: out_string,
        }
    } else {
        Fragment::Tree { children: out }
    }
}

#[derive(Serialize, Debug)]
#[serde(tag = "type")]
pub enum Fragment {
    Html { content: String },
    Tree { children: Vec<Node> },
}

fn eager_to_section(tag: &Name, children: &[ResolverNode]) -> Option<Section> {
    if tag.as_ref() != "section" {
        return None;
    }
    match children.get(0)? {
        ResolverNode::Eager {
            tag,
            attrs,
            children,
        } => {
            let level = HeadingLevel::from_tag_name(tag.as_ref())?;
            let id = attrs.get("id")?.to_str()?.to_string();
            let mut title = String::new();
            let mut content = String::new();
            text_content(&mut title, children);
            text_content(&mut content, &children[1..]);
            Some(Section {
                level,
                id,
                title,
                content,
            })
        }
        ResolverNode::Lazy {
            keep: Keep::Heading(heading),
            children,
        } => {
            let level = heading.level;
            let id = heading.slug.clone();
            let mut title = String::new();
            let mut content = String::new();
            text_content(&mut title, children);
            text_content(&mut content, &children[1..]);
            Some(Section {
                level,
                id,
                title,
                content,
            })
        }
        _ => None,
    }
}

fn extract_section_tree(sections: &mut Vec<Section>, node: &ResolverNode) {
    match node {
        ResolverNode::Eager { tag, children, .. } => {
            if let Some(section) = eager_to_section(tag, children) {
                sections.push(section);
            }
            children
                .iter()
                .for_each(|node| extract_section_tree(sections, node));
        }
        ResolverNode::Text(_) => {}
        ResolverNode::Lazy { children, .. } => children
            .iter()
            .for_each(|node| extract_section_tree(sections, node)),
    }
}

pub fn compress(document: resolver::RichTextDocument) -> RichTextDocument {
    let mut sections = Vec::new();
    document
        .root
        .iter()
        .for_each(|node| extract_section_tree(&mut sections, node));
    let footnotes = document
        .footnotes
        .into_iter()
        .map(|footnote| Footnote {
            id: footnote.id,
            reference: footnote.reference_number,
            content: compress_children(footnote.content),
        })
        .collect();

    RichTextDocument {
        root: compress_children(document.root),
        footnotes,
        sections,
    }
}
