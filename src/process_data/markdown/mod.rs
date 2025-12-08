use std::hash::Hash;

use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use tracing::warn;
use valuable::Valuable;

pub mod compress;
pub mod parser;
pub mod resolver;
mod types;
pub use types::{AttrValue, Name};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Valuable)]
#[serde(rename_all = "snake_case")]
pub enum AlertKind {
    Caution,
    Important,
    Note,
    Warning,
    Tip,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Valuable)]
pub struct Alert {
    kind: AlertKind,
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

#[derive(Debug, Clone)]
pub enum Node<K> {
    Eager {
        tag: Name,
        attrs: IndexMap<Name, AttrValue>,
        children: Vec<Node<K>>,
    },
    Text(String),
    Lazy {
        keep: K,
        children: Vec<Node<K>>,
    },
}

pub(crate) fn raw_to_expanded<E>(src: &str) -> Vec<Node<E>> {
    match html_parser::Dom::parse(src) {
        Ok(dom) => dom
            .children
            .into_iter()
            .map(|element| element.into())
            .collect(),
        Err(e) => {
            warn!(%e, "failed to parse html");
            crate::warn_entry!("failed to parse html: {e}");
            vec![Node::Text(src.to_string())]
        }
    }
}

impl<E> From<html_parser::Node> for Node<E> {
    fn from(value: html_parser::Node) -> Self {
        match value {
            html_parser::Node::Comment(_) => Node::Text("".to_string()),
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
                    .collect::<IndexMap<Name, AttrValue>>();
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
            html_parser::Node::Text(text) => Node::Text(text),
        }
    }
}

#[derive(Debug, Clone)]
pub struct RichTextDocument<K> {
    pub children: Vec<Node<K>>,
    pub footnotes: IndexMap<String, Node<K>>,
}

pub fn text_content<E>(out: &mut String, src: &[Node<E>]) {
    for child in src {
        match child {
            Node::Text(t) => out.push_str(t),
            Node::Eager { children, .. } => {
                text_content(out, children);
            }
            Node::Lazy { children, .. } => {
                text_content(out, children);
            }
        }
    }
}
