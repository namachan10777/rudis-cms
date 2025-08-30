use indexmap::IndexMap;

use crate::field::rich_text::{Node, parser::meta_parser::CodeblockMeta};

use super::{AlertKind, AttrValue, Name};

mod markdown;

pub(crate) mod meta_parser;

pub struct RichTextDocumentRaw {
    pub(crate) root: Vec<Node<KeepRaw>>,
    pub(crate) footnotes: IndexMap<String, Node<KeepRaw>>,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum RichTextLang {
    Markdown,
}

#[derive(Clone)]
pub(crate) enum KeepRaw {
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
        attrs: IndexMap<Name, AttrValue>,
    },
    Image {
        title: String,
        id: String,
        url: String,
    },
    Link {
        link_type: super::LinkType,
        dest_url: String,
        title: String,
        id: String,
    },
}

pub fn parse(src: &str, lang: RichTextLang) -> RichTextDocumentRaw {
    match lang {
        RichTextLang::Markdown => markdown::parse(src),
    }
}

impl RichTextDocumentRaw {
    pub(crate) fn for_each_content<'a, 'f, F>(&'a self, mut f: F)
    where
        'a: 'f,
        F: 'f + FnMut(&'a Node<KeepRaw>),
    {
        for node in &self.root {
            f(node);
        }
        for node in self.footnotes.values() {
            f(node);
        }
    }
}
