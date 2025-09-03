use indexmap::IndexMap;

use crate::field::markdown::{Node, compress::HeadingLevel, parser::meta_parser::CodeblockMeta};

use super::{AlertKind, AttrValue, Name};

mod markdown;

pub(crate) mod meta_parser;

pub struct RichTextDocumentRaw {
    pub(crate) root: Vec<Node<KeepRaw>>,
    pub(crate) footnotes: IndexMap<String, Vec<Node<KeepRaw>>>,
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
        level: HeadingLevel,
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

pub fn parse(src: &str) -> RichTextDocumentRaw {
    markdown::parse(src)
}

impl RichTextDocumentRaw {
    pub(crate) fn for_each_content<'a, 'f, F>(&'a self, mut f: F)
    where
        'a: 'f,
        F: 'f + FnMut(&'a Node<KeepRaw>),
    {
        self.root.iter().for_each(&mut f);
        self.footnotes.values().flatten().for_each(f);
    }
}
