use indexmap::IndexMap;

use crate::field::markdown::{Node, parser::KeepRaw, text_content};

pub(super) struct FootnoteResolver {
    index: usize,
    index_map: IndexMap<String, (usize, String)>,
    contents: IndexMap<String, String>,
}

impl FootnoteResolver {
    pub(super) fn new<E>(footnotes: &IndexMap<String, Vec<Node<E>>>) -> Self {
        Self {
            index: 0,
            index_map: Default::default(),
            contents: footnotes
                .iter()
                .map(|(id, node)| {
                    let mut content = String::new();
                    text_content(&mut content, node);
                    (id.clone(), content)
                })
                .collect(),
        }
    }

    pub(super) fn analyze(&mut self, node: &Node<KeepRaw>) {
        match node {
            Node::Eager { children, .. } => children.iter().for_each(|node| self.analyze(node)),
            Node::Lazy {
                keep: KeepRaw::FootnoteReference { id },
                children,
            } => {
                if let Some(content) = self.contents.get(id) {
                    self.index += 1;
                    self.index_map
                        .entry(id.clone())
                        .or_insert((self.index, content.clone()));
                }
                children.iter().for_each(|node| self.analyze(node));
            }
            Node::Lazy { children, .. } => children.iter().for_each(|node| self.analyze(node)),
            Node::Text(_) => {}
        }
    }

    pub(super) fn resolve(&self, id: &str) -> Option<(usize, &str)> {
        self.index_map
            .get(id)
            .map(|(index, content)| (*index, content.as_str()))
    }
}
