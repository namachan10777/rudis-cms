use indexmap::IndexMap;

use crate::field::rich_text::{Node, parser::KeepRaw};

#[derive(Default)]
pub(super) struct FootnoteResolver<'r> {
    index: usize,
    index_map: IndexMap<&'r str, usize>,
}

impl<'r> FootnoteResolver<'r> {
    pub(super) fn analyze(&mut self, node: &'r Node<KeepRaw>) {
        match node {
            Node::Eager { children, .. } => children.iter().for_each(|node| self.analyze(node)),
            Node::Lazy {
                keep: KeepRaw::FootnoteReference { id },
                children,
            } => {
                self.index += 1;
                self.index_map.entry(&*id).or_insert(self.index);
                children.iter().for_each(|node| self.analyze(node));
            }
            Node::Lazy { children, .. } => children.iter().for_each(|node| self.analyze(node)),
            Node::Text(_) => {}
        }
    }

    pub(super) fn resolve(&self, id: &str) -> Option<usize> {
        self.index_map.get(id).copied()
    }
}
