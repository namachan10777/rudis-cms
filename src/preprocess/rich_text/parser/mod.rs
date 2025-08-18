pub mod markdown;

use std::collections::HashSet;

use super::{Expanded, RawExtracted, RichTextDocument};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Failed to parse frontmatter: {0}")]
    InvalidFrontmatter(String),
    #[error("Frontmatter not found")]
    FrontmatterNotFound,
}

fn extract_image_src(ast: &Expanded<RawExtracted>) -> Vec<String> {
    match ast {
        Expanded::Text(_) => Vec::new(),
        Expanded::Lazy {
            extracted: RawExtracted::Image { url, .. },
            ..
        } => vec![url.to_string()],
        Expanded::Lazy { children, .. } => children.iter().flat_map(extract_image_src).collect(),
        Expanded::Eager { children, .. } => children.iter().flat_map(extract_image_src).collect(),
    }
}

pub trait Parser {
    fn parse(&self, src: &str) -> RichTextDocument<RawExtracted>;

    fn extract_image_srcs(&self, src: &str) -> Result<HashSet<String>, Error> {
        let root = self.parse(src);
        Ok(root.children.iter().flat_map(extract_image_src).collect())
    }
}
