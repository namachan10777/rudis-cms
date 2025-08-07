pub mod markdown;

use std::collections::HashSet;

use crate::rich_text::{Extracted, MdAst, MdRoot};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Failed to parse frontmatter: {0}")]
    InvalidFrontmatter(String),
    #[error("Frontmatter not found")]
    FrontmatterNotFound,
}

fn extract_image_src(ast: &MdAst) -> Vec<String> {
    match ast {
        MdAst::Text(_) => Vec::new(),
        MdAst::Raw(_) => Vec::new(),
        MdAst::Lazy {
            extracted: Extracted::Image { url, .. },
            ..
        } => vec![url.to_string()],
        MdAst::Lazy { children, .. } => children.iter().flat_map(extract_image_src).collect(),
        MdAst::Eager { children, .. } => children.iter().flat_map(extract_image_src).collect(),
    }
}

pub trait Parser {
    fn parse(&self, src: &str) -> Result<MdRoot, Error>;

    fn parse_only_data(&self, src: &str) -> Result<serde_json::Value, Error> {
        let root = self.parse(src)?;
        root.frontmatter.ok_or(Error::FrontmatterNotFound)
    }

    fn extract_image_srcs(&self, src: &str) -> Result<HashSet<String>, Error> {
        let root = self.parse(src)?;
        Ok(root.children.iter().flat_map(extract_image_src).collect())
    }
}
