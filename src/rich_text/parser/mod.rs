pub mod markdown;

use crate::rich_text::MdRoot;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Failed to parse frontmatter: {0}")]
    InvalidFrontmatter(String),
    #[error("Frontmatter not found")]
    FrontmatterNotFound,
}

pub trait Parser {
    fn parse(&self, src: &str) -> Result<MdRoot, Error>;

    fn parse_only_data(&self, src: &str) -> Result<serde_json::Value, Error> {
        let root = self.parse(src)?;
        root.frontmatter.ok_or(Error::FrontmatterNotFound)
    }

    fn extract_image_srcs(&self) -> Vec<String> {
        unimplemented!()
    }
}
