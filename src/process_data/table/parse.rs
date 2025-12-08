//! Frontmatter parsing for Markdown documents
//!
//! This module handles YAML and TOML frontmatter extraction from Markdown documents.

use std::sync::LazyLock;

use crate::ErrorDetail;

pub(crate) static FRONTMATTER_SEPARATOR_YAML: LazyLock<regex::Regex> =
    LazyLock::new(|| regex::Regex::new(r"(?:^|\n)---\s*\n").unwrap());

pub(crate) static FRONTMATTER_SEPARATOR_TOML: LazyLock<regex::Regex> =
    LazyLock::new(|| regex::Regex::new(r"(?:^|\n)\+\+\+\s*\n").unwrap());

/// Parse a markdown document and extract frontmatter and content.
///
/// Supports both YAML (---) and TOML (+++) frontmatter delimiters.
pub fn parse_markdown(
    content: &str,
) -> Result<(serde_json::Map<String, serde_json::Value>, &str), ErrorDetail> {
    if let Some(start) = FRONTMATTER_SEPARATOR_YAML.find(content) {
        if let Some(end) = FRONTMATTER_SEPARATOR_YAML.find_at(content, start.end() + 1) {
            let frontmatter = serde_yaml::from_str(&content[start.end()..end.start()])
                .map_err(ErrorDetail::ParseYaml)?;
            Ok((frontmatter, &content[end.end()..]))
        } else {
            Err(ErrorDetail::UnclosedFrontmatter)
        }
    } else if let Some(start) = FRONTMATTER_SEPARATOR_TOML.find(content) {
        if let Some(end) = FRONTMATTER_SEPARATOR_TOML.find_at(content, start.end() + 1) {
            let frontmatter = toml::de::from_str(&content[start.end()..end.start()])
                .map_err(ErrorDetail::ParseToml)?;
            Ok((frontmatter, &content[end.end()..]))
        } else {
            Err(ErrorDetail::UnclosedFrontmatter)
        }
    } else {
        Ok((Default::default(), content))
    }
}

/// Extract the ID field value from a map of fields.
pub fn extract_id_value(
    name: &str,
    fields: &mut serde_json::Map<String, serde_json::Value>,
) -> Result<String, ErrorDetail> {
    let Some(id) = fields.remove(name) else {
        return Err(ErrorDetail::MissingField(name.to_owned()));
    };
    match id {
        serde_json::Value::String(id) => Ok(id),
        _ => Err(ErrorDetail::TypeMismatch {
            expected: "string",
            got: id,
        }),
    }
}
