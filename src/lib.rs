use std::path::PathBuf;

use crate::process_data::{CompoundId, object_loader};

pub mod config;
pub mod deploy;
pub mod job;
pub mod process_data;
pub mod schema;
pub mod typescript;

#[derive(Debug, thiserror::Error)]
#[error("{context}: {detail}")]
pub struct Error {
    pub context: Box<ErrorContext>,
    pub detail: Box<ErrorDetail>,
}

#[derive(Debug, Clone)]
pub struct ErrorContext {
    pub path: PathBuf,
    pub id: Option<CompoundId>,
}

impl ErrorContext {
    fn new(path: PathBuf) -> Self {
        Self { path, id: None }
    }

    fn with_id(&self, id: CompoundId) -> Self {
        Self {
            path: self.path.clone(),
            id: Some(id),
        }
    }

    fn error(&self, detail: ErrorDetail) -> Error {
        Error {
            context: Box::new(self.clone()),
            detail: Box::new(detail),
        }
    }
}

impl std::fmt::Display for ErrorContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.id {
            Some(id) => write!(f, "{id}({})", self.path.display()),
            None => write!(f, "{}", self.path.display()),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ErrorDetail {
    #[error("Failed to read document: {0}")]
    ReadDocument(std::io::Error),
    #[error("Failed to parse TOML document: {0}")]
    ParseToml(toml::de::Error),
    #[error("Failed to parse YAML document: {0}")]
    ParseYaml(serde_yaml::Error),
    #[error("Unclosed frontmatter")]
    UnclosedFrontmatter,
    #[error("Type mismatch: expected {expected}, got {got}")]
    TypeMismatch {
        expected: &'static str,
        got: serde_json::Value,
    },
    #[error("Missing field: {0}")]
    MissingField(String),
    #[error("Invalid date: {0}")]
    InvalidDate(String),
    #[error("Invalid datetime: {0}")]
    InvalidDatetime(String),
    #[error("Found computed field: {0}")]
    FoundComputedField(String),
    #[error("Failed to load image: {0}")]
    LoadImage(object_loader::ImageLoadError),
    #[error("Failed to load: {0}")]
    Load(object_loader::Error),
    #[error("Invalid parent ID names")]
    InvalidParentIdNames,
    #[error("SQL Error: {0}")]
    Query(sqlx::Error),
}
