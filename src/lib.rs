use std::path::PathBuf;

use futures::future::try_join_all;
use indexmap::IndexMap;
use tracing::trace;

use crate::field::{CompoundId, object_loader};

pub mod cloudflare;
pub mod config;
pub mod field;
pub mod job;
pub mod schema;
pub mod sql;
pub mod table;
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
    Query(rusqlite::Error),
}

pub async fn batch(
    storage: &impl job::StorageBackend,
    database: &impl job::Database,
    collection: &config::Collection,
    hasher: blake3::Hasher,
    force: bool,
) -> Result<(), anyhow::Error> {
    let schema = schema::TableSchema::compile(collection)?;
    let uploads = crate::field::upload::UploadCollector::default();
    let mut tables: table::Tables = IndexMap::new();
    let tasks = glob::glob(&collection.glob)?.map(|path| async {
        table::push_rows_from_document(
            &collection.table,
            hasher.clone(),
            &schema,
            &collection.syntax,
            &uploads,
            path?,
        )
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))
        .inspect(|tables| {
            for (table, rows) in tables {
                for row in rows {
                    trace!(table, ?row, "row");
                }
            }
        })
    });
    try_join_all(tasks).await?.into_iter().for_each(|t| {
        for (table, mut rows) in t {
            tables.entry(table).or_default().append(&mut rows);
        }
    });
    let uploads = uploads.collect().await;
    let syncset = job::SyncSet { tables, uploads };

    job::batch(storage, database, &schema, syncset, force).await?;
    Ok(())
}
