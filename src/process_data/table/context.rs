//! Record processing context
//!
//! This module provides the context struct used during document processing,
//! managing schema references, hasher state, and error context.

use std::{path::PathBuf, sync::Arc};

use crate::{ErrorContext, process_data::CompoundId, process_data::CompoundIdPrefix, schema};

/// Context for processing a record/row within a table.
pub struct RecordContext {
    pub table: String,
    pub schema: Arc<schema::CollectionSchema>,
    pub hasher: blake3::Hasher,
    pub compound_id_prefix: CompoundIdPrefix,
    pub error: ErrorContext,
    pub document_path: PathBuf,
}

impl Clone for RecordContext {
    fn clone(&self) -> Self {
        Self {
            table: self.table.clone(),
            schema: self.schema.clone(),
            hasher: self.hasher.clone(),
            compound_id_prefix: self.compound_id_prefix.clone(),
            error: self.error.clone(),
            document_path: self.document_path.clone(),
        }
    }
}

impl RecordContext {
    /// Get the current table's schema.
    pub fn current_schema(&self) -> &schema::TableSchema {
        self.schema.tables.get(&self.table).unwrap()
    }

    /// Create a nested context for a child table.
    pub fn nest(self, table: impl Into<String>, id: CompoundId) -> Result<Self, crate::Error> {
        let table = table.into();
        let inherit_ids = self.schema.tables.get(&table).unwrap().inherit_ids.clone();
        let Self {
            schema,
            error,
            document_path,
            ..
        } = self;
        let compound_id_prefix = id
            .try_into_prefix(inherit_ids)
            .map_err(|detail| error.error(detail))?;
        Ok(Self {
            table,
            hasher: self.hasher.clone(),
            schema,
            compound_id_prefix,
            error,
            document_path,
        })
    }

    /// Create a compound ID from the current context.
    pub fn id(&self, id: impl Into<String>) -> CompoundId {
        self.compound_id_prefix
            .clone()
            .id(&self.current_schema().id_name, id.into())
    }

    /// Create a new context with an error ID attached.
    pub fn with_error_id(&self, id: CompoundId) -> Self {
        Self {
            error: self.error.with_id(id),
            ..self.clone()
        }
    }
}
