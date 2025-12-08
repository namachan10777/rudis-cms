//! Type definitions for table processing
//!
//! This module defines the core types used throughout the table processing pipeline.

use indexmap::IndexMap;

use crate::{
    config,
    process_data::{ColumnValue, CompoundId, StorageContent, StoragePointer, markdown::compress},
};

/// A processed row node containing fields, records, and uploads.
pub struct RowNode {
    pub id: CompoundId,
    pub fields: IndexMap<String, ColumnValue>,
    pub records: IndexMap<String, Records>,
    pub uploads: Uploads,
    pub hash: blake3::Hash,
}

/// A collection of records belonging to a table.
pub struct Records {
    pub table: String,
    pub rows: Vec<RowNode>,
}

/// Represents a processed field value.
pub enum FieldValue {
    /// A simple column value.
    Column(ColumnValue),
    /// A column value with an associated upload.
    WithUpload { column: ColumnValue, upload: Upload },
    /// A processed markdown field.
    Markdown {
        document: compress::RichTextDocument,
        storage: config::Storage,
        image_table: String,
        image_rows: Vec<RowNode>,
    },
    /// A nested records field.
    Records(Records),
}

/// An upload to be sent to storage.
#[derive(Clone)]
pub struct Upload {
    pub data: StorageContent,
    pub hash: blake3::Hash,
    pub pointer: StoragePointer,
    pub content_type: String,
}

/// A collection of uploads.
pub type Uploads = Vec<Upload>;

/// A map of table names to their rows.
pub type Tables = IndexMap<String, Vec<IndexMap<String, ColumnValue>>>;
