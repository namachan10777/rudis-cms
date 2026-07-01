//! Serialization utilities
//!
//! This module provides serialization helpers for document fields and records.

use indexmap::IndexMap;
use serde::{
    Serialize,
    ser::{SerializeMap as _, SerializeSeq},
};

use crate::process_data::ColumnValue;

use super::types::Records;

/// Wrapper for serializing frontmatter (fields + records).
pub struct Frontmatter<'a> {
    pub fields: &'a IndexMap<String, ColumnValue>,
    pub records: &'a IndexMap<String, Records>,
}

impl<'a> Serialize for Frontmatter<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut serializer =
            serializer.serialize_map(Some(self.fields.len() + self.records.len()))?;
        for (name, value) in self.fields {
            serializer.serialize_entry(name, value)?;
        }
        for (name, records) in self.records {
            serializer.serialize_entry(name, records)?;
        }
        serializer.end()
    }
}

impl Serialize for Records {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut serializer = serializer.serialize_seq(Some(self.rows.len()))?;
        for row in &self.rows {
            serializer.serialize_element(&row.fields)?;
        }
        serializer.end()
    }
}
