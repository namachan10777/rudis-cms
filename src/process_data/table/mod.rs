//! Table processing module
//!
//! This module handles the processing of documents into table rows,
//! including parsing, validation, transformation, and serialization.

use std::{path::Path, sync::Arc};

use indexmap::IndexMap;

use crate::{ErrorContext, ErrorDetail, config, process_data::ColumnValue, schema};

mod context;
mod parse;
mod serialize;
mod transform;
mod types;
mod validate;

pub use context::RecordContext;
pub use types::{Tables, Upload, Uploads};

use types::RowNode;

/// Flatten a row tree into tables and uploads.
fn flatten_table(
    schema: &schema::CollectionSchema,
    tables: &mut Tables,
    uploads: &mut Uploads,
    table: String,
    mut row: RowNode,
) {
    let mut fields = row.fields;
    for (name, id) in row.id.pairs() {
        fields.insert(name.into(), ColumnValue::Id(id.into()));
    }
    if let Some(hash_name) = schema
        .tables
        .get(&table)
        .and_then(|table| table.hash_name.as_ref())
    {
        fields.insert(hash_name.clone(), ColumnValue::Hash(row.hash));
    }
    tables.entry(table).or_default().push(fields);
    uploads.append(&mut row.uploads);
    for (_, record) in row.records {
        record.rows.into_iter().for_each(|row| {
            flatten_table(schema, tables, uploads, record.table.clone(), row);
        });
    }
}

/// Process a document and push its rows into tables.
pub async fn push_rows_from_document<P: AsRef<Path>>(
    table: &str,
    mut hasher: blake3::Hasher,
    schema: &schema::CollectionSchema,
    syntax: &config::DocumentSyntax,
    path: P,
) -> Result<(Tables, Uploads), crate::Error> {
    let ctx = ErrorContext::new(path.as_ref().to_owned());
    let document = tokio::fs::read_to_string(&path)
        .await
        .map_err(|error| ctx.clone().error(ErrorDetail::ReadDocument(error)))?;
    hasher.update(document.as_bytes());
    let fields = match syntax {
        config::DocumentSyntax::Toml => toml::de::from_str(&document)
            .map_err(|error| ctx.error(ErrorDetail::ParseToml(error)))?,
        config::DocumentSyntax::Yaml => serde_yaml::from_str(&document)
            .map_err(|error| ctx.error(ErrorDetail::ParseYaml(error)))?,
        config::DocumentSyntax::Markdown { column } => {
            let (mut frontmatter, content) =
                parse::parse_markdown(&document).map_err(|detail| ctx.error(detail))?;
            frontmatter.insert(column.clone(), content.to_owned().into());
            frontmatter
        }
    };

    let ctx = RecordContext {
        hasher,
        table: table.to_owned(),
        schema: Arc::new(schema.clone()),
        compound_id_prefix: Default::default(),
        error: ctx,
        document_path: path.as_ref().to_owned(),
    };

    let mut tables = IndexMap::new();
    let mut uploads = Vec::new();
    let tree = transform::process_row(&ctx, fields).await?;
    flatten_table(schema, &mut tables, &mut uploads, table.into(), tree);

    Ok((tables, uploads))
}
