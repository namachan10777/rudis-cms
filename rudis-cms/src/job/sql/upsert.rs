use itertools::Itertools;

use crate::schema::{FieldType, TableSchema};
use std::fmt::Write as _;

const CURRENT_TIMESTAMP: &str = "strftime('%Y-%m-%dT%H:%M:%fZ', 'now')";

fn erase_comma_newline(out: &mut String) {
    out.pop();
    out.pop();
    out.push('\n');
}

pub fn generate(out: &mut String, table: &str, schema: &TableSchema) -> std::fmt::Result {
    writeln!(
        out,
        "INSERT INTO {table}({})",
        schema
            .inherit_ids
            .iter()
            .chain(
                schema
                    .fields
                    .iter()
                    .filter(|(_, field)| !matches!(field, FieldType::Records { .. }))
                    .map(|(key, _)| key)
            )
            .join(", ")
    )?;
    writeln!(out, "SELECT")?;
    for inherit_id in &schema.inherit_ids {
        writeln!(out, "  value->>'{inherit_id}',")?;
    }
    for (name, field) in schema.fields.iter() {
        match field {
            FieldType::Records { .. } => {}
            FieldType::CreatedAt | FieldType::UpdatedAt => {
                writeln!(out, "  {CURRENT_TIMESTAMP},")?;
            }
            _ => {
                writeln!(out, "  value->>'{name}',")?;
            }
        }
    }
    erase_comma_newline(out);
    writeln!(out, "FROM json_each(?->>'{table}')")?;
    writeln!(out, "WHERE 1")?;
    writeln!(
        out,
        "ON CONFLICT ({})",
        schema
            .inherit_ids
            .iter()
            .chain(std::iter::once(&schema.id_name))
            .join(", ")
    )?;
    let data_columns = schema
        .fields
        .iter()
        .filter(|(_, field)| {
            !matches!(
                field,
                FieldType::Id
                    | FieldType::CreatedAt
                    | FieldType::UpdatedAt
                    | FieldType::Records { .. }
            )
        })
        .map(|(name, _)| name)
        .collect::<Vec<_>>();
    if data_columns.is_empty() {
        writeln!(out, "DO NOTHING;")?;
    } else {
        writeln!(out, "DO UPDATE SET")?;
        for name in &data_columns {
            writeln!(out, "  {name} = EXCLUDED.{name},")?;
        }
        if let Some(updated_at) = &schema.updated_at_name {
            writeln!(out, "  {updated_at} = {CURRENT_TIMESTAMP},")?;
        }
        erase_comma_newline(out);
        writeln!(out, "WHERE")?;
        for (idx, name) in data_columns.iter().enumerate() {
            let suffix = if idx == data_columns.len() - 1 {
                ";"
            } else {
                " OR"
            };
            writeln!(out, "  {table}.{name} IS NOT EXCLUDED.{name}{suffix}")?;
        }
    }
    Ok(())
}
