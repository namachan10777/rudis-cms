use itertools::Itertools;

use crate::schema::{FieldType, TableSchema};
use std::fmt::Write as _;

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
        if !matches!(field, FieldType::Records { .. }) {
            writeln!(out, "  value->>'{name}',")?;
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
        .filter(|(_, field)| !matches!(field, FieldType::Id | FieldType::Records { .. }))
        .map(|(name, _)| name)
        .collect::<Vec<_>>();
    if data_columns.is_empty() {
        writeln!(out, "DO NOTHING;")?;
    } else {
        writeln!(out, "DO UPDATE SET")?;
        for (idx, name) in data_columns.iter().enumerate() {
            if idx == data_columns.len() - 1 {
                writeln!(out, "  {name} = EXCLUDED.{name};")?;
            } else {
                writeln!(out, "  {name} = EXCLUDED.{name},")?;
            }
        }
    }
    Ok(())
}
