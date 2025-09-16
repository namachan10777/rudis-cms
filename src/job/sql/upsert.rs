use itertools::Itertools;

use crate::schema::TableSchema;
use std::fmt::Write as _;

pub fn generate(out: &mut String, table: &str, schema: &TableSchema) -> std::fmt::Result {
    write!(
        out,
        "INSERT INTO {table}({})",
        schema
            .inherit_ids
            .iter()
            .chain(schema.fields.keys())
            .join(", ")
    )?;
    writeln!(out, "SELECT")?;
    for inherit_id in &schema.inherit_ids {
        writeln!(out, "  value->>'{inherit_id}'")?;
    }
    for (i, (name, _)) in schema.fields.iter().enumerate() {
        if i == schema.fields.len() - 1 {
            writeln!(out, "  value->>'{name}'")?;
        } else {
            writeln!(out, "  value->>'{name}',")?;
        }
    }
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
    if schema.fields.len() > 1 {
        writeln!(out, "DO UPDATE SET")?;
        for (i, (name, field)) in schema.fields.iter().enumerate() {
            if matches!(field, crate::schema::FieldType::Id) {
                continue;
            }
            let is_last = i == schema.fields.len() - 1;
            if is_last {
                writeln!(out, "  {name} = EXCLUDED.{name};")?;
            } else {
                writeln!(out, "  {name} = EXCLUDED.{name},")?;
            }
        }
    } else {
        writeln!(out, "DO NOTIHNG;")?;
    }
    Ok(())
}
