use itertools::Itertools;

use crate::schema::TableSchema;
use std::fmt::Write;

pub fn generate(out: &mut String, table: &str, schema: &TableSchema) -> std::fmt::Result {
    let id = &schema.id_name;
    writeln!(out, "DELETE FROM {table}")?;
    if schema.inherit_ids.is_empty() {
        writeln!(out, "WHERE {id} NOT IN (")?;
    } else {
        writeln!(
            out,
            "WHERE ({}) NOT IN (",
            schema
                .inherit_ids
                .iter()
                .chain(std::iter::once(&schema.id_name))
                .join(" ,")
        )?;
    }
    writeln!(out, "  SELECT")?;
    for id in &schema.inherit_ids {
        writeln!(out, "    value->>'{id}',")?;
    }
    writeln!(out, "    value->>'{id}'")?;
    writeln!(out, "  FROM json_each(?->>'{table}')")?;
    writeln!(out, ");")?;
    Ok(())
}
