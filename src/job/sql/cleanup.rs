use crate::schema::TableSchema;
use std::fmt::Write;

pub fn generate(out: &mut String, table: &str, schema: &TableSchema) -> std::fmt::Result {
    let id = &schema.id_name;
    writeln!(out, "DELETE FROM {table}")?;
    writeln!(out, "WHERE {id} NOT IN (")?;
    writeln!(out, "  SELECT value->>'{id}' FROM json_each(?->>'{table}')")?;
    writeln!(out, ");")?;
    Ok(())
}
