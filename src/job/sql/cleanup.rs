use crate::schema::CollectionSchema;
use std::fmt::Write;

pub fn generate(out: &mut String, schema: &CollectionSchema) -> std::fmt::Result {
    let (name, main_table) = schema.tables.iter().next().unwrap();
    let id = &main_table.id_name;
    writeln!(out, "DELETE FROM {name}")?;
    writeln!(out, "WHERE {id} NOT IN (")?;
    writeln!(out, "  SELECT value->>'{id}' FROM json_each(?->>'{name}')")?;
    writeln!(out, ");")?;
    Ok(())
}
