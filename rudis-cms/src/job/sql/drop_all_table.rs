use crate::schema::CollectionSchema;
use std::fmt::Write;

pub fn generate(out: &mut String, schema: &CollectionSchema) -> std::fmt::Result {
    for table in schema.tables.keys() {
        writeln!(out, "DROP TABLE IF EXISTS {table};")?;
    }
    Ok(())
}
