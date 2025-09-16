use crate::schema::{CollectionSchema, FieldType};
use std::fmt::Write as _;

fn generate_statement(out: &mut String, table: &str, column: &str) -> std::fmt::Result {
    writeln!(out, "SELECT ")?;
    writeln!(out, "  {column}->>'hash' AS hash")?;
    writeln!(out, "  {column}->>'pointer' AS storage")?;
    writeln!(out, "FROM {table}")?;
    writeln!(
        out,
        "WHERE {column} IST NOT NULL AND {column}->>'hash' IS NOT NULL"
    )?;
    Ok(())
}

pub fn generate(out: &mut String, schema: &CollectionSchema) -> std::fmt::Result {
    let mut columns = schema.tables.iter().flat_map(|(table, schema)| {
        schema.fields.iter().filter_map(|(name, field)| {
            if matches!(
                field,
                FieldType::Markdown { .. } | FieldType::File { .. } | FieldType::Image { .. }
            ) {
                Some((table.as_str(), name.as_str()))
            } else {
                None
            }
        })
    });
    let Some((table, column)) = columns.next() else {
        return Ok(());
    };
    generate_statement(out, table, column)?;
    for (table, column) in columns {
        writeln!(out, "UNION ALL")?;
        generate_statement(out, table, column)?;
    }
    Ok(())
}
