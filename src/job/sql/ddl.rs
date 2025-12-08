use crate::schema::{CollectionSchema, FieldType};
use std::fmt::Write as _;

use super::builder::{sqlite_index_expr, sqlite_type};

pub fn generate(out: &mut String, schema: &CollectionSchema) -> std::fmt::Result {
    for (table, schema) in &schema.tables {
        writeln!(out, "CREATE TABLE IF NOT EXISTS {table} (")?;
        for inherit_id in &schema.inherit_ids {
            writeln!(out, "  {inherit_id} TEXT NOT NULL,")?;
        }
        for (name, field) in &schema.fields {
            let Some(type_name) = sqlite_type(field) else {
                continue;
            };
            write!(out, "  {name} {type_name}")?;
            if field.is_required_field() {
                writeln!(out, " NOT NULL,")?;
            } else {
                writeln!(out, ",")?;
            }
        }
        if let Some(parent) = &schema.parent {
            writeln!(
                out,
                "  FOREIGN KEY ({}) REFERENCES {}({}) ON DELETE CASCADE,",
                schema.inherit_ids.join(", "),
                parent.name,
                parent.id_names.join(", "),
            )?;
        }
        write!(out, "  PRIMARY KEY (")?;
        for inherit_id in &schema.inherit_ids {
            write!(out, "{inherit_id}, ")?;
        }
        writeln!(out, "{})", schema.id_name)?;
        writeln!(out, ");")?;
        for (name, field) in &schema.fields {
            if !field.requires_index()
                || matches!(
                    field,
                    FieldType::Image { .. } | FieldType::File { .. } | FieldType::Markdown { .. }
                )
            {
                continue;
            }
            let Some(index) = sqlite_index_expr(name, field) else {
                continue;
            };
            writeln!(
                out,
                "CREATE INDEX IF NOT EXISTS index_{table}_{name} ON {table}({index});"
            )?;
        }
    }
    Ok(())
}
