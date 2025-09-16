use crate::schema::{CollectionSchema, FieldType};
use std::{borrow::Cow, fmt::Write as _};

fn sqlite_type(field: &FieldType) -> Option<&'static str> {
    Some(match field {
        FieldType::Id => "TEXT",
        FieldType::Hash => "TEXT",
        FieldType::String { .. } => "TEXT",
        FieldType::Integer { .. } => "INTEGER",
        FieldType::Real { .. } => "REAL",
        FieldType::Boolean { .. } => "INTEGER",
        FieldType::Date { .. } => "TEXT",
        FieldType::Datetime { .. } => "TEXT",
        FieldType::Image { .. } => "TEXT",
        FieldType::File { .. } => "TEXT",
        FieldType::Markdown { .. } => "TEXT",
        FieldType::Records { .. } => return None,
    })
}

fn sqlite_index<'a>(name: &'a str, field: &FieldType) -> Option<Cow<'a, str>> {
    Some(match field {
        FieldType::Id
        | FieldType::Hash
        | FieldType::String { .. }
        | FieldType::Integer { .. }
        | FieldType::Real { .. }
        | FieldType::Boolean { .. } => name.into(),
        FieldType::Date { .. } => format!("date({name})").into(),
        FieldType::Datetime { .. } => format!("datetime({name})").into(),
        FieldType::Image { .. } | FieldType::File { .. } | FieldType::Markdown { .. } => {
            format!("json_extract({name}, 'hash')").into()
        }
        FieldType::Records { .. } => return None,
    })
}

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
                writeln!(out, ",")?;
            } else {
                writeln!(out, " NOT NULL,")?;
            }
        }
        if let Some(parent) = &schema.parent {
            writeln!(
                out,
                "  FOREIGN KEY ({}) REFERENCES {}({}) ON DELETE CASCADE",
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
        for (name, field) in &schema.fields {
            if !field.requires_index()
                || matches!(
                    field,
                    FieldType::Image { .. } | FieldType::File { .. } | FieldType::Markdown { .. }
                )
            {
                continue;
            }
            let Some(index) = sqlite_index(name, field) else {
                continue;
            };
            writeln!(
                out,
                "CREATE INDEX IF NOT EXISTS index_{table}_{name} ON {table}({index})"
            )?;
        }
    }
    Ok(())
}
