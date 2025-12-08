//! SQL Builder utilities
//!
//! This module provides common utilities for SQL generation across DDL, DML, and other queries.

use std::fmt::Write;

use crate::schema::FieldType;

/// Get the SQLite type name for a field type.
pub(crate) fn sqlite_type(field: &FieldType) -> Option<&'static str> {
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

/// Get the SQLite index expression for a field.
pub(crate) fn sqlite_index_expr<'a>(
    name: &'a str,
    field: &FieldType,
) -> Option<std::borrow::Cow<'a, str>> {
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

/// Write a comma-separated list of items.
pub fn write_comma_separated<I, F>(
    out: &mut String,
    items: I,
    mut write_item: F,
) -> std::fmt::Result
where
    I: IntoIterator,
    F: FnMut(&mut String, I::Item) -> std::fmt::Result,
{
    let mut first = true;
    for item in items {
        if !first {
            write!(out, ", ")?;
        }
        first = false;
        write_item(out, item)?;
    }
    Ok(())
}

/// Erase trailing comma and newline from output, replacing with newline.
pub fn erase_trailing_comma_newline(out: &mut String) {
    if out.ends_with(",\n") {
        out.pop();
        out.pop();
        out.push('\n');
    }
}

/// Write a JSON field extraction expression.
pub fn write_json_extract(out: &mut String, field_name: &str) -> std::fmt::Result {
    writeln!(out, "  value->>'{field_name}',")
}

/// Write a SELECT clause for JSON extraction of multiple fields.
pub fn write_json_select_fields<'a>(
    out: &mut String,
    fields: impl Iterator<Item = &'a str>,
) -> std::fmt::Result {
    writeln!(out, "SELECT")?;
    for field in fields {
        write_json_extract(out, field)?;
    }
    erase_trailing_comma_newline(out);
    Ok(())
}
