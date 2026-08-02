use std::fmt::Write as _;

use crate::schema::{CollectionSchema, FieldType, TableSchema};

const CURRENT_TIMESTAMP: &str = "strftime('%Y-%m-%dT%H:%M:%fZ', 'now')";

fn primary_key(schema: &TableSchema) -> impl Iterator<Item = &String> {
    schema
        .inherit_ids
        .iter()
        .chain(std::iter::once(&schema.id_name))
}

fn ancestor_key_mapping<'a>(
    ancestor: &'a TableSchema,
    descendant: &'a TableSchema,
) -> impl Iterator<Item = (&'a String, &'a String)> {
    primary_key(ancestor).zip(descendant.inherit_ids.iter())
}

fn is_descendant(schema: &CollectionSchema, table: &str, ancestor: &str) -> bool {
    let mut current = schema
        .tables
        .get(table)
        .and_then(|table| table.parent.as_ref());
    while let Some(parent) = current {
        if parent.name == ancestor {
            return true;
        }
        current = schema
            .tables
            .get(&parent.name)
            .and_then(|table| table.parent.as_ref());
    }
    false
}

fn write_incoming_change(
    out: &mut String,
    ancestor_table: &str,
    ancestor: &TableSchema,
    descendant_table: &str,
    descendant: &TableSchema,
) -> std::fmt::Result {
    writeln!(out, "  EXISTS (")?;
    writeln!(out, "    SELECT 1")?;
    writeln!(
        out,
        "    FROM json_each(?1->>'{descendant_table}') AS incoming"
    )?;
    writeln!(out, "    WHERE")?;
    for (ancestor_name, descendant_name) in ancestor_key_mapping(ancestor, descendant) {
        writeln!(
            out,
            "      incoming.value->>'{descendant_name}' IS {ancestor_table}.{ancestor_name} AND"
        )?;
    }
    writeln!(out, "      NOT EXISTS (")?;
    writeln!(out, "        SELECT 1")?;
    writeln!(out, "        FROM {descendant_table} AS stored")?;
    writeln!(out, "        WHERE")?;
    let key = primary_key(descendant).collect::<Vec<_>>();
    let data = descendant
        .fields
        .iter()
        .filter(|(_, field)| {
            !matches!(field, FieldType::Id | FieldType::Records { .. }) && !field.is_timestamp()
        })
        .map(|(name, _)| name)
        .collect::<Vec<_>>();
    for (idx, name) in key.iter().chain(data.iter()).enumerate() {
        let is_last = idx == key.len() + data.len() - 1;
        let suffix = if is_last { "" } else { " AND" };
        writeln!(
            out,
            "          stored.{name} IS incoming.value->>'{name}'{suffix}"
        )?;
    }
    writeln!(out, "      )")?;
    writeln!(out, "  )")
}

fn write_deleted_change(
    out: &mut String,
    ancestor_table: &str,
    ancestor: &TableSchema,
    descendant_table: &str,
    descendant: &TableSchema,
) -> std::fmt::Result {
    writeln!(out, "  EXISTS (")?;
    writeln!(out, "    SELECT 1")?;
    writeln!(out, "    FROM {descendant_table} AS stored")?;
    writeln!(out, "    WHERE")?;
    for (ancestor_name, descendant_name) in ancestor_key_mapping(ancestor, descendant) {
        writeln!(
            out,
            "      stored.{descendant_name} IS {ancestor_table}.{ancestor_name} AND"
        )?;
    }
    writeln!(out, "      NOT EXISTS (")?;
    writeln!(out, "        SELECT 1")?;
    writeln!(
        out,
        "        FROM json_each(?1->>'{descendant_table}') AS incoming"
    )?;
    writeln!(out, "        WHERE")?;
    let key = primary_key(descendant).collect::<Vec<_>>();
    for (idx, name) in key.iter().enumerate() {
        let suffix = if idx == key.len() - 1 { "" } else { " AND" };
        writeln!(
            out,
            "          incoming.value->>'{name}' IS stored.{name}{suffix}"
        )?;
    }
    writeln!(out, "      )")?;
    writeln!(out, "  )")
}

pub fn generate(
    out: &mut String,
    table: &str,
    table_schema: &TableSchema,
    schema: &CollectionSchema,
) -> std::fmt::Result {
    let Some(updated_at) = &table_schema.updated_at_name else {
        return Ok(());
    };
    let descendants = schema
        .tables
        .iter()
        .filter(|(candidate, _)| is_descendant(schema, candidate, table))
        .collect::<Vec<_>>();
    if descendants.is_empty() {
        return Ok(());
    }

    writeln!(out, "UPDATE {table}")?;
    writeln!(out, "SET {updated_at} = {CURRENT_TIMESTAMP}")?;
    writeln!(out, "WHERE")?;
    for (idx, (descendant_table, descendant)) in descendants.iter().enumerate() {
        write_incoming_change(out, table, table_schema, descendant_table, descendant)?;
        writeln!(out, "  OR")?;
        write_deleted_change(out, table, table_schema, descendant_table, descendant)?;
        if idx == descendants.len() - 1 {
            writeln!(out, ";")?;
        } else {
            writeln!(out, "  OR")?;
        }
    }
    Ok(())
}
