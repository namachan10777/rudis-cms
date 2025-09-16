use std::{fmt::Write, path::PathBuf};

use indexmap::IndexMap;

use crate::{
    config,
    schema::{self, CollectionSchema, FieldType, TableSchema},
};

mod valibot;

fn upper_camel_case(s: &str) -> String {
    stringcase::camel_case(s)
        .char_indices()
        .map(|(i, c)| if i == 0 { c.to_ascii_uppercase() } else { c })
        .collect::<String>()
}

fn storage_pointer(storage: &config::Storage) -> &'static str {
    match storage {
        config::Storage::R2 { .. } => "R2StoragePointer",
        config::Storage::Asset { .. } => "AssetStoragePointer",
        config::Storage::Kv { .. } => "KvStoragePointer",
        config::Storage::Inline => "InlineStoragePointer",
    }
}

fn generate_markdown_keep_types(
    out: &mut String,
    upper_camel_case: &str,
    image_storage: &config::Storage,
) -> std::fmt::Result {
    write!(out, "export type {upper_camel_case}Keep = ")?;
    for keep in [
        "AlertKeep",
        "FootnoteReferenceKeep",
        "LinkCardKeep",
        "CodeblockKeep",
        "HeadingKeep",
        "ImageKeep",
    ] {
        if keep == "ImageKeep" {
            write!(
                out,
                "\n| rudis.{keep}<rudis.{}>",
                storage_pointer(image_storage)
            )?;
        } else {
            write!(out, "\n| rudis.{keep}")?;
        }
    }
    writeln!(out, ";")
}

fn generate_column_type(
    out: &mut String,
    name: &str,
    field: &schema::FieldType,
) -> std::fmt::Result {
    match field {
        FieldType::Markdown { storage, image, .. } => {
            let upper_camel_case = upper_camel_case(name);
            generate_markdown_keep_types(out, &upper_camel_case, &image.storage)?;
            writeln!(
                out,
                "export type {upper_camel_case}Root = rudis.MarkdownRoot<{upper_camel_case}Keep>;"
            )?;
            if !matches!(storage, config::Storage::Inline) {
                writeln!(
                    out,
                    "export type {upper_camel_case}Document = rudis.MarkdownRoot<Frontmatter, {upper_camel_case}Keep>;"
                )?;
            }
            writeln!(
                out,
                "export type {upper_camel_case}Column = rudis.MarkdownReference<rudis.{}>;",
                storage_pointer(&storage)
            )?;
        }
        FieldType::File { storage, .. } => {
            let upper_camel_case = upper_camel_case(name);
            writeln!(
                out,
                "export type {upper_camel_case}Column = rudis.FileReference<rudis.{}>;",
                storage_pointer(storage)
            )?;
        }
        FieldType::Image { storage, .. } => {
            let upper_camel_case = upper_camel_case(name);
            writeln!(
                out,
                "export type {upper_camel_case}Column = rudis.ImageReference<rudis.{}>;",
                storage_pointer(storage)
            )?;
        }
        _ => {}
    }
    Ok(())
}

fn generate_table_type_field(out: &mut String, name: &str, field: &FieldType) -> std::fmt::Result {
    write!(out, "{name}: ")?;
    match field {
        FieldType::Boolean { .. } => {
            write!(out, "boolean")?;
        }
        FieldType::Id => {
            write!(out, "string")?;
        }
        FieldType::Hash => {
            write!(out, "hash")?;
        }
        FieldType::String { .. } => {
            write!(out, "string")?;
        }
        FieldType::Integer { .. } => {
            write!(out, "number")?;
        }
        FieldType::Real { .. } => {
            write!(out, "number")?;
        }
        FieldType::Date { .. } => {
            write!(out, "Date")?;
        }
        FieldType::Datetime { .. } => {
            write!(out, "Date")?;
        }
        FieldType::Image { .. } => {
            write!(out, "{}Column", upper_camel_case(name))?;
        }
        FieldType::File { .. } => {
            write!(out, "{}Column", upper_camel_case(name))?;
        }
        FieldType::Markdown { .. } => {
            write!(out, "{}Column", upper_camel_case(name))?;
        }
        FieldType::Records { .. } => {}
    }
    if !field.is_required_field() {
        writeln!(out, "| null;")?;
    } else {
        writeln!(out, ";")?;
    }
    Ok(())
}

fn generate_table_type<'o, 'i>(
    out: &'o mut String,
    mut fields: impl Iterator<Item = (&'i String, &'i FieldType)>,
) -> std::fmt::Result {
    write!(out, "export interface Table {{")?;
    fields.try_for_each(|(name, field)| generate_table_type_field(out, name, field))?;
    writeln!(out, "}}")
}

fn generate_frontmatter_type<'o, 'i>(
    out: &'o mut String,
    mut fields: impl Iterator<Item = (&'i String, &'i FieldType)>,
) -> std::fmt::Result {
    write!(out, "export interface Frontmatter {{")?;
    fields.try_for_each(|(name, field)| {
        match field {
            FieldType::Markdown { .. } => {}
            FieldType::Records { table, .. } => {
                writeln!(out, "{name}: {table}.FrontmatterWithMarkdownColumns[];")?
            }
            field => generate_table_type_field(out, name, field)?,
        }
        generate_table_type_field(out, name, field)
    })?;
    writeln!(out, "}}")
}

fn generate_frontmatter_with_markdown_columns_type<'o, 'i>(
    out: &'o mut String,
    mut fields: impl Iterator<Item = (&'i String, &'i FieldType)>,
) -> std::fmt::Result {
    write!(out, "export interface FrontmatterWithMarkdownColumns {{")?;
    fields.try_for_each(|(name, field)| {
        match field {
            FieldType::Records { table, .. } => {
                writeln!(out, "{name}: {table}.FrontmatterWithMarkdownColumns[];")?
            }
            field => generate_table_type_field(out, name, field)?,
        }
        generate_table_type_field(out, name, field)
    })?;
    writeln!(out, "}}")
}

fn generate_sub_table_imports<'i, 'o>(
    out: &'o mut String,
    mut fields: impl Iterator<Item = &'i FieldType>,
) -> std::fmt::Result {
    fields.try_for_each(|field| {
        if let &FieldType::Records { ref table, .. } = field {
            writeln!(out, r#"import * as {table} from "./table.ts""#)?;
        }
        Ok(())
    })
}

pub fn generate_type(out: &mut String, schema: &TableSchema) -> std::fmt::Result {
    writeln!(out, r#"import * as rudis from "../rudis.ts""#)?;
    generate_sub_table_imports(out, schema.fields.values())?;
    schema
        .fields
        .iter()
        .try_for_each(|(name, field)| generate_column_type(out, name, field))?;
    generate_table_type(out, schema.fields.iter())?;
    generate_frontmatter_type(out, schema.fields.iter())?;
    generate_frontmatter_with_markdown_columns_type(out, schema.fields.iter())?;
    Ok(())
}

pub fn file_map(
    schema: &CollectionSchema,
    name: &str,
    enable_valibot: bool,
) -> IndexMap<PathBuf, String> {
    let mut map = IndexMap::new();
    for (table, schema) in &schema.tables {
        let mut contents = String::new();
        generate_type(&mut contents, schema).unwrap();
        map.insert(PathBuf::from(format!("{name}/{table}.ts")), contents);
    }
    if enable_valibot {
        for (table, schema) in &schema.tables {
            let mut contents = String::new();
            valibot::generate_type(&mut contents, schema).unwrap();
            map.insert(
                PathBuf::from(format!("{name}/{table}-valibot.ts")),
                contents,
            );
        }
    }
    map
}
