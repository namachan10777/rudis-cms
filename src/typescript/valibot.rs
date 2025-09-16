use std::fmt::Write;

use crate::{
    config,
    schema::{self, FieldType, TableSchema},
};

fn storage_pointer(storage: &config::Storage) -> &'static str {
    match storage {
        config::Storage::R2 { .. } => "r2StoragePointer",
        config::Storage::Asset { .. } => "assetStoragePointer",
        config::Storage::Kv { .. } => "kvStoragePointer",
        config::Storage::Inline => "inlineStoragePointer",
    }
}

fn generate_markdown_keep_validators(
    out: &mut String,
    camel_case: &str,
    image_storage: &config::Storage,
) {
    write!(out, "export type {camel_case}Validator = v.union([");
    for keep in [
        "alertKeep",
        "footnoteReferenceKeep",
        "linkCardKeep",
        "codeblockKeep",
        "headingKeep",
        "imageKeep",
    ] {
        if keep == "imageKeep" {
            write!(
                out,
                "\n| rudis.{keep}(rudis.{})",
                storage_pointer(image_storage)
            );
        } else {
            write!(out, "\n| rudis.{keep}");
        }
    }
    writeln!(out, ";");
}

fn generate_column_type(out: &mut String, name: &str, field: &schema::FieldType) {
    match field {
        FieldType::Markdown { storage, image, .. } => {
            let camel_case = stringcase::camel_case(name);
            generate_markdown_keep_validators(out, &camel_case, &image.storage);
            writeln!(
                out,
                "export const {camel_case}Root = rudis.markdownRoot({camel_case}Keep);"
            );
            if !matches!(storage, config::Storage::Inline) {
                writeln!(
                    out,
                    "export const {camel_case}Document = rudis.markdownDocument(frontmatter, {camel_case}Keep);"
                );
            }
            writeln!(
                out,
                "export type {camel_case}Column = rudis.markdownReference(rudis.{});",
                storage_pointer(&storage)
            );
        }
        FieldType::File { storage, .. } => {
            let camel_case = stringcase::camel_case(name);
            writeln!(
                out,
                "export type {camel_case}Column = rudis.fileReference(rudis.{});",
                storage_pointer(storage)
            );
        }
        FieldType::Image { storage, .. } => {
            let camel_case = stringcase::camel_case(name);
            writeln!(
                out,
                "export type {camel_case}Column = rudis.imageReference(rudis.{});",
                storage_pointer(storage)
            );
        }
        _ => {}
    }
}

fn generate_table_validator_field(out: &mut String, name: &str, field: &FieldType) {
    write!(out, "{name}: ");
    if !field.is_required_field() {
        write!(out, "v.nullable(");
    }
    match field {
        FieldType::Boolean { .. } => {
            write!(out, "v.boolean()");
        }
        FieldType::Id | FieldType::Hash | FieldType::String { .. } => {
            write!(out, "v.string()");
        }
        FieldType::Integer { .. } => {
            write!(out, "v.pipe(v.number(), v.integer())");
        }
        FieldType::Real { .. } => {
            write!(out, "v.number()");
        }
        FieldType::Date { .. } => {
            write!(out, "v.date()");
        }
        FieldType::Datetime { .. } => {
            write!(out, "v.date()");
        }
        FieldType::Image { .. } | FieldType::File { .. } | FieldType::Markdown { .. } => {
            write!(
                out,
                "v.pipe(v.string(), v.parseJson(), {}Column)",
                stringcase::camel_case(name)
            );
        }
        FieldType::Records { .. } => {}
    }
    if !field.is_required_field() {
        writeln!(out, "),");
    } else {
        writeln!(out, ",");
    }
}

fn generate_table_validator<'o, 'i>(
    out: &'o mut String,
    fields: impl Iterator<Item = (&'i String, &'i FieldType)>,
) {
    writeln!(out, "export const table = v.object({{");
    fields.for_each(|(name, field)| {
        generate_table_validator_field(out, name, field);
    });
    writeln!(out, "}});");
}

fn generate_frontmatter_type<'o, 'i>(
    out: &'o mut String,
    fields: impl Iterator<Item = (&'i String, &'i FieldType)>,
) {
    write!(out, "export const frontmatter = v.object({{");
    fields.for_each(|(name, field)| {
        match field {
            FieldType::Markdown { .. } => {}
            FieldType::Records { table, .. } => {
                writeln!(
                    out,
                    "{name}: v.array({table}.frontmatterWithMarkdownColumns),"
                );
            }
            field => generate_table_validator_field(out, name, field),
        }
        generate_table_validator_field(out, name, field);
    });
    writeln!(out, "}});");
}

fn generate_frontmatter_with_markdown_columns_type<'o, 'i>(
    out: &'o mut String,
    fields: impl Iterator<Item = (&'i String, &'i FieldType)>,
) {
    write!(
        out,
        "export const frontmatterWithMarkdownColumns = v.object({{"
    );
    fields.for_each(|(name, field)| {
        match field {
            FieldType::Records { table, .. } => {
                writeln!(
                    out,
                    "{name}: v.array({table}.frontmatterWithMarkdownColumns),"
                );
            }
            field => generate_table_validator_field(out, name, field),
        }
        generate_table_validator_field(out, name, field);
    });
    writeln!(out, "}});");
}

fn generate_sub_table_imports<'i, 'o>(
    out: &'o mut String,
    fields: impl Iterator<Item = &'i FieldType>,
) {
    fields.for_each(|field| {
        if let &FieldType::Records { ref table, .. } = field {
            writeln!(out, r#"import * as {table} from "./table-validator.ts""#);
        }
    });
}

pub fn generate_type(out: &mut String, schema: &TableSchema) {
    writeln!(out, r#"import * as rudis from "../rudis-valibot.ts""#);
    writeln!(out, r#"import * as v from "valibot";"#);
    generate_sub_table_imports(out, schema.fields.values());
    schema
        .fields
        .iter()
        .for_each(|(name, field)| generate_column_type(out, name, field));
    generate_table_validator(out, schema.fields.iter());
    generate_frontmatter_type(out, schema.fields.iter());
    generate_frontmatter_with_markdown_columns_type(out, schema.fields.iter());
}
