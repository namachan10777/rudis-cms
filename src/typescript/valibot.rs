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
) -> std::fmt::Result {
    writeln!(out, "export const {camel_case}Keep = v.union([")?;
    for keep in [
        "alertKeep",
        "footnoteReferenceKeep",
        "linkCardKeep",
        "codeblockKeep",
        "headingKeep",
        "imageKeep",
    ] {
        if keep == "imageKeep" {
            writeln!(
                out,
                "  rudis.{keep}(rudis.{}),",
                storage_pointer(image_storage)
            )?;
        } else {
            writeln!(out, "  rudis.{keep},")?;
        }
    }
    writeln!(out, "]);")
}

fn generate_markdown_column_validator(
    out: &mut String,
    name: &str,
    field: &schema::FieldType,
) -> std::fmt::Result {
    match field {
        FieldType::Markdown { storage, image, .. } => {
            let camel_case = stringcase::camel_case(name);
            generate_markdown_keep_validators(out, &camel_case, &image.storage)?;
            writeln!(
                out,
                "export const {camel_case}Root = rudis.markdownRoot({camel_case}Keep);"
            )?;
            if !matches!(storage, config::Storage::Inline) {
                writeln!(
                    out,
                    "export const {camel_case}Document = rudis.markdownDocument(frontmatter, {camel_case}Keep);"
                )?;
            }
            writeln!(
                out,
                "export const {camel_case}Column = rudis.markdownReference(rudis.{});",
                storage_pointer(storage)
            )
        }
        _ => Ok(()),
    }
}

fn generate_column_validator(
    out: &mut String,
    name: &str,
    field: &schema::FieldType,
) -> std::fmt::Result {
    match field {
        FieldType::File { storage, .. } => {
            let camel_case = stringcase::camel_case(name);
            writeln!(
                out,
                "export const {camel_case}Column = rudis.fileReference(rudis.{});",
                storage_pointer(storage)
            )
        }
        FieldType::Image { storage, .. } => {
            let camel_case = stringcase::camel_case(name);
            writeln!(
                out,
                "export const {camel_case}Column = rudis.imageReference(rudis.{});",
                storage_pointer(storage)
            )
        }
        _ => Ok(()),
    }
}

fn generate_table_validator_field(
    out: &mut String,
    name: &str,
    field: &FieldType,
    sqlite: bool,
) -> std::fmt::Result {
    if matches!(field, FieldType::Records { .. }) {
        return Ok(());
    }
    write!(out, "  {name}: ")?;
    if !field.is_required_field() {
        write!(out, "v.nullable(")?;
    }
    match field {
        FieldType::Boolean { .. } if sqlite => {
            write!(
                out,
                "v.pipe(v.number(), v.integer(), v.transform((flag) => flag === 1), v.boolean())"
            )?;
        }
        FieldType::Boolean { .. } => {
            write!(out, "v.boolean()")?;
        }
        FieldType::Id | FieldType::Hash | FieldType::String { .. } => {
            write!(out, "v.string()")?;
        }
        FieldType::Integer { .. } => {
            write!(out, "v.pipe(v.number(), v.integer())")?;
        }
        FieldType::Real { .. } => {
            write!(out, "v.number()")?;
        }
        FieldType::Date { .. } => {
            write!(out, "v.pipe(v.string(), v.isoDate())")?;
        }
        FieldType::Datetime { .. } => {
            write!(
                out,
                "v.pipe(v.string(), v.transform((datetime) => new Date(datetime)))"
            )?;
        }
        FieldType::Image { .. } | FieldType::File { .. } | FieldType::Markdown { .. } if sqlite => {
            write!(
                out,
                "v.pipe(v.string(), v.parseJson(), {}Column)",
                stringcase::camel_case(name)
            )?;
        }
        FieldType::Image { .. } | FieldType::File { .. } | FieldType::Markdown { .. } => {
            write!(out, "{}Column", stringcase::camel_case(name))?;
        }
        FieldType::Records { .. } => return Ok(()),
    }
    if !field.is_required_field() {
        writeln!(out, "),")
    } else {
        writeln!(out, ",")
    }
}

fn generate_table_validator<'o, 'i>(
    out: &'o mut String,
    mut fields: impl Iterator<Item = (&'i String, &'i FieldType)>,
) -> std::fmt::Result {
    writeln!(out, "export const table = v.object({{")?;
    fields.try_for_each(|(name, field)| generate_table_validator_field(out, name, field, true))?;
    writeln!(out, "}});")
}

fn generate_frontmatter_validator<'o, 'i>(
    out: &'o mut String,
    mut fields: impl Iterator<Item = (&'i String, &'i FieldType)>,
) -> std::fmt::Result {
    writeln!(out, "export const frontmatter = v.object({{")?;
    fields.try_for_each(|(name, field)| match field {
        FieldType::Markdown { .. } => Ok(()),
        FieldType::Records { table, .. } => {
            writeln!(
                out,
                "  {name}: v.array({table}.frontmatterWithMarkdownColumns),"
            )
        }
        field => generate_table_validator_field(out, name, field, false),
    })?;
    writeln!(out, "}});")
}

fn generate_frontmatter_with_markdown_columns_validor<'o, 'i>(
    out: &'o mut String,
    mut fields: impl Iterator<Item = (&'i String, &'i FieldType)>,
) -> std::fmt::Result {
    writeln!(
        out,
        "export const frontmatterWithMarkdownColumns = v.object({{"
    )?;
    fields.try_for_each(|(name, field)| match field {
        FieldType::Records { table, .. } => writeln!(
            out,
            "  {name}: v.array({table}.frontmatterWithMarkdownColumns),"
        ),
        field => generate_table_validator_field(out, name, field, false),
    })?;
    writeln!(out, "}});")
}

fn generate_sub_table_imports<'i, 'o>(
    out: &'o mut String,
    mut fields: impl Iterator<Item = &'i FieldType>,
) -> std::fmt::Result {
    fields.try_for_each(|field| {
        if let FieldType::Records { table, .. } = field {
            writeln!(out, r#"import * as {table} from "./{table}-valibot""#)?;
        }
        Ok(())
    })
}

pub fn generate_type(out: &mut String, schema: &TableSchema) -> std::fmt::Result {
    writeln!(out, r#"import * as rudis from "../rudis-valibot""#)?;
    writeln!(out, r#"import * as v from "valibot";"#)?;
    schema
        .fields
        .iter()
        .try_for_each(|(name, field)| generate_column_validator(out, name, field))?;
    generate_frontmatter_validator(out, schema.fields.iter())?;
    schema
        .fields
        .iter()
        .try_for_each(|(name, field)| generate_markdown_column_validator(out, name, field))?;
    generate_sub_table_imports(out, schema.fields.values())?;

    generate_table_validator(out, schema.fields.iter())?;
    generate_frontmatter_with_markdown_columns_validor(out, schema.fields.iter())?;
    Ok(())
}
