use std::{
    path::{Path, PathBuf},
    sync::LazyLock,
};

use indexmap::IndexMap;

use crate::{
    config,
    schema::{CollectionSchema, FieldType, TableSchema},
};

fn sub_tables(schema: &TableSchema) -> Vec<&str> {
    schema
        .fields
        .iter()
        .filter_map(|(_, field)| match field {
            FieldType::Records { table, .. } => Some(table.as_str()),
            _ => None,
        })
        .collect()
}

fn camel_case(name: &str) -> String {
    stringcase::camel_case(name)
        .char_indices()
        .map(|(i, c)| {
            if i == 0 {
                c.to_lowercase().to_string()
            } else {
                c.to_string()
            }
        })
        .collect::<String>()
}

fn capital_camel_case(name: &str) -> String {
    stringcase::camel_case(name)
        .char_indices()
        .map(|(i, c)| {
            if i == 0 {
                c.to_uppercase().to_string()
            } else {
                c.to_string()
            }
        })
        .collect::<String>()
}

fn is_field_nullable(field: &FieldType) -> bool {
    match field {
        FieldType::Boolean { required, .. } => !*required,
        FieldType::Date { required, .. } => !*required,
        FieldType::Datetime { required, .. } => !*required,
        FieldType::File { required, .. } => !*required,
        FieldType::Hash => false,
        FieldType::Id => false,
        FieldType::String { required, .. } => !*required,
        FieldType::Image { required, .. } => !*required,
        FieldType::Markdown { required, .. } => !*required,
        FieldType::Real { required, .. } => !*required,
        FieldType::Integer { required, .. } => !*required,
        FieldType::Records { .. } => false,
    }
}

fn ts_type_name(field: &FieldType) -> Option<&'static str> {
    match field {
        FieldType::Boolean { .. } => Some("boolean"),
        FieldType::Date { .. } => Some("Date"),
        FieldType::Datetime { .. } => Some("Date"),
        FieldType::File { .. } => None,
        FieldType::Hash => Some("string"),
        FieldType::Id => Some("string"),
        FieldType::String { .. } => Some("string"),
        FieldType::Image { .. } => None,
        FieldType::Markdown { .. } => None,
        FieldType::Real { .. } => Some("number"),
        FieldType::Integer { .. } => Some("number"),
        FieldType::Records { .. } => None,
    }
}

fn markdown_type(storage: &config::MarkdownStorage, name: &str) -> String {
    match storage {
        config::MarkdownStorage::Inline => {
            format!(
                "MarkdownInlineStorageColumn<{}Keep>",
                capital_camel_case(name)
            )
        }
        config::MarkdownStorage::Kv { .. } => "MarkdownKvStorageColumn".into(),
    }
}

fn markdown_schema(storage: &config::MarkdownStorage) -> &'static str {
    match storage {
        config::MarkdownStorage::Inline => "markdownInlineStorageColumn",
        config::MarkdownStorage::Kv { .. } => "markdownKvStorageColumn",
    }
}

fn image_storage_pointer(storage: &config::ImageStorage) -> &'static str {
    match storage {
        config::ImageStorage::R2 { .. } => "R2StoragePointer",
        config::ImageStorage::Asset { .. } => "AssetStoragePointer",
    }
}

fn markdown_keeps(_: &config::MarkdownConfig, image: &config::MarkdownImageConfig) -> Vec<String> {
    vec![
        format!("Image<rudis.{}>", image_storage_pointer(&image.storage)),
        "Alert".into(),
        "Codeblock".into(),
        "FootnoteReference".into(),
        "Heading".into(),
        "LinkCard".into(),
    ]
}

fn image_storage(storage: &config::ImageStorage) -> &'static str {
    match storage {
        config::ImageStorage::R2 { .. } => "R2StoragePointer",
        config::ImageStorage::Asset { .. } => "AssetStoragePointer",
    }
}

fn image_storage_schema(storage: &config::ImageStorage) -> &'static str {
    match storage {
        config::ImageStorage::R2 { .. } => "r2StoragePointer",
        config::ImageStorage::Asset { .. } => "assetStoragePointer",
    }
}

fn file_storage(storage: &config::FileStorage) -> &'static str {
    match storage {
        config::FileStorage::R2 { .. } => "r2StoragePointer",
        config::FileStorage::Asset { .. } => "assetStoragePointer",
    }
}

fn file_storage_schema(storage: &config::FileStorage) -> &'static str {
    match storage {
        config::FileStorage::R2 { .. } => "r2StoragePointer",
        config::FileStorage::Asset { .. } => "assetStoragePointer",
    }
}

fn ctx(schema: &TableSchema) -> liquid::Object {
    let columns = schema.fields.iter().map(|(name, field)| match field {
        FieldType::Id
        | FieldType::Boolean { .. }
        | FieldType::Real { .. }
        | FieldType::Integer { .. }
        | FieldType::String { .. }
        | FieldType::Hash => liquid::object!({
            "kind": "primitive",
            "name": name,
            "camel_case": camel_case(name),
            "capital_camel_case": capital_camel_case(name),
            "nullable": is_field_nullable(field),
            "inherited": false,
            "type": ts_type_name(field).unwrap(),
        }),
        FieldType::Date { .. } | FieldType::Datetime { .. } => liquid::object!({
            "kind": "datetime",
            "name": name,
            "camel_case": camel_case(name),
            "capital_camel_case": capital_camel_case(name),
            "nullable": is_field_nullable(field),
            "inherited": false,
            "type": ts_type_name(field).unwrap(),
        }),
        FieldType::Markdown {
            config,
            storage,
            image,
            ..
        } => {
            liquid::object!({
                "kind": "markdown",
                "name": name,
                "camel_case": camel_case(name),
                "capital_camel_case": capital_camel_case(name),
                "nullable": is_field_nullable(field),
                "inherited": false,
                "markdown_type": markdown_type(storage, name),
                "schema": markdown_schema(storage),
                "keeps": markdown_keeps(config, image),
            })
        }
        FieldType::Image { storage, .. } => {
            liquid::object!({
                "kind": "image",
                "name": name,
                "camel_case": camel_case(name),
                "capital_camel_case": capital_camel_case(name),
                "nullable": is_field_nullable(field),
                "inherited": false,
                "storage": image_storage(storage),
                "storage_schema": image_storage_schema(storage),
            })
        }
        FieldType::Records { table, .. } => {
            liquid::object!({
                "kind": "records",
                "name": name,
                "camel_case": camel_case(name),
                "capital_camel_case": capital_camel_case(name),
                "inherited": false,
                "table": table,
            })
        }
        FieldType::File { storage, .. } => {
            liquid::object!({
                "kind": "file",
                "name": name,
                "camel_case": camel_case(name),
                "capital_camel_case": capital_camel_case(name),
                "nullable": is_field_nullable(field),
                "storage": file_storage(storage),
                "storage_schema": file_storage_schema(storage),
                "inherited": false,
            })
        }
    });

    let inherited_columns = schema.inherit_ids.iter().map(|name| {
        liquid::object!({
            "kind": "primitive",
            "name": name,
            "camel_case": camel_case(name),
            "capital_camel_case": capital_camel_case(name),
            "nullable": false,
            "inherited": true,
            "type": "string",
        })
    });

    let columns = inherited_columns.chain(columns).collect::<Vec<_>>();
    liquid::object!({
        "columns": columns,
        "sub_tables": sub_tables(schema),
    })
}

static TEMPLATE: LazyLock<liquid::Template> = LazyLock::new(|| {
    liquid::ParserBuilder::with_stdlib()
        .build()
        .unwrap()
        .parse(include_str!("./table.ts.liquid"))
        .unwrap()
});

pub const RUDIS_TYPE_LIB: &str = include_str!("./rudis.ts.txt");

pub fn render(schema: &CollectionSchema) -> IndexMap<PathBuf, String> {
    schema
        .tables
        .iter()
        .map(|(name, table)| {
            let name: &Path = name.as_ref();
            let content = TEMPLATE.render(&ctx(table)).unwrap();
            (name.with_extension("ts"), content)
        })
        .collect()
}
