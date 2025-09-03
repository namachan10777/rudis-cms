use std::sync::LazyLock;

use indexmap::IndexMap;

use crate::schema;

fn is_object_field(field: &schema::FieldType) -> bool {
    matches!(
        field,
        schema::FieldType::File { .. }
            | schema::FieldType::Image { .. }
            | schema::FieldType::Markdown { .. }
    )
}

fn sqlite_type_name(field: &schema::FieldType) -> Option<&'static str> {
    let ty = match field {
        schema::FieldType::Boolean { .. } => "INTEGER",
        schema::FieldType::Date { .. } => "TEXT",
        schema::FieldType::Datetime { .. } => "TEXT",
        schema::FieldType::File { .. } => "TEXT",
        schema::FieldType::Hash => "TEXT",
        schema::FieldType::Id => "TEXT",
        schema::FieldType::Image { .. } => "TEXT",
        schema::FieldType::Integer { .. } => "INTEGER",
        schema::FieldType::Markdown { .. } => "TEXT",
        schema::FieldType::Real { .. } => "REAL",
        schema::FieldType::String { .. } => "TEXT",
        schema::FieldType::Records { .. } => return None,
    };
    Some(ty)
}

fn sqlite_index(name: &str, field: &schema::FieldType) -> String {
    match field {
        schema::FieldType::Date { .. } => format!("date({name})"),
        schema::FieldType::Datetime { .. } => format!("datetime({name})"),
        _ => name.to_owned(),
    }
}

fn to_be_indexed(field: &schema::FieldType) -> bool {
    match field {
        schema::FieldType::Boolean { index, .. } => *index,
        schema::FieldType::Date { index, .. } => *index,
        schema::FieldType::Datetime { index, .. } => *index,
        schema::FieldType::Image { .. } => false,
        schema::FieldType::Integer { index, .. } => *index,
        schema::FieldType::Real { index, .. } => *index,
        schema::FieldType::String { index, .. } => *index,
        schema::FieldType::Markdown { .. } => false,
        schema::FieldType::Id => true,
        schema::FieldType::Hash => true,
        schema::FieldType::File { .. } => false,
        schema::FieldType::Records { .. } => false,
    }
}

fn is_required(field: &schema::FieldType) -> bool {
    match field {
        schema::FieldType::Boolean { required, .. } => *required,
        schema::FieldType::Date { required, .. } => *required,
        schema::FieldType::Datetime { required, .. } => *required,
        schema::FieldType::Image { .. } => false,
        schema::FieldType::Integer { required, .. } => *required,
        schema::FieldType::Real { required, .. } => *required,
        schema::FieldType::String { required, .. } => *required,
        schema::FieldType::Markdown { required, .. } => *required,
        schema::FieldType::Id => true,
        schema::FieldType::Hash => true,
        schema::FieldType::File { required, .. } => *required,
        schema::FieldType::Records { .. } => false,
    }
}

const LIQUID_TEMPLATE_PARSER: LazyLock<liquid::Parser> =
    LazyLock::new(|| liquid::ParserBuilder::with_stdlib().build().unwrap());

pub const SQL_DDL: LazyLock<liquid::Template> = LazyLock::new(|| {
    LIQUID_TEMPLATE_PARSER
        .parse(include_str!("./templates/ddl.sql.liquid"))
        .unwrap()
});

fn liquid_inherit_id_columns(schema: &schema::TableSchema) -> impl Iterator<Item = liquid::Object> {
    schema.inherit_ids.iter().map(|id| {
        liquid::object!({
            "name": id,
            "type": "TEXT",
            "not_null": true,
            "is_primary_key": true,
        })
    })
}

fn liquid_data_columns(schema: &schema::TableSchema) -> impl Iterator<Item = liquid::Object> {
    schema.fields.iter().filter_map(|(name, field)| {
        let ty = sqlite_type_name(field)?;
        if matches!(field, schema::FieldType::Id) {
            return None;
        }
        Some(liquid::object!({
            "name": name,
            "type": ty,
            "not_null": is_required(field),
            "is_primary_key": matches!(field, schema::FieldType::Id),
        }))
    })
}

fn liqui_id_column(schema: &schema::TableSchema) -> liquid::Object {
    liquid::object!({
        "name": schema.id_name,
        "type": "TEXT",
        "not_null": true,
        "is_primary_key": true,
    })
}

fn liquid_internal_column_indexes(
    schema: &schema::TableSchema,
) -> impl Iterator<Item = liquid::Object> {
    schema.fields.iter().filter_map(|(name, field)| {
        if to_be_indexed(field) {
            Some(liquid::object!({
                "name": name,
                "index": sqlite_index(name, field)
            }))
        } else {
            None
        }
    })
}

fn liquid_object_column_hash_indexes(
    schema: &schema::TableSchema,
) -> impl Iterator<Item = liquid::Object> {
    schema.fields.iter().filter_map(|(name, field)| {
        if is_object_field(field) {
            Some(liquid::object!({
                "name": name,
                "index": format!("{name}->>'hash'"),
                "where": if is_required(field) { Some("IS NOT NULL") } else { None },
            }))
        } else {
            None
        }
    })
}

fn liquid_parent_table(schema: &schema::TableSchema) -> Option<liquid::Object> {
    schema.parent.as_ref().map(|parent| {
        liquid::object!({
            "table": parent.name,
            "parent_ids": parent.id_names,
            "local_ids": schema.inherit_ids,
        })
    })
}

fn liquid_primary_key(schema: &schema::TableSchema) -> Vec<String> {
    let mut primary_key = schema.inherit_ids.clone();
    primary_key.push(schema.id_name.clone());
    primary_key
}

fn liquid_object_columns(schema: &schema::CollectionSchema) -> Vec<liquid::Object> {
    schema
        .tables
        .iter()
        .flat_map(|(table, schema)| {
            schema.fields.iter().filter_map(|(column, field)| {
                if is_object_field(field) {
                    Some(liquid::object!({
                        "name": column.clone(),
                        "table": table.clone(),
                    }))
                } else {
                    None
                }
            })
        })
        .collect()
}

fn liquid_tables(schema: &schema::CollectionSchema) -> IndexMap<String, liquid::Object> {
    schema
        .tables
            .iter()
            .map(|(table, schema)| {
                let ctx = liquid::object!({
                    "name": table,
                    "id_name": schema.id_name,
                    "columns": liquid_inherit_id_columns(schema).chain(std::iter::once(liqui_id_column(schema))).chain(liquid_data_columns(schema)).collect::<Vec<_>>(),
                    "data_columns": liquid_data_columns(schema).collect::<Vec<_>>(),
                    "indexes": liquid_internal_column_indexes(schema).chain(liquid_object_column_hash_indexes(schema)).collect::<Vec<_>>(),
                    "parent": liquid_parent_table(schema),
                    "primary_key": liquid_primary_key(schema),
                });
                (table.clone(), ctx)
            })
            .collect::<IndexMap<String, _>>()
}

pub fn liquid_default_context(schema: &schema::CollectionSchema) -> liquid::Object {
    liquid::object!({
        "tables": liquid_tables(schema).into_values().collect::<Vec<_>>(),
        "object_columns": liquid_object_columns(schema),
    })
}

pub const SQL_FETCH_ALL_OBJECT: LazyLock<liquid::Template> = LazyLock::new(|| {
    LIQUID_TEMPLATE_PARSER
        .parse(include_str!("./templates/fetch_all_hash.liquid"))
        .unwrap()
});

pub const SQL_UPSERT: LazyLock<liquid::Template> = LazyLock::new(|| {
    LIQUID_TEMPLATE_PARSER
        .parse(include_str!("./templates/upsert.liquid"))
        .unwrap()
});

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("SQL error: {0}")]
    Sql(rusqlite::Error),
    #[error("Parse hash error: {0}")]
    ParseHash(blake3::HexError),
}

impl From<rusqlite::Error> for Error {
    fn from(value: rusqlite::Error) -> Self {
        Self::Sql(value)
    }
}
