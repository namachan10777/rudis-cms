use std::sync::LazyLock;

use crate::schema;

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

const PARSER: LazyLock<liquid::Parser> =
    LazyLock::new(|| liquid::ParserBuilder::with_stdlib().build().unwrap());

pub const DDL: LazyLock<liquid::Template> = LazyLock::new(|| {
    PARSER
        .parse(include_str!("./templates/ddl.sql.liquid"))
        .unwrap()
});

pub const INSERT: LazyLock<liquid::Template> = LazyLock::new(|| {
    PARSER
        .parse(include_str!("./templates/insert.sql.liquid"))
        .unwrap()
});

pub fn create_liquid_context(schema: &schema::TableSchemas) -> liquid::Object {
    let mut tables = schema
        .iter()
        .map(|(table, schema)| {
            let inherit_id_columns = schema.inherit_ids.iter().map(|id| {
                liquid::object!({
                    "name": id,
                    "type": "TEXT",
                    "not_null": true,
                    "is_primary_key": true,
                })
            });
            let columns = schema.fields.iter().filter_map(|(name, field)| {
                let ty = sqlite_type_name(field)?;
                Some(liquid::object!({
                    "name": name,
                    "type": ty,
                    "not_null": is_required(field),
                    "is_primary_key": matches!(field, schema::FieldType::Id),
                }))
            });
            let columns = inherit_id_columns.chain(columns).collect::<Vec<_>>();
            let indexes = schema
                .fields
                .iter()
                .filter_map(|(name, field)| {
                    if to_be_indexed(field) {
                        Some(liquid::object!({
                            "name": name,
                            "index": sqlite_index(name, field)
                        }))
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>();
            let parent = schema.parent.as_ref().map(|parent| {
                liquid::object!({
                    "table": parent.name,
                    "parent_ids": parent.id_names,
                    "local_ids": schema.inherit_ids,
                })
            });
            let mut primary_key = schema.inherit_ids.clone();
            primary_key.push(schema.id_name.clone());
            liquid::object!({
                "name": table,
                "columns": columns,
                "indexes": indexes,
                "parent": parent,
                "primary_key": primary_key,
            })
        })
        .collect::<Vec<_>>();
    tables.reverse();
    liquid::object!({
        "tables": tables,
    })
}
