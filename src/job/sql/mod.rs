use std::sync::LazyLock;

use crate::schema::{self, FieldType, TableSchema};

fn primary_keys(schema: &TableSchema) -> Vec<liquid::Object> {
    schema
        .inherit_ids
        .iter()
        .map(|id| {
            liquid::object!({
                "name": id,
                "reference": true
            })
        })
        .chain(std::iter::once(liquid::object!({
            "name": schema.id_name,
            "reference": false
        })))
        .collect::<Vec<_>>()
}

fn sqlite_type_name(field: &FieldType) -> Option<&'static str> {
    match field {
        FieldType::Boolean { .. } => Some("INTEGER"),
        FieldType::Date { .. } => Some("TEXT"),
        FieldType::Datetime { .. } => Some("TEXT"),
        FieldType::File { .. } => Some("TEXT"),
        FieldType::Hash => Some("TEXT"),
        FieldType::Image { .. } => Some("TEXT"),
        FieldType::Integer { .. } => Some("REAL"),
        FieldType::String { .. } => Some("TEXT"),
        FieldType::Markdown { .. } => Some("TEXT"),
        FieldType::Id => Some("TEXT"),
        FieldType::Real { .. } => Some("FLOATING"),
        FieldType::Records { .. } => None,
    }
}

fn data_column(name: &str, field: &FieldType) -> Option<liquid::Object> {
    let ty = sqlite_type_name(field)?;
    if matches!(field, FieldType::Id) {
        return None;
    }
    let index = if field.requires_index() {
        Some(match field {
            FieldType::Date { .. } => format!("date({name})"),
            FieldType::Datetime { .. } => format!("datetime({name})"),
            _ => name.to_string(),
        })
    } else {
        None
    };
    Some(liquid::object!({
        "name": name,
        "type": ty,
        "nullable": !field.is_required_field(),
        "index": index,
    }))
}

fn data_columns(schema: &TableSchema) -> Vec<liquid::Object> {
    schema
        .fields
        .iter()
        .flat_map(|(name, field)| data_column(name, field))
        .collect::<Vec<_>>()
}

fn table_liquid_context(name: &str, schema: &TableSchema) -> liquid::Object {
    let parent = schema.parent.as_ref().map(|parent| {
        liquid::object!({
            "table": parent.name,
            "parent_ids": parent.id_names,
        })
    });
    liquid::object!({
        "table": name,
        "primary_keys": primary_keys(schema),
        "data_columns": data_columns(schema),
        "parent": parent,
    })
}

fn object_columns<'t>(schema: crate::schema::CollectionSchema) -> Vec<liquid::Object> {
    schema
        .tables
        .iter()
        .flat_map(|(table_name, table)| {
            table.fields.iter().filter_map(move |(name, field)| {
                if matches!(
                    field,
                    FieldType::File { .. } | FieldType::Image { .. } | FieldType::Markdown { .. }
                ) {
                    Some(liquid::object!({
                        "name": name,
                        "table": table_name,
                    }))
                } else {
                    None
                }
            })
        })
        .collect()
}

static PARSER: LazyLock<liquid::Parser> =
    LazyLock::new(|| liquid::ParserBuilder::with_stdlib().build().unwrap());

pub struct SqlStatements {
    pub upsert: Vec<String>,
    pub cleanup: String,
    pub fetch_objects: String,
    pub ddl: String,
}

static SQL_DDL: LazyLock<liquid::Template> =
    LazyLock::new(|| PARSER.parse(include_str!("./ddl.sql.liquid")).unwrap());
static SQL_UPSERT: LazyLock<liquid::Template> =
    LazyLock::new(|| PARSER.parse(include_str!("./upsert.sql.liquid")).unwrap());
static SQL_CLEANUP: LazyLock<liquid::Template> =
    LazyLock::new(|| PARSER.parse(include_str!("./cleanup.sql.liquid")).unwrap());
static SQL_FETCH_OBJRECT: LazyLock<liquid::Template> = LazyLock::new(|| {
    PARSER
        .parse(include_str!("./fetch_object.sql.liquid"))
        .unwrap()
});

impl SqlStatements {
    pub fn new(collection: &schema::CollectionSchema) -> Self {
        let tables = collection
            .tables
            .iter()
            .map(|(name, schema)| table_liquid_context(name, schema))
            .collect::<Vec<_>>();
        let object_columns = object_columns(collection.clone());
        let upsert = tables
            .iter()
            .map(|ctx| SQL_UPSERT.render(&ctx).unwrap())
            .collect::<Vec<_>>();
        let ctx = liquid::object!({
            "tables": tables,
        });
        let cleanup = SQL_CLEANUP.render(&ctx).unwrap();
        let ddl = SQL_DDL.render(&ctx).unwrap();
        let ctx = liquid::object!({
            "object_columns": object_columns
        });
        let fetch_objects = SQL_FETCH_OBJRECT.render(&ctx).unwrap();
        Self {
            upsert,
            cleanup,
            ddl,
            fetch_objects,
        }
    }
}
