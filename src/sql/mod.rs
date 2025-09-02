use std::{path::PathBuf, sync::LazyLock};

use indexmap::IndexMap;
use tracing::{debug, trace};

use crate::{
    field::StoragePointer,
    field::upload::{AssetUpload, KvUpload, R2Upload, Uploads},
    record,
    schema::{self, TableSchemas},
};

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

const PARSER: LazyLock<liquid::Parser> =
    LazyLock::new(|| liquid::ParserBuilder::with_stdlib().build().unwrap());

pub const DDL: LazyLock<liquid::Template> = LazyLock::new(|| {
    PARSER
        .parse(include_str!("./templates/ddl.sql.liquid"))
        .unwrap()
});

fn create_ctx_inherit_id_columns(schema: &schema::Schema) -> impl Iterator<Item = liquid::Object> {
    schema.inherit_ids.iter().map(|id| {
        liquid::object!({
            "name": id,
            "type": "TEXT",
            "not_null": true,
            "is_primary_key": true,
        })
    })
}

fn create_ctx_columns(schema: &schema::Schema) -> impl Iterator<Item = liquid::Object> {
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

fn create_ctx_id_column(schema: &schema::Schema) -> liquid::Object {
    liquid::object!({
        "name": schema.id_name,
        "type": "TEXT",
        "not_null": true,
        "is_primary_key": true,
    })
}

fn create_ctx_internal_column_indexes(
    schema: &schema::Schema,
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

fn create_ctx_object_hash_indexes(schema: &schema::Schema) -> impl Iterator<Item = liquid::Object> {
    schema.fields.iter().filter_map(|(name, field)| {
        if is_object_field(field) {
            Some(liquid::object!({
                "name": name,
                "index": format!("{name}->>'hash'")
            }))
        } else {
            None
        }
    })
}

fn create_ctx_parent(schema: &schema::Schema) -> Option<liquid::Object> {
    schema.parent.as_ref().map(|parent| {
        liquid::object!({
            "table": parent.name,
            "parent_ids": parent.id_names,
            "local_ids": schema.inherit_ids,
        })
    })
}

fn create_ctx_primary_key(schema: &schema::Schema) -> Vec<String> {
    let mut primary_key = schema.inherit_ids.clone();
    primary_key.push(schema.id_name.clone());
    primary_key
}

fn create_ctx_object_columns(schema: &schema::TableSchemas) -> Vec<liquid::Object> {
    schema
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

fn create_ctx_tables(schema: &schema::TableSchemas) -> IndexMap<String, liquid::Object> {
    let mut tables = schema
            .iter()
            .map(|(table, schema)| {
                let ctx = liquid::object!({
                    "name": table,
                    "columns": create_ctx_inherit_id_columns(schema).chain(std::iter::once(create_ctx_id_column(schema))).chain(create_ctx_columns(schema)).collect::<Vec<_>>(),
                    "data_columns": create_ctx_columns(schema).collect::<Vec<_>>(),
                    "indexes": create_ctx_internal_column_indexes(schema).chain(create_ctx_object_hash_indexes(schema)).collect::<Vec<_>>(),
                    "parent": create_ctx_parent(schema),
                    "primary_key": create_ctx_primary_key(schema),
                });
                (table.clone(), ctx)
            })
            .collect::<IndexMap<String, _>>();
    tables.reverse();
    tables
}

pub fn create_ctx(schema: &schema::TableSchemas) -> liquid::Object {
    liquid::object!({
        "tables": create_ctx_tables(schema).into_values().collect::<Vec<_>>(),
        "object_columns": create_ctx_object_columns(schema),
    })
}

const FETCH_ALL: LazyLock<liquid::Template> = LazyLock::new(|| {
    PARSER
        .parse(include_str!("./templates/fetch_all_hash.liquid"))
        .unwrap()
});

const UPSERT: LazyLock<liquid::Template> = LazyLock::new(|| {
    PARSER
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

async fn fetch_all_hashes(
    conn: &rusqlite::Connection,
    ctx: &liquid::Object,
) -> Result<IndexMap<blake3::Hash, String>, Error> {
    struct Row {
        storage: String,
        hash: String,
    }
    let mut stmt = conn.prepare(&FETCH_ALL.render(ctx).unwrap())?;
    let rows = stmt
        .query_map([], |row| {
            Ok(Row {
                hash: row.get(0)?,
                storage: row.get(1)?,
            })
        })?
        .collect::<Result<Vec<_>, _>>()?;
    let present_hashes = rows
        .into_iter()
        .map(|row| {
            let hash = row.hash.parse()?;
            Ok((hash, row.storage))
        })
        .collect::<Result<IndexMap<_, _>, _>>()
        .map_err(Error::ParseHash)?;
    Ok(present_hashes)
}

async fn upsert(
    conn: &rusqlite::Connection,
    schema: &TableSchemas,
    tables: &record::Tables,
) -> Result<(), Error> {
    let upsert_ctx = liquid::object!({
        "tables": create_ctx_tables(schema).into_iter().filter_map(|(table, schema)| if tables.contains_key(&table) {
            Some(schema)
        } else {
            None
        }).collect::<Vec<_>>()
    });
    let statements = UPSERT.render(&upsert_ctx).unwrap();
    for statement in statements.split(";") {
        if statement.trim().is_empty() {
            continue;
        }
        conn.execute(statement, [serde_json::to_string(&tables).unwrap()])?;
    }
    Ok(())
}

pub struct R2Delete {
    pub bucket: String,
    pub key: String,
}

pub struct KvDelete {
    pub namespace: String,
    pub key: String,
}

pub struct AssetDelete {
    pub path: PathBuf,
}

pub struct Deletions {
    pub r2: Vec<R2Delete>,
    pub kv: Vec<KvDelete>,
    pub asset: Vec<AssetDelete>,
}

pub trait StorageBackend {
    fn upload(
        &self,
        r2: impl Iterator<Item = R2Upload>,
        kv: impl Iterator<Item = KvUpload>,
        asset: impl Iterator<Item = AssetUpload>,
    ) -> impl Future<Output = Result<(), Error>>;
    fn delete(
        &self,
        r2: impl Iterator<Item = R2Delete>,
        kv: impl Iterator<Item = KvDelete>,
        asset: impl Iterator<Item = AssetDelete>,
    ) -> impl Future<Output = Result<(), Error>>;
}

pub async fn batch(
    conn: &rusqlite::Connection,
    schema: &TableSchemas,
    tables: record::Tables,
    mut upload_candidates: Uploads,
    backend: &impl StorageBackend,
) -> Result<(), Error> {
    let base_ctx = create_ctx(schema);
    let present_hashes = fetch_all_hashes(conn, &base_ctx).await?;
    upload_candidates
        .r2
        .retain(|k, _| !present_hashes.contains_key(k));
    upload_candidates
        .kv
        .retain(|k, _| !present_hashes.contains_key(k));
    upload_candidates
        .asset
        .retain(|k, _| !present_hashes.contains_key(k));
    debug!("upload filtered");
    let Uploads { r2, kv, asset } = upload_candidates;
    backend
        .upload(r2.into_values(), kv.into_values(), asset.into_values())
        .await?;
    debug!("upload finished");
    upsert(conn, schema, &tables).await?;
    debug!("upsert finished");
    let after_all_hashes = fetch_all_hashes(conn, &base_ctx).await?;
    let mut r2_deletes = Vec::new();
    let mut kv_deletes = Vec::new();
    let mut asset_deletes = Vec::new();
    for (hash, storage) in present_hashes {
        if after_all_hashes.contains_key(&hash) {
            continue;
        }
        let Ok(storage) = serde_json::from_str::<StoragePointer>(&storage) else {
            continue;
        };
        match storage {
            StoragePointer::R2 { bucket, key } => {
                r2_deletes.push(R2Delete { bucket, key });
            }
            StoragePointer::Kv { namespace, key } => {
                kv_deletes.push(KvDelete { namespace, key });
            }
            StoragePointer::Asset { path } => {
                asset_deletes.push(AssetDelete { path });
            }
        }
    }
    backend
        .delete(
            r2_deletes.into_iter(),
            kv_deletes.into_iter(),
            asset_deletes.into_iter(),
        )
        .await?;
    debug!("clean up finished");
    Ok(())
}
