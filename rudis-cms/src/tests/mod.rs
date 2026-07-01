use std::path::Path;

use blake3::Hasher;

use indexmap::IndexMap;

use crate::{
    config::{self, DocumentSyntax},
    deploy::{
        self,
        local::{
            db::Client,
            storage::{AssetClient, KvClient, R2Client},
        },
    },
    job::{self, JobExecutor},
    process_data::{self, ColumnValue, table::Upload},
    schema::{self, CollectionSchema},
};

mod attachment;
mod subtable;

async fn load_schema(
    path: &str,
) -> anyhow::Result<(CollectionSchema, blake3::Hasher, DocumentSyntax)> {
    let mut hasher = Hasher::new();
    let config = tokio::fs::read_to_string(path).await.unwrap();
    hasher.update(config.as_bytes());
    let config: config::Collection = serde_yaml::from_str(&config).unwrap();
    let schema = schema::TableSchema::compile(&config).unwrap();
    Ok((schema, hasher, config.syntax))
}

async fn load_files<P: AsRef<Path>>(
    hasher: &blake3::Hasher,
    schema: &schema::CollectionSchema,
    syntax: &DocumentSyntax,
    paths: &[P],
) -> anyhow::Result<(
    IndexMap<String, Vec<IndexMap<String, ColumnValue>>>,
    Vec<Upload>,
)> {
    let mut all_tables = IndexMap::<String, Vec<_>>::new();
    let mut all_uploads = Vec::new();
    for path in paths {
        let (table, uploads) = process_data::table::push_rows_from_document(
            schema.tables.keys().next().unwrap(),
            hasher.clone(),
            schema,
            syntax,
            path,
        )
        .await
        .unwrap();
        for (table, rows) in table {
            all_tables.entry(table).or_default().extend(rows);
        }
        all_uploads.extend(uploads);
    }
    Ok((all_tables, all_uploads))
}

struct Uploader {
    executor: JobExecutor<Client, KvClient, R2Client, AssetClient>,
    db: deploy::local::db::LocalDatabase,
    #[allow(unused)]
    storage: deploy::local::storage::LocalStorage,
}

async fn local_uploader() -> Uploader {
    let db = deploy::local::db::LocalDatabase::open("sqlite::memory:")
        .await
        .unwrap();
    let storage = deploy::local::storage::LocalStorage::open("sqlite::memory:")
        .await
        .unwrap();
    let executor = job::JobExecutor {
        d1: db.client(),
        kv: storage.kv_client(),
        r2: storage.r2_client(),
        asset: storage.asset_client(),
    };
    Uploader {
        executor,
        storage,
        db,
    }
}
