use std::str::FromStr;

use blake3::Hasher;
use indexmap::IndexMap;

use crate::{config, deploy, job, process_data, schema};

#[tokio::test]
async fn test() {
    let mut hasher = Hasher::new();
    let config = tokio::fs::read_to_string("src/tests/scenario1/config.yaml")
        .await
        .unwrap();
    hasher.update(config.as_bytes());
    let config: config::Collection = serde_yaml::from_str(&config).unwrap();
    let schema = schema::TableSchema::compile(&config).unwrap();
    let mut all_tables = IndexMap::<String, Vec<_>>::new();
    let mut all_uploads = Vec::new();
    for path in [
        "src/tests/scenario1/mail/flavius.yaml",
        "src/tests/scenario1/mail/gaius.yaml",
        "src/tests/scenario1/mail/sextus.yaml",
    ] {
        let (table, uploads) = process_data::table::push_rows_from_document(
            "mail",
            hasher.clone(),
            &schema,
            &config.syntax,
            path,
        )
        .await
        .unwrap();
        for (table, rows) in table {
            all_tables.entry(table).or_default().extend(rows);
        }
        all_uploads.extend(uploads);
    }

    let options = sqlx::sqlite::SqliteConnectOptions::from_str("sqlite::memory:").unwrap();
    let pool = sqlx::sqlite::SqlitePool::connect_with(options)
        .await
        .unwrap();

    let d1 = deploy::local::d1::LocalSqlite { pool: pool.clone() };
    let kv = deploy::local::kv::Client::default();
    let r2 = deploy::local::r2::Client::default();
    let asset = deploy::local::asset::Client::default();
    let executor = job::JobExecutor {
        d1: d1,
        kv: &kv,
        r2: &r2,
        asset: &asset,
    };
    executor
        .batch(&schema, &all_tables, all_uploads, false)
        .await
        .unwrap();
}
