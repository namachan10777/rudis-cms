use std::str::FromStr;

use blake3::Hasher;
use image::EncodableLayout;
use indexmap::IndexMap;
use sqlx::prelude::FromRow;

use crate::{
    config, deploy, job,
    process_data::{self, ObjectReference, StoragePointer},
    schema,
};

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
    #[derive(FromRow, Debug, PartialEq, Eq)]
    struct MailRow {
        id: String,
    }
    let mail_rows = sqlx::query_as::<_, MailRow>("SELECT * from mail ORDER BY id ASC")
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(
        &mail_rows,
        &[
            MailRow {
                id: "flavius".into()
            },
            MailRow { id: "gaius".into() },
            MailRow {
                id: "sextus".into()
            }
        ]
    );
    #[derive(FromRow, Debug, PartialEq, Eq)]
    struct AttachmentRow {
        mail_id: String,
        id: String,
        #[sqlx(json)]
        file: ObjectReference<()>,
    }
    let attchment_rows =
        sqlx::query_as::<_, AttachmentRow>("SELECT * from attachments ORDER BY id ASC")
            .fetch_all(&pool)
            .await
            .unwrap();
    assert_eq!(
        &attchment_rows,
        &[
            AttachmentRow {
                mail_id: "gaius".into(),
                id: "caesar".into(),
                file: ObjectReference {
                    hash: "fecad7959d53bb0331ad7cc7ec0efae3b2d795af21acb99c5a6704a3a54c3802"
                        .parse()
                        .unwrap(),
                    size: 7,
                    content_type: "text/plain".into(),
                    meta: (),
                    pointer: StoragePointer::R2 {
                        bucket: "assets".into(),
                        key: "mail/attachments/gaius/caesar".into()
                    }
                }
            },
            AttachmentRow {
                mail_id: "flavius".into(),
                id: "fimbria".into(),
                file: ObjectReference {
                    hash: "e835f9dd03cc946ebbdfdcb2678708ec1c6c36bb0c33539f9f29af265305eb87"
                        .parse()
                        .unwrap(),
                    size: 8,
                    content_type: "text/plain".into(),
                    meta: (),
                    pointer: StoragePointer::R2 {
                        bucket: "assets".into(),
                        key: "mail/attachments/flavius/fimbria".into(),
                    }
                }
            },
            AttachmentRow {
                mail_id: "flavius".into(),
                id: "galerius".into(),
                file: ObjectReference {
                    hash: "aaf44898ee61ea73558ac64013273221c110c77ac5ef4254bd1e70f8213e2944"
                        .parse()
                        .unwrap(),
                    size: 9,
                    content_type: "text/plain".into(),
                    meta: (),
                    pointer: StoragePointer::R2 {
                        bucket: "assets".into(),
                        key: "mail/attachments/flavius/galerius".into()
                    }
                }
            }
        ]
    );

    let (caesar_bytes, caesar_content_type) = r2
        .get("assets", "mail/attachments/gaius/caesar")
        .await
        .unwrap();
    assert_eq!(caesar_bytes.as_bytes(), "caesar\n".as_bytes());
    assert_eq!(&caesar_content_type, "text/plain");

    let (fimbria_bytes, fimbria_content_type) = r2
        .get("assets", "mail/attachments/flavius/fimbria")
        .await
        .unwrap();
    assert_eq!(fimbria_bytes.as_bytes(), "fimbria\n".as_bytes());
    assert_eq!(&fimbria_content_type, "text/plain");

    let (galerius_bytes, galerius_content_type) = r2
        .get("assets", "mail/attachments/flavius/galerius")
        .await
        .unwrap();
    assert_eq!(galerius_bytes.as_bytes(), "galerius\n".as_bytes());
    assert_eq!(&galerius_content_type, "text/plain");

    let mut all_tables = IndexMap::<String, Vec<_>>::new();
    let mut all_uploads = Vec::new();
    for path in [
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
    executor
        .batch(&schema, &all_tables, all_uploads, false)
        .await
        .unwrap();
    let mail_rows = sqlx::query_as::<_, MailRow>("SELECT * from mail ORDER BY id ASC")
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(
        &mail_rows,
        &[
            MailRow { id: "gaius".into() },
            MailRow {
                id: "sextus".into()
            }
        ]
    );
    let attchment_rows =
        sqlx::query_as::<_, AttachmentRow>("SELECT * from attachments ORDER BY id ASC")
            .fetch_all(&pool)
            .await
            .unwrap();
    assert_eq!(
        &attchment_rows,
        &[AttachmentRow {
            mail_id: "gaius".into(),
            id: "caesar".into(),
            file: ObjectReference {
                hash: "fecad7959d53bb0331ad7cc7ec0efae3b2d795af21acb99c5a6704a3a54c3802"
                    .parse()
                    .unwrap(),
                size: 7,
                content_type: "text/plain".into(),
                meta: (),
                pointer: StoragePointer::R2 {
                    bucket: "assets".into(),
                    key: "mail/attachments/gaius/caesar".into()
                }
            }
        },]
    );

    let (caesar_bytes, caesar_content_type) = r2
        .get("assets", "mail/attachments/gaius/caesar")
        .await
        .unwrap();
    assert_eq!(caesar_bytes.as_bytes(), "caesar\n".as_bytes());
    assert_eq!(&caesar_content_type, "text/plain");

    assert!(
        r2.get("assets", "mail/attachments/flavius/fimbria")
            .await
            .is_none()
    );

    assert!(
        r2.get("assets", "mail/attachments/flavius/galerius")
            .await
            .is_none()
    );
}
