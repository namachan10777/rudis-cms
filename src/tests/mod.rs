use blake3::Hasher;

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
    executor
        .batch(&schema, &all_tables, all_uploads, false)
        .await
        .unwrap();
    #[derive(FromRow, Debug, PartialEq, Eq)]
    struct MailRow {
        id: String,
    }
    let mail_rows = sqlx::query_as::<_, MailRow>("SELECT * from mail ORDER BY id ASC")
        .fetch_all(db.pool())
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
            .fetch_all(db.pool())
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

    #[derive(FromRow, Debug, PartialEq, Eq)]
    pub struct R2Row {
        body: Vec<u8>,
        content_type: String,
    }

    assert_eq!(
        sqlx::query_as::<_, R2Row>("SELECT * FROM r2 WHERE bucket = ? AND key = ?")
            .bind("assets")
            .bind("mail/attachments/gaius/caesar")
            .fetch_one(storage.pool())
            .await
            .unwrap(),
        R2Row {
            body: "caesar\n".as_bytes().to_vec(),
            content_type: "text/plain".into()
        }
    );

    assert_eq!(
        sqlx::query_as::<_, R2Row>("SELECT * FROM r2 WHERE bucket = ? AND key = ?")
            .bind("assets")
            .bind("mail/attachments/flavius/fimbria")
            .fetch_one(storage.pool())
            .await
            .unwrap(),
        R2Row {
            body: "fimbria\n".as_bytes().to_vec(),
            content_type: "text/plain".into()
        }
    );

    assert_eq!(
        sqlx::query_as::<_, R2Row>("SELECT * FROM r2 WHERE bucket = ? AND key = ?")
            .bind("assets")
            .bind("mail/attachments/flavius/galerius")
            .fetch_one(storage.pool())
            .await
            .unwrap(),
        R2Row {
            body: "galerius\n".as_bytes().to_vec(),
            content_type: "text/plain".into()
        }
    );

    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM r2")
            .fetch_one(storage.pool())
            .await
            .unwrap(),
        3
    );

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
        .fetch_all(db.pool())
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
            .fetch_all(db.pool())
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

    assert_eq!(
        sqlx::query_as::<_, R2Row>("SELECT * FROM r2 WHERE bucket = ? AND key = ?")
            .bind("assets")
            .bind("mail/attachments/gaius/caesar")
            .fetch_one(storage.pool())
            .await
            .unwrap(),
        R2Row {
            body: "caesar\n".as_bytes().to_vec(),
            content_type: "text/plain".into()
        }
    );

    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM r2")
            .fetch_one(storage.pool())
            .await
            .unwrap(),
        1
    );
}
