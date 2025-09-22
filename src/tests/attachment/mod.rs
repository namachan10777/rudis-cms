use serde::Deserialize;
use sqlx::prelude::FromRow;

use crate::tests::local_uploader;

#[derive(FromRow, Debug, PartialEq, Eq)]
struct PostRow {
    id: String,
}

#[derive(Deserialize, PartialEq, Eq, Debug)]
#[serde(rename_all = "snake_case", tag = "type")]
enum StoragePointer {
    R2 { bucket: String, key: String },
}

#[derive(Deserialize, Debug, PartialEq, Eq)]
struct FileColumn {
    size: usize,
    content_type: String,
    meta: (),
    pointer: StoragePointer,
}

#[derive(FromRow, Debug, PartialEq, Eq)]
struct AttachmentRow {
    post_id: String,
    id: String,
    #[sqlx(json)]
    file: FileColumn,
}

#[derive(FromRow, Debug, PartialEq, Eq)]
struct R2Row {
    bucket: String,
    key: String,
    content_type: String,
}

#[tokio::test]
async fn upsert() {
    let (schema, hasher, syntax) = super::load_schema("src/tests/attachment/config.yaml")
        .await
        .unwrap();
    let (tables, uploads) = super::load_files(
        &hasher,
        &schema,
        &syntax,
        &[
            "src/tests/attachment/posts/post1.yaml",
            "src/tests/attachment/posts/post2.yaml",
        ],
    )
    .await
    .unwrap();
    let uploader = local_uploader().await;
    uploader
        .executor
        .batch(&schema, &tables, uploads, false)
        .await
        .unwrap();

    assert_eq!(
        &sqlx::query_as::<_, PostRow>("SELECT * FROM posts")
            .fetch_all(uploader.db.pool())
            .await
            .unwrap(),
        &[
            PostRow {
                id: "post1".to_string()
            },
            PostRow {
                id: "post2".to_string()
            },
        ]
    );
    assert_eq!(
        &sqlx::query_as::<_, AttachmentRow>("SELECT * FROM attachments ORDER BY id")
            .fetch_all(uploader.db.pool())
            .await
            .unwrap(),
        &[
            AttachmentRow {
                id: "data1-1".into(),
                post_id: "post1".into(),
                file: FileColumn {
                    size: 4,
                    content_type: "text/plain".into(),
                    meta: (),
                    pointer: StoragePointer::R2 {
                        bucket: "assets".into(),
                        key: "posts/attachments/post1/data1-1".into()
                    }
                }
            },
            AttachmentRow {
                id: "data1-2".into(),
                post_id: "post1".into(),
                file: FileColumn {
                    size: 4,
                    content_type: "text/plain".into(),
                    meta: (),
                    pointer: StoragePointer::R2 {
                        bucket: "assets".into(),
                        key: "posts/attachments/post1/data1-2".into()
                    }
                }
            },
            AttachmentRow {
                id: "data2-1".to_string(),
                post_id: "post2".into(),
                file: FileColumn {
                    size: 4,
                    content_type: "text/plain".into(),
                    meta: (),
                    pointer: StoragePointer::R2 {
                        bucket: "assets".into(),
                        key: "posts/attachments/post2/data2-1".into()
                    }
                }
            },
        ]
    );
    assert_eq!(
        &sqlx::query_as::<_, R2Row>("SELECT * FROM r2 ORDER BY key")
            .fetch_all(uploader.storage.pool())
            .await
            .unwrap(),
        &[
            R2Row {
                bucket: "assets".into(),
                key: "posts/attachments/post1/data1-1".into(),
                content_type: "text/plain".into(),
            },
            R2Row {
                bucket: "assets".into(),
                key: "posts/attachments/post1/data1-2".into(),
                content_type: "text/plain".into(),
            },
            R2Row {
                bucket: "assets".into(),
                key: "posts/attachments/post2/data2-1".into(),
                content_type: "text/plain".into(),
            },
        ]
    )
}

#[tokio::test]
async fn cleanup() {
    let (schema, hasher, syntax) = super::load_schema("src/tests/attachment/config.yaml")
        .await
        .unwrap();
    let (tables, uploads) = super::load_files(
        &hasher,
        &schema,
        &syntax,
        &[
            "src/tests/attachment/posts/post1.yaml",
            "src/tests/attachment/posts/post2.yaml",
        ],
    )
    .await
    .unwrap();
    let uploader = local_uploader().await;
    uploader
        .executor
        .batch(&schema, &tables, uploads, false)
        .await
        .unwrap();

    let (tables, uploads) = super::load_files(
        &hasher,
        &schema,
        &syntax,
        &["src/tests/attachment/posts/post1.yaml"],
    )
    .await
    .unwrap();
    uploader
        .executor
        .batch(&schema, &tables, uploads, false)
        .await
        .unwrap();

    assert_eq!(
        &sqlx::query_as::<_, PostRow>("SELECT * FROM posts")
            .fetch_all(uploader.db.pool())
            .await
            .unwrap(),
        &[PostRow {
            id: "post1".to_string()
        },]
    );
    assert_eq!(
        &sqlx::query_as::<_, AttachmentRow>("SELECT * FROM attachments ORDER BY id")
            .fetch_all(uploader.db.pool())
            .await
            .unwrap(),
        &[
            AttachmentRow {
                id: "data1-1".into(),
                post_id: "post1".into(),
                file: FileColumn {
                    size: 4,
                    content_type: "text/plain".into(),
                    meta: (),
                    pointer: StoragePointer::R2 {
                        bucket: "assets".into(),
                        key: "posts/attachments/post1/data1-1".into()
                    }
                }
            },
            AttachmentRow {
                id: "data1-2".into(),
                post_id: "post1".into(),
                file: FileColumn {
                    size: 4,
                    content_type: "text/plain".into(),
                    meta: (),
                    pointer: StoragePointer::R2 {
                        bucket: "assets".into(),
                        key: "posts/attachments/post1/data1-2".into()
                    }
                }
            },
        ]
    );
    assert_eq!(
        &sqlx::query_as::<_, R2Row>("SELECT * FROM r2 ORDER BY key")
            .fetch_all(uploader.storage.pool())
            .await
            .unwrap(),
        &[
            R2Row {
                bucket: "assets".into(),
                key: "posts/attachments/post1/data1-1".into(),
                content_type: "text/plain".into(),
            },
            R2Row {
                bucket: "assets".into(),
                key: "posts/attachments/post1/data1-2".into(),
                content_type: "text/plain".into(),
            },
        ]
    )
}
