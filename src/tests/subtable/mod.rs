use sqlx::prelude::FromRow;

use crate::tests::local_uploader;

#[derive(FromRow, PartialEq, Eq, Debug)]
struct TagRow {
    post_id: String,
    tag: String,
}

#[tokio::test]
async fn update_subtable() {
    let (schema, hasher, syntax) = super::load_schema("src/tests/subtable/config.yaml")
        .await
        .unwrap();
    let (tables, uploads) = super::load_files(
        &hasher,
        &schema,
        &syntax,
        &["src/tests/subtable/post/before.yaml"],
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
        &sqlx::query_as::<_, TagRow>("SELECT * FROM tags ORDER BY tag")
            .fetch_all(uploader.db.pool())
            .await
            .unwrap(),
        &[
            TagRow {
                post_id: "post1".to_string(),
                tag: "tag1".to_string(),
            },
            TagRow {
                post_id: "post1".to_string(),
                tag: "tag2".to_string(),
            },
        ]
    );
    let (tables, uploads) = super::load_files(
        &hasher,
        &schema,
        &syntax,
        &["src/tests/subtable/post/after.yaml"],
    )
    .await
    .unwrap();
    uploader
        .executor
        .batch(&schema, &tables, uploads, false)
        .await
        .unwrap();

    assert_eq!(
        &sqlx::query_as::<_, TagRow>("SELECT * FROM tags ORDER BY tag")
            .fetch_all(uploader.db.pool())
            .await
            .unwrap(),
        &[
            TagRow {
                post_id: "post1".to_string(),
                tag: "tag1".to_string(),
            },
            TagRow {
                post_id: "post1".to_string(),
                tag: "tag3".to_string(),
            },
        ]
    );
}
