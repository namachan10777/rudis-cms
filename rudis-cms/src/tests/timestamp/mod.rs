use sqlx::prelude::FromRow;

use crate::tests::local_uploader;

#[derive(FromRow, Debug)]
struct TimestampRow {
    created: String,
    modified: String,
}

#[tokio::test]
async fn descendant_change_touches_every_ancestor() {
    const OLD_TIME: &str = "2000-01-01T00:00:00.000Z";

    let (schema, hasher, syntax) = super::load_schema("src/tests/timestamp/config.yaml")
        .await
        .unwrap();
    let uploader = local_uploader().await;
    let (tables, uploads) = super::load_files(
        &hasher,
        &schema,
        &syntax,
        &["src/tests/timestamp/posts/before.yaml"],
    )
    .await
    .unwrap();
    uploader
        .executor
        .batch(&schema, &tables, uploads, false)
        .await
        .unwrap();

    for table in ["posts", "comments", "replies"] {
        sqlx::query(&format!("UPDATE {table} SET created = ?, modified = ?"))
            .bind(OLD_TIME)
            .bind(OLD_TIME)
            .execute(uploader.db.pool())
            .await
            .unwrap();
    }

    let (tables, uploads) = super::load_files(
        &hasher,
        &schema,
        &syntax,
        &["src/tests/timestamp/posts/after.yaml"],
    )
    .await
    .unwrap();
    uploader
        .executor
        .batch(&schema, &tables, uploads, false)
        .await
        .unwrap();

    for table in ["posts", "comments", "replies"] {
        let timestamp = sqlx::query_as::<_, TimestampRow>(&format!(
            "SELECT created, modified FROM {table} LIMIT 1"
        ))
        .fetch_one(uploader.db.pool())
        .await
        .unwrap();
        assert_eq!(timestamp.created, OLD_TIME, "created changed for {table}");
        assert_ne!(
            timestamp.modified, OLD_TIME,
            "modified was not updated for {table}"
        );
    }
}
