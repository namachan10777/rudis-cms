use std::str::FromStr as _;

use image::EncodableLayout;
use tracing::error;

use crate::job;

pub struct LocalStorage {
    pool: sqlx::SqlitePool,
}

pub struct R2Client {
    pool: sqlx::SqlitePool,
}

pub struct KvClient {
    pool: sqlx::SqlitePool,
}

pub struct AssetClient {
    pool: sqlx::SqlitePool,
}

impl LocalStorage {
    pub async fn open(url: &str) -> Result<Self, sqlx::Error> {
        let options = sqlx::sqlite::SqliteConnectOptions::from_str(url)
            .inspect_err(|error| error!(%error, %url, "Failed to open local storage db"))?;
        let pool = sqlx::pool::PoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await
            .inspect_err(|error| error!(%error, %url, "Failed to open local storage db"))?;
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS r2(
                bucket TEXT NOT NULL,
                key TEXT NOT NULL,
                content_type TEXT NOT NULL,
                body BLOB NOT NULL,
                PRIMARY KEY(bucket, key)
            );

            CREATE TABLE IF NOT EXISTS kv(
                namespace TEXT NOT NULL,
                key TEXT NOT NULL,
                value TEXT NOT NULL,
                base64 INTEGER NOT NULL,
                expiration INTEGER,
                expiration_ttl INTEGER,
                PRIMARY KEY(namespace, key)
            );

            CREATE TABLE IF NOT EXISTS asset(
                path TEXT NOT NULL PRIMARY KEY,
                content BLOB NOT NULL
            );
        "#,
        )
        .execute(&pool)
        .await
        .inspect_err(|error| error!(%error, %url, "Failed to execute DDL to storage db"))?;
        Ok(Self { pool })
    }

    pub fn pool(&self) -> &sqlx::SqlitePool {
        &self.pool
    }

    pub fn r2_client(&self) -> R2Client {
        R2Client {
            pool: self.pool.clone(),
        }
    }

    pub fn kv_client(&self) -> KvClient {
        KvClient {
            pool: self.pool.clone(),
        }
    }

    pub fn asset_client(&self) -> AssetClient {
        AssetClient {
            pool: self.pool.clone(),
        }
    }
}

impl job::storage::r2::Client for R2Client {
    type Error = sqlx::Error;
    async fn delete(&self, bucket: String, key: String) -> Result<(), Self::Error> {
        sqlx::query("DELETE FROM r2 WHERE bucket = ? AND key = ?")
            .bind(bucket)
            .bind(key)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn put(
        &self,
        bucket: String,
        key: String,
        content_type: String,
        body: aws_sdk_s3::primitives::ByteStream,
    ) -> Result<(), Self::Error> {
        let body = body.collect().await.unwrap().into_bytes();
        sqlx::query(
            r#"
            INSERT INTO r2(bucket, key, content_type, body)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(bucket, key)
            DO UPDATE SET
                content_type = EXCLUDED.content_type,
                body = EXCLUDED.body
        "#,
        )
        .bind(bucket)
        .bind(key)
        .bind(content_type)
        .bind(body.as_bytes())
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}

impl job::storage::asset::Client for AssetClient {
    type Error = sqlx::Error;

    async fn delete(&self, path: &std::path::Path) -> Result<(), Self::Error> {
        sqlx::query("DELETE FROM asset WHERE path = ?")
            .bind(path.display().to_string())
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn put(&self, path: &std::path::Path, content: &[u8]) -> Result<(), Self::Error> {
        sqlx::query(
            r#"
            INSERT INTO asset(path, content)
            VALUES (?, ?)
            ON CONFLICT(path)
            DO UPDATE SET
                content = EXCLUDED.content
        "#,
        )
        .bind(path.display().to_string())
        .bind(content)
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}

impl job::storage::kv::Client for KvClient {
    type Error = sqlx::Error;
    async fn delete_multiple(&self, namespace: &str, keys: &[String]) -> Result<(), Self::Error> {
        sqlx::query(
            r#"
            DELETE FROM kv
            WHERE
                namespace = ?
                AND key NOT IN (
                    SELECT value FROM json_each(?)
                )
        "#,
        )
        .bind(namespace)
        .bind(serde_json::to_string(keys).unwrap())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn write_multiple(
        &self,
        namespace: &str,
        pairs: &[job::storage::kv::Pair],
    ) -> Result<(), Self::Error> {
        sqlx::query(
            r#"
            INSERT INTO kv(namespace, key, value, base64, expiration, expiration_ttl)
            SELECT ?, value->>'key', value->>'value', value->>'base64', value->>'expiration', value->>'expiration_ttl'
            FROM json_each(?)
            WHERE 1
            ON CONFLICT(namespace, key)
            DO UPDATE SET
                value = EXCLUDED.value,
                base64 = EXCLUDED.base64,
                expiration = EXCLUDED.expiration,
                expiration_ttl = EXCLUDED.expiration_ttl
        "#,
        )
        .bind(namespace)
        .bind(serde_json::to_string(pairs).unwrap())
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}
