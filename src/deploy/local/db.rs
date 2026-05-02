use std::str::FromStr;

use crate::job;
pub struct LocalDatabase {
    pool: sqlx::SqlitePool,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("sqlite error: {0}")]
    Sqlite(sqlx::Error),
}

pub struct Client {
    pool: sqlx::SqlitePool,
}

impl LocalDatabase {
    pub async fn open(url: &str) -> Result<Self, sqlx::Error> {
        let options = sqlx::sqlite::SqliteConnectOptions::from_str(url)?;
        let pool = sqlx::pool::PoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await?;
        Ok(Self { pool })
    }

    pub fn pool(&self) -> &sqlx::SqlitePool {
        &self.pool
    }

    pub fn client(&self) -> Client {
        Client {
            pool: self.pool.clone(),
        }
    }
}

impl job::storage::sqlite::Client for Client {
    type Error = Error;

    async fn query<R>(&self, statement: &str, params: &[&str]) -> Result<Vec<R>, Self::Error>
    where
        R: serde::de::DeserializeOwned
            + for<'a> sqlx::FromRow<'a, sqlx::sqlite::SqliteRow>
            + Send
            + Unpin,
    {
        let query = params.iter().fold(
            sqlx::query_as::<sqlx::Sqlite, R>(statement),
            |query, param| query.bind(*param),
        );
        query.fetch_all(&self.pool).await.map_err(Error::Sqlite)
    }
}
