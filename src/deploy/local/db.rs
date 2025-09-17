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
        let pool = sqlx::sqlite::SqlitePool::connect_with(options).await?;
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

    async fn query<
        'q,
        R: serde::de::DeserializeOwned
            + for<'a> sqlx::FromRow<'a, sqlx::sqlite::SqliteRow>
            + Send
            + Unpin,
        P: job::storage::sqlite::Param + sqlx::Encode<'q, sqlx::Sqlite> + sqlx::Type<sqlx::Sqlite>,
    >(
        &self,
        statement: &'q str,
        params: &'q [&'q P],
    ) -> Result<Vec<R>, Self::Error> {
        let query = params.iter().fold(
            sqlx::query_as::<sqlx::Sqlite, R>(statement),
            |query, param| query.bind(param),
        );
        query.fetch_all(&self.pool).await.map_err(Error::Sqlite)
    }
}
