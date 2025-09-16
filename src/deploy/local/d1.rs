use crate::job;

pub struct LocalSqlite {
    conn: sqlx::SqlitePool,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("sqlite error: {0}")]
    Sqlite(sqlx::Error),
}

impl job::storage::sqlite::Client for LocalSqlite {
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
        query.fetch_all(&self.conn).await.map_err(Error::Sqlite)
    }
}
