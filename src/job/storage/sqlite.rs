use serde::de::DeserializeOwned;

pub trait Client {
    type Error: super::BackendError;

    fn query<R>(
        &self,
        statement: &str,
        params: &[&str],
    ) -> impl Future<Output = Result<Vec<R>, Self::Error>> + Send
    where
        R: DeserializeOwned + for<'a> sqlx::FromRow<'a, sqlx::sqlite::SqliteRow> + Send + Unpin;
}
