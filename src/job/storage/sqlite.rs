use serde::{Serialize, de::DeserializeOwned};

pub trait Param: Serialize {}

impl Param for &str {}

pub trait Client {
    type Error;

    fn query<
        'q,
        R: DeserializeOwned + for<'a> sqlx::FromRow<'a, sqlx::sqlite::SqliteRow> + Send + Unpin,
        P: Param + sqlx::Encode<'q, sqlx::Sqlite> + sqlx::Type<sqlx::Sqlite>,
    >(
        &self,
        statement: &'q str,
        params: &'q [&'q P],
    ) -> impl Future<Output = Result<Vec<R>, Self::Error>>;
}
