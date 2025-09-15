use serde::{Serialize, de::DeserializeOwned};

pub trait Param: Serialize {}

impl Param for &str {}

pub trait Client {
    type Error;

    fn query<R: DeserializeOwned, P: Param>(
        &self,
        statement: &str,
        params: &[&P],
    ) -> impl Future<Output = Result<Vec<R>, Self::Error>>;
}
