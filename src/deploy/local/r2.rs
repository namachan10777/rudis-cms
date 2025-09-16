use std::collections::HashMap;

use aws_sdk_s3::primitives::ByteStreamError;

use crate::job;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("failed to aggregate body: {0}")]
    AggregateBody(ByteStreamError),
}

pub struct Client {
    map: tokio::sync::Mutex<
        HashMap<String, tokio::sync::Mutex<HashMap<String, (bytes::Bytes, String)>>>,
    >,
}

impl job::storage::r2::Client for Client {
    type Error = Error;
    async fn delete(&self, bucket: String, key: String) -> Result<(), Self::Error> {
        if let Some(bucket) = self.map.lock().await.get(&bucket) {
            bucket.lock().await.remove(&key);
        }
        Ok(())
    }

    async fn put(
        &self,
        bucket: String,
        key: String,
        content_type: String,
        body: aws_sdk_s3::primitives::ByteStream,
    ) -> Result<(), Self::Error> {
        let body = body
            .collect()
            .await
            .map_err(Error::AggregateBody)?
            .into_bytes();
        self.map
            .lock()
            .await
            .entry(bucket)
            .or_default()
            .lock()
            .await
            .insert(key, (body, content_type));
        Ok(())
    }
}
