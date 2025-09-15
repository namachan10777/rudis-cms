use aws_config::BehaviorVersion;

use crate::job;

pub struct Client {
    client: aws_sdk_s3::Client,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Failed to delete object: {0}")]
    Delete(String),
    #[error("Failed to put object: {0}")]
    Put(String),
}

impl Client {
    pub async fn new(account_id: &str, access_key_id: &str, secret_access_key: &str) -> Self {
        let config = aws_config::defaults(BehaviorVersion::latest())
            .endpoint_url(format!("https://{account_id}.r2.cloudflarestorage.com"))
            .credentials_provider(aws_sdk_s3::config::Credentials::new(
                access_key_id,
                secret_access_key,
                None, // session token is not used with R2
                None,
                "R2",
            ))
            .region("auto")
            .load()
            .await;
        Self {
            client: aws_sdk_s3::Client::new(&config),
        }
    }
}

impl job::storage::r2::Client for Client {
    type Error = Error;

    async fn delete(&self, bucket: String, key: String) -> Result<(), Self::Error> {
        self.client
            .delete_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .map_err(|error| Error::Delete(error.to_string()))?;
        Ok(())
    }

    async fn put(
        &self,
        bucket: String,
        key: String,
        content_type: String,
        body: aws_sdk_s3::primitives::ByteStream,
    ) -> Result<(), Self::Error> {
        self.client
            .put_object()
            .bucket(bucket)
            .key(key)
            .content_type(content_type)
            .body(body)
            .send()
            .await
            .map_err(|error| Error::Put(error.to_string()))?;
        Ok(())
    }
}
