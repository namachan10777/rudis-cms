use std::collections::HashMap;

use aws_sdk_s3::primitives::ByteStreamError;

use crate::job::{self, storage::kv::Pair};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("failed to aggregate body: {0}")]
    AggregateBody(ByteStreamError),
}

pub struct Client {
    map: tokio::sync::Mutex<HashMap<String, tokio::sync::Mutex<HashMap<String, Pair>>>>,
}

impl job::storage::kv::Client for Client {
    type Error = Error;
    async fn delete_multiple(&self, namespace: &str, keys: &[String]) -> Result<(), Self::Error> {
        let mut namespaces = self.map.lock().await;
        let mut namespace = namespaces.entry(namespace.into()).or_default().lock().await;
        for key in keys {
            namespace.remove(key);
        }
        Ok(())
    }

    async fn write_multiple(
        &self,
        namespace: &str,
        pairs: &[job::storage::kv::Pair],
    ) -> Result<(), Self::Error> {
        let mut namespaces = self.map.lock().await;
        let mut namespace = namespaces.entry(namespace.into()).or_default().lock().await;
        for pair in pairs {
            namespace.insert(pair.key.clone(), pair.clone());
        }
        Ok(())
    }
}
