use std::{collections::HashMap, path::PathBuf};

use crate::job;

#[derive(Default)]
pub struct Client {
    map: tokio::sync::Mutex<HashMap<PathBuf, Box<[u8]>>>,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {}

impl job::storage::asset::Client for &Client {
    type Error = Error;

    async fn delete(&self, path: &std::path::Path) -> Result<(), Self::Error> {
        self.map.lock().await.remove(path);
        Ok(())
    }

    async fn put(&self, path: &std::path::Path, content: &[u8]) -> Result<(), Self::Error> {
        self.map
            .lock()
            .await
            .insert(path.to_owned(), content.to_vec().into_boxed_slice());
        Ok(())
    }
}
