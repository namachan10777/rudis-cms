use crate::job;

pub struct Client {}

impl job::storage::asset::Client for Client {
    type Error = std::io::Error;
    async fn delete(&self, path: &std::path::Path) -> Result<(), Self::Error> {
        tokio::fs::remove_file(path).await
    }

    async fn put(&self, path: &std::path::Path, content: &[u8]) -> Result<(), Self::Error> {
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        tokio::fs::write(path, content).await
    }
}
