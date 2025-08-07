pub mod d1;

pub struct CloudflareBackend {}

#[derive(Debug, thiserror::Error)]
pub enum Error {}

impl super::Backend for CloudflareBackend {
    type Error = Error;

    async fn changed(
        &self,
        local_hash_set: std::collections::HashMap<String, blake3::Hash>,
    ) -> Result<String, Self::Error> {
        unimplemented!()
    }

    async fn changed_image(
        &self,
        local_hash_set: std::collections::HashMap<String, blake3::Hash>,
    ) -> Result<String, Self::Error> {
        unimplemented!()
    }
}
