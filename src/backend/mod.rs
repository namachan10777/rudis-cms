use std::collections::HashMap;

pub mod cloudflare;

pub trait Backend {
    type Error;

    fn changed(
        &self,
        local_hash_set: HashMap<String, blake3::Hash>,
    ) -> impl Future<Output = Result<String, Self::Error>>;

    fn changed_image(
        &self,
        local_hash_set: HashMap<String, blake3::Hash>,
    ) -> impl Future<Output = Result<String, Self::Error>>;
}
