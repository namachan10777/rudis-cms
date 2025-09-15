use std::path::Path;

pub trait Client {
    type Error;
    fn put(&self, path: &Path, content: &[u8]) -> impl Future<Output = Result<(), Self::Error>>;
    fn delete(&self, path: &Path) -> impl Future<Output = Result<(), Self::Error>>;
}
