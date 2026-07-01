pub trait Client {
    type Error: super::BackendError;
    fn put(
        &self,
        bucket: String,
        key: String,
        content_type: String,
        body: bytes::Bytes,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    fn delete(
        &self,
        bucket: String,
        key: String,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
}
