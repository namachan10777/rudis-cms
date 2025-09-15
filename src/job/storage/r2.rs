use aws_sdk_s3::primitives::ByteStream;

pub trait Client {
    type Error;
    fn put(
        &self,
        bucket: String,
        key: String,
        content_type: String,
        body: ByteStream,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    fn delete(
        &self,
        bucket: String,
        key: String,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
}
