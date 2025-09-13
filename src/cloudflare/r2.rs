use aws_config::BehaviorVersion;

pub type Client = aws_sdk_s3::Client;

pub async fn create_client(
    account_id: &str,
    access_key_id: &str,
    secret_access_key: &str,
) -> Client {
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
    aws_sdk_s3::Client::new(&config)
}
