use std::path::PathBuf;

use aws_config::BehaviorVersion;
use futures::{
    FutureExt,
    future::{join_all, try_join_all},
    join, try_join,
};
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tracing::{debug, trace, warn};

use crate::{field::StoragePointer, sql};

pub struct CloudflareStorage {
    r2: aws_sdk_s3::Client,
    token: String,
    account_id: String,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("R2 upload failed: {bucket}/{key}: {error}")]
    R2UploadFailed {
        bucket: String,
        key: String,
        error: String,
    },
    #[error("KV upload failed: {namespace}: {error}")]
    KvTransport { namespace: String, error: String },
    #[error("KV upload failed: {namespace}: {msg}")]
    KvUploadFailed { namespace: String, msg: String },
    #[error("Asset upload failed: {path}: {error}")]
    AssetUploadFailed {
        path: PathBuf,
        error: std::io::Error,
    },
    #[error("D1 query failed: {0}")]
    D1Transport(reqwest::Error),
    #[error("D1 query failed: {error}")]
    D1QueryFailed { error: String },
}

impl CloudflareStorage {
    pub async fn new(
        cf_account_id: &str,
        cf_api_token: &str,
        r2_access_key_id: &str,
        r2_secret_access_key: &str,
    ) -> Self {
        let config = aws_config::defaults(BehaviorVersion::latest())
            .endpoint_url(format!("https://{cf_account_id}.r2.cloudflarestorage.com"))
            .credentials_provider(aws_sdk_s3::config::Credentials::new(
                r2_access_key_id,
                r2_secret_access_key,
                None, // session token is not used with R2
                None,
                "R2",
            ))
            .region("auto")
            .load()
            .await;

        let r2 = aws_sdk_s3::Client::new(&config);
        Self {
            r2,
            account_id: cf_account_id.into(),
            token: cf_api_token.into(),
        }
    }
}

#[derive(Deserialize, Serialize, Debug)]
struct KvResult {
    success: bool,
    result: serde_json::Value,
    messages: Vec<serde_json::Value>,
    errors: Vec<serde_json::Value>,
}

impl super::StorageBackend for CloudflareStorage {
    type Error = Error;
    async fn delete(
        &self,
        r2: impl Iterator<Item = super::R2Delete>,
        kv: impl Iterator<Item = super::KvDelete>,
        asset: impl Iterator<Item = super::AssetDelete>,
    ) -> Result<(), Self::Error> {
        let r2_deletes = r2.map(|delete| {
            self.r2
                .delete_object()
                .bucket(delete.bucket.clone())
                .key(delete.key.clone())
                .send()
                .map(move |result| {
                    if let Err(error) = result {
                        warn!(
                            %error,
                            bucket = delete.bucket,
                            key = delete.key,
                            "failed to delete R2 object",
                        );
                    }
                })
        });
        let mut kv_pairs = IndexMap::<_, Vec<_>>::new();
        for delete in kv {
            kv_pairs
                .entry(delete.namespace)
                .or_default()
                .push(delete.key);
        }
        let kv_deletes = kv_pairs.into_iter().map(|(namespace, keys)| {
            debug!(?keys, "delete kv pairs");
            reqwest::Client::new().delete(
                format!("https://api.cloudflare.com/client/v4/accounts/{}/storage/kv/namespaces/{namespace}/bulk", self.account_id)
            )
            .bearer_auth(&self.token)
                .header("Content-Type", "application/json")
                .body(serde_json::to_string(&keys).unwrap())
                .send()
                .then(|result| async move { match result  {
                    Err(error) => warn!(namespace, %error, "failed to delete key"),
                    Ok(result) => {
                        if !result.status().is_success() {
                            let status = result.status().as_u16();
                            let text = result.text().await.unwrap_or_default();
                            warn!( status, text, "failed to delete key");
                        }
                    }
                }})
        });
        let asset_deletes = asset.map(|upload| async move {
            if let Err(error) = tokio::fs::remove_file(&upload.path).await {
                warn!(path=?upload.path, %error, "failed to remove asset file");
            }
        });
        join!(
            join_all(r2_deletes),
            join_all(kv_deletes),
            join_all(asset_deletes),
        );
        Ok(())
    }

    async fn upload(
        &self,
        r2: impl Iterator<Item = super::R2Upload>,
        kv: impl Iterator<Item = super::KvUpload>,
        asset: impl Iterator<Item = super::AssetUpload>,
    ) -> Result<(), Self::Error> {
        let r2_uploads = r2.map(|upload| {
            self.r2
                .put_object()
                .bucket(upload.bucket.clone())
                .key(upload.key.clone())
                .body(upload.body.into_vec().into())
                .content_type(upload.content_type)
                .send()
                .map(|result| {
                    result.map_err(|e| Error::R2UploadFailed {
                        bucket: upload.bucket,
                        key: upload.key,
                        error: e.to_string(),
                    })
                })
        });
        let mut kv_pairs = IndexMap::<_, Vec<_>>::new();
        for upload in kv {
            kv_pairs
                .entry(upload.namespace)
                .or_default()
                .push((upload.key, upload.content));
        }
        let kv_uploads = kv_pairs.into_iter().map(|(namespace, values)| {
            debug!(keys=?values.iter().map(|(key, _)| key).collect::<Vec<_>>(), "upload kv pairs");
            let body = values.into_iter().map(|(key, content)|
                json!({
                    "key": key,
                    "value": content,
                    "base64": false,
                })
            ).collect::<Vec<_>>();
            reqwest::Client::new().put(
                format!("https://api.cloudflare.com/client/v4/accounts/{}/storage/kv/namespaces/{namespace}/bulk", self.account_id)
            )
            .bearer_auth(&self.token)
            .json(&body)
            .send()
            .then(|result| async move { match result {
                Err(error) => Err(
                    Error::KvTransport {
                        namespace,
                        error: error.to_string(),
                    }
                ),
                Ok(response) => {
                    let msg = response.json::<KvResult>().await.map_err(|error|
                        Error::KvTransport {
                            namespace: namespace.clone(),
                            error: error.to_string(),
                        })?;
                    if msg.success {
                        trace!(?msg, "kv success");
                        Ok(())
                    }
                    else {
                        Err(Error::KvUploadFailed { namespace, msg: serde_json::to_string(&msg).unwrap() })
                    }
                }
            }})
        });
        let asset_uploads = asset.map(|upload| async move {
            if let Some(path) = upload
                .path
                .canonicalize()
                .map_err(|error| Error::AssetUploadFailed {
                    path: upload.path.clone(),
                    error,
                })?
                .parent()
            {
                tokio::fs::create_dir_all(path).await.map_err(|error| {
                    Error::AssetUploadFailed {
                        path: upload.path.clone(),
                        error,
                    }
                })?;
            }
            tokio::fs::write(&upload.path, &upload.body)
                .await
                .map_err(|error| Error::AssetUploadFailed {
                    path: upload.path.clone(),
                    error,
                })
        });
        try_join!(
            try_join_all(r2_uploads),
            try_join_all(kv_uploads),
            try_join_all(asset_uploads),
        )?;
        Ok(())
    }
}

pub struct D1Database {
    account_id: String,
    token: String,
    database_id: String,
}

impl D1Database {
    pub fn new(account_id: &str, token: &str, database_id: &str) -> Self {
        Self {
            account_id: account_id.into(),
            token: token.into(),
            database_id: database_id.into(),
        }
    }
}

#[derive(Deserialize)]
struct D1Result<Row> {
    results: Vec<Row>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct D1Error {
    pub code: u16,
    pub message: String,
}

#[derive(Deserialize)]
struct D1Response<R> {
    result: R,
    errors: Vec<D1Error>,
}

#[derive(Deserialize)]
struct ObjectRow {
    hash: String,
    storage: String,
}

impl super::Database for D1Database {
    type Error = Error;
    type Context = liquid::Object;

    async fn create_context(
        &self,
        schema: &crate::schema::CollectionSchema,
    ) -> Result<Self::Context, Self::Error> {
        Ok(sql::liquid_default_context(schema))
    }

    async fn fetch_objects_metadata(
        &self,
        ctx: &Self::Context,
    ) -> Result<IndexMap<blake3::Hash, crate::field::StoragePointer>, Self::Error> {
        let response = reqwest::Client::new()
            .post(format!(
                "https://api.cloudflare.com/client/v4/accounts/{}/d1/database/{}/query",
                self.account_id, self.database_id
            ))
            .bearer_auth(&self.token)
            .json(&json!({
                "sql": sql::SQL_FETCH_ALL_OBJECT.render(ctx).unwrap(),
                "params": [],
            }))
            .send()
            .await
            .map_err(Error::D1Transport)?;
        let body = response
            .json::<D1Response<[D1Result<ObjectRow>; 1]>>()
            .await
            .map_err(Error::D1Transport)?;
        if body.errors.is_empty() {
            let mut metadata = IndexMap::new();
            for row in &body.result[0].results {
                let hash = row.hash.parse().ok();
                let storage = serde_json::from_str::<StoragePointer>(&row.storage).ok();
                if let (Some(hash), Some(storage)) = (hash, storage) {
                    metadata.insert(hash, storage);
                }
            }
            Ok(metadata)
        } else {
            Err(Error::D1QueryFailed {
                error: serde_json::to_string(&body.errors).unwrap(),
            })
        }
    }

    async fn sync(
        &self,
        ctx: &Self::Context,
        tables: &crate::table::Tables,
    ) -> Result<(), Self::Error> {
        let query = sql::SQL_UPSERT.render(ctx).unwrap();
        let tasks =
            query
                .split(";")
                .map(|statement| statement.trim())
                .map(|statement| async move {
                    if !statement.is_empty() {
                        let response = reqwest::Client::new()
                        .post(format!(
                            "https://api.cloudflare.com/client/v4/accounts/{}/d1/database/{}/query",
                            self.account_id, self.database_id
                        ))
                        .bearer_auth(&self.token)
                        .json(&json!({
                            "sql": statement,
                            "params": [serde_json::to_string(tables).unwrap()],
                        }))
                        .send()
                        .await
                        .map_err(Error::D1Transport)?;
                        if !response.status().is_success() {
                            let msg = response.text().await.map_err(Error::D1Transport)?;
                            return Err(Error::D1QueryFailed { error: msg });
                        }
                        let response = response
                            .json::<D1Response<Vec<serde_json::Value>>>()
                            .await
                            .map_err(Error::D1Transport)?;
                        debug!(result=?response.result, statement, "deleted keys");
                        if !response.errors.is_empty() {
                            Err(Error::D1QueryFailed {
                                error: serde_json::to_string(&response.errors).unwrap(),
                            })
                        } else {
                            Ok(())
                        }
                    } else {
                        Ok(())
                    }
                });
        for task in tasks {
            task.await?;
        }
        Ok(())
    }
}
