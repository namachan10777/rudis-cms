use std::path::PathBuf;

use aws_config::BehaviorVersion;
use futures::{
    FutureExt,
    future::{join_all, try_join_all},
    join, try_join,
};
use indexmap::IndexMap;
use serde::Deserialize;
use serde_json::json;
use tracing::warn;

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
    KvUploadFailed { namespace: String, error: String },
    #[error("Asset upload failed: {path}: {error}")]
    AssetUploadFailed {
        path: PathBuf,
        error: std::io::Error,
    },
    #[error("D1 query failed: {0}")]
    D1Transport(surf::Error),
    #[error("D1 query failed: {errors:?}")]
    D1QueryFailed { errors: Vec<D1Error> },
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

impl super::StorageBackend for CloudflareStorage {
    type Error = Error;
    async fn delete(
        &self,
        r2: impl Iterator<Item = super::R2Delete>,
        kv: impl Iterator<Item = super::KvDelete>,
        asset: impl Iterator<Item = super::AssetDelete>,
    ) -> Result<(), Self::Error> {
        let r2_uploads = r2.map(|delete| {
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
        let kv_uploads = kv_pairs.into_iter().map(|(namespace, keys)| {
            surf::delete(format!("https://api.cloudflare.com/client/v4/accounts/{}/storage/kv/namespaces/{namespace}/bulk", self.account_id))
                .header("Authorization", format!("Bearer {}", self.token))
                .header("Content-Type", "application/json")
                .body(serde_json::to_string(&keys).unwrap())
                .send()
                .map(move |result| if let Err(error) = result {
                    warn!(namespace, %error, "failed to delete key");
                })
        });
        let asset_uploads = asset.map(|upload| async move {
            if let Err(error) = smol::fs::remove_file(&upload.path).await {
                warn!(path=?upload.path, %error, "failed to remove asset file");
            }
        });
        join!(
            join_all(r2_uploads),
            join_all(kv_uploads),
            join_all(asset_uploads),
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
            let body = values.into_iter().map(|(key, content)|
                json!({
                    "key": key,
                    "value": content,
                    "base64": false,
                })
            ).collect::<Vec<_>>();
            surf::put(format!("https://api.cloudflare.com/client/v4/accounts/{}/storage/kv/namespaces/{namespace}/bulk", self.account_id))
                .header("Authorization", format!("Bearer {}", self.token))
                .header("Content-Type", "application/json")
                .body(serde_json::to_string(&body).unwrap())
                .send()
                .map(|result| result.map_err(|error| Error::KvUploadFailed {
                    namespace,
                    error: error.to_string(),
                }))
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
                smol::fs::create_dir_all(path)
                    .await
                    .map_err(|error| Error::AssetUploadFailed {
                        path: upload.path.clone(),
                        error,
                    })?;
            }
            smol::fs::write(&upload.path, &upload.body)
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

#[derive(Debug, Deserialize)]
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
        let mut response = surf::post(format!(
            "https://api.cloudflare.com/client/v4/accounts/{}/d1/database/{}/query",
            self.account_id, self.database_id
        ))
        .content_type("application/json")
        .header("Authorization", format!("Bearer {}", self.token))
        .body(
            serde_json::to_string(&json!({
                "sql": sql::SQL_FETCH_ALL_OBJECT.render(ctx).unwrap(),
                "params": [],
            }))
            .unwrap(),
        )
        .send()
        .await
        .map_err(Error::D1Transport)?;
        let body = response
            .body_json::<D1Response<[D1Result<ObjectRow>; 1]>>()
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
                errors: body.errors,
            })
        }
    }

    async fn sync(
        &self,
        ctx: &Self::Context,
        tables: crate::table::Tables,
    ) -> Result<(), Self::Error> {
        let mut response = surf::post(format!(
            "https://api.cloudflare.com/client/v4/accounts/{}/d1/database/{}/query",
            self.account_id, self.database_id
        ))
        .content_type("application/json")
        .header("Authorization", format!("Bearer {}", self.token))
        .body(
            serde_json::to_string(&json!({
                "sql": sql::SQL_UPSERT.render(ctx).unwrap(),
                "params": [serde_json::to_string(&tables).unwrap()],
            }))
            .unwrap(),
        )
        .send()
        .await
        .map_err(Error::D1Transport)?;
        let body = response
            .body_json::<D1Response<Vec<serde_json::Value>>>()
            .await
            .map_err(Error::D1Transport)?;
        if body.errors.is_empty() {
            Ok(())
        } else {
            Err(Error::D1QueryFailed {
                errors: body.errors,
            })
        }
    }
}
