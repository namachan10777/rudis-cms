//! Job executor implementation
//!
//! This module provides the main job executor that coordinates
//! database operations and storage uploads/deletions.

use std::{collections::HashSet, str::FromStr as _};

use futures::{future::try_join_all, join};
use indexmap::IndexMap;
use serde::Deserialize;
use serde_with::{json::JsonString, serde_as};
use sqlx::FromRow;
use tracing::{debug, error};

use crate::{
    process_data::{self, StorageContent, StoragePointer},
    schema::CollectionSchema,
};

use super::{
    filter::{disappeared_objects, filter_uploads},
    multiplex::{
        AssetDelete, AssetUpload, KvDelete, KvUpload, R2Delete, R2Upload, multiplex_delete,
        multiplex_upload,
    },
    sql,
    storage::{self, kv},
};

/// Job executor that coordinates database and storage operations.
pub struct JobExecutor<D, K, R, A> {
    pub d1: D,
    pub kv: K,
    pub r2: R,
    pub asset: A,
}

/// Error type for job execution.
#[derive(Debug, thiserror::Error)]
pub enum JobError<DE, KE, OE, AE> {
    #[error("database: {0}")]
    Database(DE),
    #[error("kv: {0}")]
    Kv(KE),
    #[error("objstore: {0}")]
    ObjectStorage(OE),
    #[error("asset: {0}")]
    Asset(AE),
}

fn deserialize_hash<'de, D>(deserializer: D) -> Result<blake3::Hash, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    blake3::Hash::from_str(&s).map_err(serde::de::Error::custom)
}

struct Ignore;

impl<'de> Deserialize<'de> for Ignore {
    fn deserialize<D>(_: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Ok(Self)
    }
}

impl<'r, R: sqlx::Row> FromRow<'r, R> for Ignore {
    fn from_row(_: &'r R) -> Result<Self, sqlx::Error> {
        Ok(Self)
    }
}

impl<
    D: storage::sqlite::Client,
    K: storage::kv::Client,
    O: storage::r2::Client,
    A: storage::asset::Client,
> JobExecutor<D, K, O, A>
{
    /// Fetch existing object metadata from the database.
    pub async fn fetch_objects_metadata(
        &self,
        schema: &CollectionSchema,
    ) -> Result<
        IndexMap<blake3::Hash, StoragePointer>,
        JobError<D::Error, K::Error, O::Error, A::Error>,
    > {
        #[derive(Deserialize)]
        struct B3Hash(#[serde(deserialize_with = "deserialize_hash")] blake3::Hash);

        impl<'q> sqlx::Decode<'q, sqlx::Sqlite> for B3Hash {
            fn decode(
                value: <sqlx::Sqlite as sqlx::Database>::ValueRef<'q>,
            ) -> Result<Self, sqlx::error::BoxDynError> {
                let s = <String as sqlx::Decode<sqlx::Sqlite>>::decode(value)?;
                blake3::Hash::from_str(&s)
                    .map_err::<sqlx::error::BoxDynError, _>(|e| Box::new(e))
                    .map(B3Hash)
            }
        }

        impl sqlx::Type<sqlx::Sqlite> for B3Hash {
            fn type_info() -> <sqlx::Sqlite as sqlx::Database>::TypeInfo {
                <String as sqlx::Type<sqlx::Sqlite>>::type_info()
            }

            fn compatible(ty: &<sqlx::Sqlite as sqlx::Database>::TypeInfo) -> bool {
                <String as sqlx::Type<sqlx::Sqlite>>::compatible(ty)
            }
        }

        #[serde_as]
        #[derive(Deserialize, FromRow)]
        struct Row {
            hash: B3Hash,
            #[serde_as(as = "JsonString")]
            #[sqlx(json)]
            storage: StoragePointer,
        }
        let objects = self
            .d1
            .query::<Row, &str>(&sql::fetch_objects(schema), &[])
            .await
            .map_err(JobError::Database)?
            .into_iter()
            .map(|row| (row.hash.0, row.storage))
            .collect::<IndexMap<_, _>>();
        Ok(objects)
    }

    async fn upload_objstore(
        &self,
        uploads: impl Iterator<Item = R2Upload>,
    ) -> Result<(), O::Error> {
        let tasks = uploads.map(|upload| {
            self.r2.put(
                upload.bucket,
                upload.key,
                upload.content_type,
                upload.body.into_vec().into(),
            )
        });
        try_join_all(tasks).await?;
        Ok(())
    }

    async fn upload_kv(&self, uploads: impl Iterator<Item = KvUpload>) -> Result<(), K::Error> {
        let mut namespaces = IndexMap::<_, Vec<_>>::new();
        for upload in uploads {
            let pair = kv::Pair::builder().key(upload.key);
            let pair = match upload.content {
                StorageContent::Bytes(bin) => pair.binary_value(&bin),
                StorageContent::Text(text) => pair.string_value(text),
            };
            namespaces
                .entry(upload.namespace.clone())
                .or_default()
                .push(pair.build().unwrap());
        }
        for (namespace, pairs) in namespaces {
            debug!(
                namespace,
                count = pairs.len(),
                "write multiple pairs into kv"
            );
            self.kv.write_multiple(&namespace, &pairs).await?;
        }
        Ok(())
    }

    async fn upload_asset(
        &self,
        uploads: impl Iterator<Item = AssetUpload>,
    ) -> Result<(), A::Error> {
        let tasks =
            uploads.map(|asset| async move { self.asset.put(&asset.path, &asset.body).await });
        try_join_all(tasks).await?;
        Ok(())
    }

    async fn delete_objstore(
        &self,
        deletes: impl Iterator<Item = R2Delete>,
    ) -> Result<(), O::Error> {
        let tasks = deletes
            .into_iter()
            .map(|delete| self.r2.delete(delete.bucket, delete.key));
        try_join_all(tasks).await?;
        Ok(())
    }

    async fn delete_kv(&self, deletes: impl Iterator<Item = KvDelete>) -> Result<(), K::Error> {
        let mut namespaces = IndexMap::<_, Vec<_>>::new();
        for delete in deletes {
            namespaces
                .entry(delete.namespace)
                .or_default()
                .push(delete.key);
        }
        let tasks = namespaces.into_iter().map(|(namespace, keys)| async move {
            self.kv.delete_multiple(&namespace, &keys).await
        });
        try_join_all(tasks).await?;
        Ok(())
    }

    async fn delete_asset(
        &self,
        assets: impl Iterator<Item = AssetDelete>,
    ) -> Result<(), A::Error> {
        let tasks = assets.map(|asset| async move { self.asset.delete(&asset.path).await });
        try_join_all(tasks).await?;
        Ok(())
    }

    async fn full_sync_db(
        &self,
        schema: &CollectionSchema,
        tables: &process_data::table::Tables,
    ) -> Result<(), D::Error> {
        let param = serde_json::to_string(tables).expect("tables must be encodable");
        for (table, schema) in &schema.tables {
            self.d1
                .query::<Ignore, _>(&sql::upsert(table, schema), &[&param.as_str()])
                .await?;
        }

        for (table, schema) in &schema.tables {
            self.d1
                .query::<Ignore, _>(&sql::cleanup(table, schema), &[&param.as_str()])
                .await?;
        }
        Ok(())
    }

    async fn create_tables_if_not_exist(&self, schema: &CollectionSchema) -> Result<(), D::Error> {
        self.d1
            .query::<Ignore, &str>(&sql::ddl(schema), &[])
            .await?;
        Ok(())
    }

    /// Execute a batch job: upload new objects, sync database, delete old objects.
    pub async fn batch(
        &self,
        schema: &CollectionSchema,
        tables: &process_data::table::Tables,
        uploads: process_data::table::Uploads,
        force: bool,
    ) -> Result<(), JobError<D::Error, K::Error, O::Error, A::Error>>
    where
        D::Error: std::error::Error,
        K::Error: std::error::Error,
        O::Error: std::error::Error,
        A::Error: std::error::Error,
    {
        self.create_tables_if_not_exist(schema)
            .await
            .map_err(JobError::Database)
            .inspect_err(|error| error!(%error, "failed to execute DDL"))?;
        let present_objects = self
            .fetch_objects_metadata(schema)
            .await
            .inspect_err(|error| error!(%error, "failed to fetch object list"))?;
        let delete_mask = uploads
            .iter()
            .map(|upload| &upload.pointer)
            .cloned()
            .collect::<HashSet<_>>();
        let uploads = filter_uploads(uploads.into_iter(), &present_objects, force);

        let (r2, kv, asset) = multiplex_upload(uploads);

        let (upload_r2, upload_kv, upload_asset) = join!(
            self.upload_objstore(r2.into_iter()),
            self.upload_kv(kv.into_iter()),
            self.upload_asset(asset.into_iter()),
        );
        upload_r2
            .map_err(JobError::ObjectStorage)
            .inspect_err(|error| error!(%error, "failed to upload objstore object list"))?;
        upload_kv
            .map_err(JobError::Kv)
            .inspect_err(|error| error!(%error, "failed to upload kv object list"))?;
        upload_asset
            .map_err(JobError::Asset)
            .inspect_err(|error| error!(%error, "failed to upload asset object list"))?;

        self.full_sync_db(schema, tables)
            .await
            .map_err(JobError::Database)
            .inspect_err(|error| error!(%error, "failed to synchronize database"))?;

        let appeared_objects = self
            .fetch_objects_metadata(schema)
            .await
            .inspect_err(|error| error!(%error, "failed to fetch object list"))?;
        let deletions = disappeared_objects(present_objects, &appeared_objects, &delete_mask);
        let (r2, kv, asset) = multiplex_delete(deletions);
        let (delete_objstore, delete_kv, delete_asset) = join!(
            self.delete_objstore(r2.into_iter()),
            self.delete_kv(kv.into_iter()),
            self.delete_asset(asset.into_iter()),
        );
        delete_objstore
            .map_err(JobError::ObjectStorage)
            .inspect_err(|error| error!(%error, "failed to delete objstore object"))?;
        delete_kv
            .map_err(JobError::Kv)
            .inspect_err(|error| error!(%error, "failed to delete kv object"))?;
        delete_asset
            .map_err(JobError::Asset)
            .inspect_err(|error| error!(%error, "failed to delete asset object"))?;

        Ok(())
    }

    /// Drop all tables (for dump/reset).
    pub async fn drop_all_table_for_dump(
        &self,
        schema: &CollectionSchema,
    ) -> Result<(), JobError<D::Error, K::Error, O::Error, A::Error>>
    where
        D::Error: std::error::Error,
        K::Error: std::error::Error,
        O::Error: std::error::Error,
        A::Error: std::error::Error,
    {
        self.d1
            .query::<Ignore, &str>(&sql::drop_all_tables(schema), &[])
            .await
            .map_err(JobError::Database)
            .inspect_err(|error| error!(%error, "failed to synchronize database"))?;
        Ok(())
    }
}
